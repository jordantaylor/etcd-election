package election

import (
	"context"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

func New(c Config) (*Election, error) {
	e := &Election{
		ctx:              c.Context,
		client:           c.Client,
		electionName:     c.ElectionName,
		candidateName:    c.CandidateName,
		reconnectBackoff: c.ReconnectBackoff,
		ttl:              c.TTL,
		resumeLeader:     c.ResumeLeader,
	}

	if e.ctx == nil {
		return nil, errors.New("a non-nil context value is required")
	}

	if e.candidateValue == "" {
		e.candidateValue = e.candidateName
	}

	if e.reconnectBackoff.Milliseconds() <= 0 {
		e.reconnectBackoff = 2 * time.Second
	}

	if e.ttl <= 0 {
		e.ttl = 15
	}

	return e, nil
}

type Config struct {
	Context          context.Context
	Client           *etcd.Client
	ElectionName     string
	CandidateName    string
	CandidateValue   string
	ReconnectBackoff time.Duration
	TTL              int
	ResumeLeader     bool
}

type Election struct {
	ctx              context.Context
	client           *etcd.Client
	electionName     string
	candidateName    string
	candidateValue   string
	reconnectBackoff time.Duration
	ttl              int
	resumeLeader     bool

	isLeader   bool
	leaderChan chan bool
	session    *concurrency.Session
	election   *concurrency.Election
}

func (e *Election) Run() (<-chan bool, error) {
	var (
		observe <-chan etcd.GetResponse
		node    *etcd.GetResponse
		errChan chan error
		err     error
		ctx     = e.ctx
	)

	if err = e.newSession(ctx, 0); err != nil {
		return nil, errors.Wrap(err, "while creating initial session")
	}

	go func() {
		e.leaderChan = make(chan bool)
		defer close(e.leaderChan)

		for {
			// Discover who if any, is leader of this election
			if node, err = e.election.Leader(ctx); err != nil {
				if err != concurrency.ErrElectionNoLeader {
					log.Errorf("while determining election leader: %s", err)
					goto reconnect
				}
			} else {
				// If we are resuming an election from which we previously had leadership we
				// have 2 options
				// 1. Resume the leadership if the lease has not expired. This is a race as the
				//    lease could expire in between the `Leader()` call and when we resume
				//    observing changes to the election. If this happens we should detect the
				//    session has expired during the observation loop.
				// 2. Resign the leadership immediately to allow a new leader to be chosen.
				//    This option will almost always result in transfer of leadership.
				if string(node.Kvs[0].Value) == e.candidateName {
					// If we want to resume leadership
					if e.resumeLeader {
						// Recreate our session with the old lease id
						if err = e.newSession(ctx, node.Kvs[0].Lease); err != nil {
							log.Errorf("while re-establishing session with lease: %s", err)
							goto reconnect
						}

						e.election = concurrency.ResumeElection(
							e.session,
							e.electionName,
							string(node.Kvs[0].Key),
							node.Kvs[0].CreateRevision,
						)

						// Because Campaign() only returns if the election entry doesn't exist
						// we must skip the campaign call and go directly to observe when resuming
						goto observe
					} else {
						// If resign takes longer than our TTL then lease is expired and we are no
						// longer leader anyway.
						ctx, cancel := context.WithTimeout(ctx, time.Duration(e.ttl)*time.Second)
						e.election = concurrency.ResumeElection(
							e.session,
							e.electionName,
							string(node.Kvs[0].Key),
							node.Kvs[0].CreateRevision,
						)

						err = e.election.Resign(ctx)
						cancel()
						if err != nil {
							log.Errorf("while resigning leadership after reconnect: %s", err)
							goto reconnect
						}
					}
				}
			}
			// Reset leadership if we had it previously
			e.setLeader(false)

			// Attempt to become leader
			errChan = make(chan error)

			go func() {
				// Make this a non blocking call so we can check for session close
				errChan <- e.election.Campaign(ctx, e.candidateName)
			}()

			select {
			case err = <-errChan:
				if err != nil {
					if errors.Cause(err) == context.Canceled {
						return
					}
					// NOTE: Campaign currently does not return an error if session expires
					log.Errorf("while campaigning for leader: %s", err)
					e.session.Close()

					goto reconnect
				}
			case <-ctx.Done():
				e.session.Close()
				return
			case <-e.session.Done():
				goto reconnect
			}

		observe:
			// If Campaign() returned without error, we are leader
			e.setLeader(true)

			// Observe changes to leadership
			observe = e.election.Observe(ctx)

			for {
				select {
				case resp, ok := <-observe:
					if !ok {
						// NOTE: Observe will not close if the session expires, we must
						// watch for session.Done()
						e.session.Close()
						goto reconnect
					}

					if string(resp.Kvs[0].Value) == e.candidateName {
						e.setLeader(true)
					} else {
						// We are not leader
						e.setLeader(false)
						break
					}
				case <-ctx.Done():
					if e.isLeader {
						// If resign takes longer than our TTL then lease is expired and we are no
						// longer leader anyway.
						ctx, cancel := context.WithTimeout(context.Background(), time.Duration(e.ttl)*time.Second)
						if err = e.election.Resign(ctx); err != nil {
							log.Errorf("while resigning leadership during shutdown: %s", err)
						}

						cancel()
					}

					e.session.Close()

					return
				case <-e.session.Done():
					goto reconnect
				}
			}

		reconnect:
			e.setLeader(false)

			for {
				if err = e.newSession(ctx, 0); err != nil {
					if errors.Cause(err) == context.Canceled {
						return
					}

					log.Errorf("while creating new session: %s", err)

					tick := time.NewTicker(e.reconnectBackoff)
					select {
					case <-ctx.Done():
						tick.Stop()
						return
					case <-tick.C:
						tick.Stop()
					}

					continue
				}

				break
			}
		}
	}()

	// Wait until we have a leader before returning
	for {
		resp, err := e.election.Leader(ctx)
		if err != nil {
			if err != concurrency.ErrElectionNoLeader {
				return nil, err
			}

			time.Sleep(time.Millisecond * 300)

			continue
		}
		// If we are not leader, notify the channel
		if string(resp.Kvs[0].Value) != e.candidateName {
			e.leaderChan <- false
		}

		break
	}

	return e.leaderChan, nil
}

func (e *Election) setLeader(leaderState bool) {
	if e.isLeader != leaderState {
		e.isLeader = leaderState
		e.leaderChan <- leaderState
	}
}

func (e *Election) newSession(ctx context.Context, leaseID int64) (err error) {
	e.session, err = concurrency.NewSession(
		e.client,
		concurrency.WithTTL(e.ttl),
		concurrency.WithContext(ctx),
		concurrency.WithLease(etcd.LeaseID(leaseID)),
	)
	if err != nil {
		return
	}

	e.election = concurrency.NewElection(e.session, e.electionName)
	err = nil

	return
}
