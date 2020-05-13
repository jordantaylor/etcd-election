package election

import (
	"context"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	DefaultTTLSec     = 35
	DefaultBackoffSec = 2
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

	e.leaderChan = make(chan bool)

	if e.ctx == nil {
		return nil, errors.New("a non-nil context value is required")
	}

	if e.reconnectBackoff.Milliseconds() <= 0 {
		e.reconnectBackoff = DefaultBackoffSec * time.Second
	}

	if e.ttl <= 0 {
		e.ttl = DefaultTTLSec
	}

	return e, nil
}

type Config struct {
	Context          context.Context
	Client           *etcd.Client
	ElectionName     string
	CandidateName    string
	ReconnectBackoff time.Duration
	TTL              int
	ResumeLeader     bool
}

type Election struct {
	ctx              context.Context
	client           *etcd.Client
	electionName     string
	candidateName    string
	reconnectBackoff time.Duration
	ttl              int
	resumeLeader     bool

	isLeader   bool
	leaderChan chan bool
	session    *concurrency.Session
	election   *concurrency.Election
}

// Run will start the election management goroutine and returns once the first leader has been decided
// subsequent leaders will automatically be determined by the manageElection goroutine after the initial call to Run().
func (e *Election) Run() (<-chan bool, error) {
	if err := e.newSession(e.ctx, 0); err != nil {
		return nil, errors.Wrap(err, "while creating initial session")
	}

	go e.manageElection()

	// Wait until we have a leader before returning
	for {
		resp, err := e.election.Leader(e.ctx)
		if err != nil {
			if err != concurrency.ErrElectionNoLeader {
				return nil, err
			}

			time.Sleep(time.Millisecond * 300)

			log.Info("no leader set yet")

			continue
		}
		// If we are not leader, notify the channel
		leader := string(resp.Kvs[0].Value)
		log.Info("leader is ", leader)

		if leader != e.candidateName {
			e.setLeader(false)
		}

		break
	}

	return e.leaderChan, nil
}

func (e *Election) manageElection() {
	var (
		//observe <-chan etcd.GetResponse
		node    *etcd.GetResponse
		err     error
		errChan chan error
	)

	defer close(e.leaderChan)

	for {
		// Discover who if any, is leader of this election
		if node, err = e.election.Leader(e.ctx); err != nil {
			if err != concurrency.ErrElectionNoLeader {
				if err == context.Canceled {
					return
				}

				log.Error("unable to determine election leader: ", err)
				e.reconnect()

				continue
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
				// resume as leader if resumeLeadership is true
				if e.resumeLeader {
					// Recreate our session with the old lease id
					if err = e.newSession(e.ctx, node.Kvs[0].Lease); err != nil {
						log.Error("unable to re-establish session with lease: ", err)
						e.reconnect()

						continue
					}

					e.election = concurrency.ResumeElection(
						e.session,
						e.electionName,
						string(node.Kvs[0].Key),
						node.Kvs[0].CreateRevision,
					)

					// Because Campaign() only returns if the election entry doesn't exist
					// we must skip the campaign call and go directly to observe when resuming

					//goto observe
					if e.observe() {
						log.Info("resigned leadership and now exiting")
						return
					}
				} else {
					// If resign takes longer than our TTL then lease is expired and we are no
					// longer leader anyway.
					ctx, cancel := context.WithTimeout(e.ctx, time.Duration(e.ttl)*time.Second)
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
						e.reconnect()

						//continue
					}
				}
			}
		}
		// Reset leadership if we had it previously
		e.setLeader(false)

		errChan = make(chan error)
		// Make this a non blocking call so we can check for session close
		go func() {
			errChan <- e.election.Campaign(e.ctx, e.candidateName)
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
				e.reconnect()

				continue
			}
		case <-e.ctx.Done():
			e.session.Close()
			return
		case <-e.session.Done():
			e.reconnect()
			continue
		}
	}
}

//func (e *Election) resumeOrResign()

func (e *Election) observe() (halt bool) {
	// If Campaign() returned without error, we are the leader
	e.setLeader(true)

	// Observe changes to leadership
	observe := e.election.Observe(e.ctx)

	for {
		select {
		case resp, ok := <-observe:
			if !ok {
				// NOTE: Observe will not close if the session expires, we must watch for session.Done()
				e.session.Close()
				e.reconnect()

				continue
			}

			e.setLeader(string(resp.Kvs[0].Value) == e.candidateName)

			log.Info("observed response with value of ", string(resp.Kvs[0].Value))

			if !e.isLeader {
				e.reconnect()
				return
			}
		case <-e.ctx.Done():
			if e.isLeader {
				// If resign takes longer than our TTL, the lease has expired and we are no longer the leader
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(e.ttl)*time.Second)
				if err := e.election.Resign(ctx); err != nil {
					log.Errorf("while resigning leadership during shutdown: %s", err)
				}

				cancel()
			}

			e.session.Close()

			halt = true

			return
		case <-e.session.Done():
			e.reconnect()
			continue
		}
	}
}

func (e *Election) reconnect() {
	var err error

	e.setLeader(false)

	for {
		log.Info("trying to reconnect...")

		if err = e.newSession(e.ctx, 0); err != nil {
			if errors.Cause(err) == context.Canceled {
				return
			}

			log.Error("problem creating new session: ", err)

			tick := time.NewTicker(e.reconnectBackoff)
			select {
			case <-e.ctx.Done():
				tick.Stop()
				return
			case <-tick.C:
				tick.Stop()
			}

			continue
		}

		log.Info("reconnected successfully")

		return
	}
}

func (e *Election) setLeader(leaderState bool) {
	if e.isLeader != leaderState {
		e.isLeader = leaderState
		e.leaderChan <- e.isLeader
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
