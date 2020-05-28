package election

import (
	"context"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	DefaultTTLSec     = 35
	DefaultBackoffSec = 2
)

type Leader struct {
	IsLeader bool
	URI      string
}

func New(c Config) (*Election, error) {
	e := &Election{
		ctx:              c.Context,
		client:           c.Client,
		electionName:     c.ElectionName,
		CandidateName:    c.CandidateName,
		reconnectBackoff: c.ReconnectBackoff,
		ttl:              c.TTL,
		resumeLeader:     c.ResumeLeader,
		leader: &Leader{
			IsLeader: false,
			URI:      "",
		},
		leaderChan: make(chan Leader),
	}

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
	reconnectBackoff time.Duration
	ttl              int
	resumeLeader     bool

	leader     *Leader
	leaderChan chan Leader

	session  *concurrency.Session
	election *concurrency.Election

	CandidateName string
}

// Run will start the election management goroutine and returns a channel for reading leader status updates
func (e *Election) Run() (<-chan Leader, error) {
	if err := e.newSession(e.ctx, 0); err != nil {
		return nil, errors.Wrap(err, "while creating initial session")
	}

	go e.manageElection()

	return e.leaderChan, nil
}

func (e *Election) manageElection() {
	var (
		node    *etcd.GetResponse
		err     error
		errChan chan error
	)

	defer close(e.leaderChan)

	for {
		errChan = make(chan error)

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
			if len(node.Kvs) > 0 {
				l := string(node.Kvs[0].Value)
				if l == e.CandidateName {
					if resign := e.leaderResumeElection(node.Kvs[0]); resign {
						return
					}
				} else {
					e.leaderChan <- Leader{IsLeader: l == e.CandidateName, URI: l}
				}
			} else {
				log.Error("unexpected empty Kvs response from etcd with nil error")
				continue
			}
		}

		// Reset leadership if we had it previously
		e.setLeader(false)

		// Make this a non blocking call so we can check for session close
		go func() {
			errChan <- e.election.Campaign(e.ctx, e.CandidateName)
		}()

		select {
		case err = <-errChan:
			if err != nil {
				if errors.Cause(err) == context.Canceled {
					return
				}

				// NOTE: Campaign currently does not return an error if session expires
				log.Error("problem campaigning for leader: ", err)

				if er := e.session.Close(); er != nil {
					log.Warn("problem closing session: ", er)
				}

				e.reconnect()

				continue
			}
		case <-e.ctx.Done():
			if er := e.session.Close(); er != nil {
				log.Warn("problem closing session: ", er)
			}

			return
		case <-e.session.Done():
			e.reconnect()
			continue
		}
	}
}

// leaderResumeElection attempts to resume an election it had leadership of previously
// we have 2 options:
//    1. Resume the leadership if the lease has not expired. This is a race as the
//       lease could expire in between the `Leader()` call and when we resume
//       observing changes to the election. If this happens we should detect the
//       session has expired during the observation loop.
//    2. Resign the leadership immediately to allow a new leader to be chosen.
//       This option will almost always result in transfer of leadership.
func (e *Election) leaderResumeElection(kv *mvccpb.KeyValue) (resign bool) {
	if e.resumeLeader {
		// Recreate our session with the old lease id
		if err := e.newSession(e.ctx, kv.Lease); err != nil {
			log.Error("unable to re-establish session with lease: ", err)
			e.reconnect()

			return false
		}

		e.election = concurrency.ResumeElection(e.session, e.electionName, string(kv.Key), kv.CreateRevision)

		// Because Campaign() only returns if the election entry doesn't exist
		// we must skip the campaign call and go directly to observe when resuming
		if e.observe() {
			log.Info("resigned leadership and now exiting")
			return true
		}
	} else {
		// If resign takes longer than our TTL then lease is expired and we are no
		// longer leader anyway.
		ctx, cancel := context.WithTimeout(e.ctx, time.Duration(e.ttl)*time.Second)
		e.election = concurrency.ResumeElection(e.session, e.electionName, string(kv.Key), kv.CreateRevision)

		err := e.election.Resign(ctx)
		cancel()
		if err != nil {
			log.Errorf("while resigning leadership after reconnect: %s", err)
			e.reconnect()
		}
	}

	return false
}

func (e *Election) observe() (halt bool) {
	// If Campaign() returned without error, we are the leader
	e.setLeader(true, e.CandidateName)

	// Observe changes to leadership
	observe := e.election.Observe(e.ctx)

	for {
		select {
		case resp, ok := <-observe:
			if !ok {
				// NOTE: Observe will not close if the session expires, we must watch for session.Done()
				e.session.Close()
				e.reconnect()
			}

			if len(resp.Kvs) > 0 {
				l := string(resp.Kvs[0].Value)
				e.setLeader(l == e.CandidateName, l)
				log.Info("observed response with value of ", l)

				if !e.getLeader() {
					e.reconnect()
					return
				}
			}
		case <-e.ctx.Done():
			if e.getLeader() {
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

func (e *Election) setLeader(leaderState bool, uri ...string) {
	if e.leader.IsLeader != leaderState {
		e.leader.IsLeader = leaderState

		if len(uri) > 0 {
			e.leader.URI = uri[0]
		}

		e.leaderChan <- Leader{
			IsLeader: leaderState,
			URI:      e.leader.URI,
		}
	}
}

func (e *Election) getLeader() (isLeader bool) {
	return e.leader.IsLeader
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
