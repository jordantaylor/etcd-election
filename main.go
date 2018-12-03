package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var (
	electionName     = "/election-test"
	candidateName    = "worker01"
	resumeLeader     = true
	TTL              = 35
	reconnectBackOff = time.Second * 2
	session          *concurrency.Session
	election         *concurrency.Election
	client           *etcd.Client
)

func main() {
	var err error

	log.SetLevel(log.InfoLevel)

	client, err = etcd.New(etcd.Config{
		Endpoints: []string{"localhost:2379"},
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	leaderChan, err := runElection(ctx)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT)
	go func() {
		for {
			select {
			case <-c:
				fmt.Printf("Resign election and exit\n")
				cancel()
			}
		}
	}()

	for leader := range leaderChan {
		fmt.Printf("Leader: %t\n", leader)
	}

	cancel()
}

func runElection(ctx context.Context) (<-chan bool, error) {
	var observe <-chan etcd.GetResponse
	var node *etcd.GetResponse
	var errChan chan error
	var isLeader bool
	var err error

	var leaderChan chan bool
	setLeader := func(set bool) {
		// Only report changes in leadership
		if isLeader == set {
			return
		}
		isLeader = set
		leaderChan <- set
	}

	if err = newSession(ctx, 0); err != nil {
		return nil, errors.Wrap(err, "while creating initial session")
	}

	go func() {
		leaderChan = make(chan bool, 10)
		defer close(leaderChan)

		for {
			// Discover who if any, is leader of this election
			if node, err = election.Leader(ctx); err != nil {
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
				if string(node.Kvs[0].Value) == candidateName {
					// If we want to resume leadership
					if resumeLeader {
						// Recreate our session with the old lease id
						if err = newSession(ctx, node.Kvs[0].Lease); err != nil {
							log.Errorf("while re-establishing session with lease: %s", err)
							goto reconnect
						}
						election = concurrency.ResumeElection(session, electionName,
							string(node.Kvs[0].Key), node.Kvs[0].CreateRevision)

						// Because Campaign() only returns if the election entry doesn't exist
						// we must skip the campaign call and go directly to observe when resuming
						goto observe
					} else {
						// If resign takes longer than our TTL then lease is expired and we are no
						// longer leader anyway.
						ctx, cancel := context.WithTimeout(ctx, time.Duration(TTL)*time.Second)
						election := concurrency.ResumeElection(session, electionName,
							string(node.Kvs[0].Key), node.Kvs[0].CreateRevision)
						err = election.Resign(ctx)
						cancel()
						if err != nil {
							log.Errorf("while resigning leadership after reconnect: %s", err)
							goto reconnect
						}
					}
				}
			}
			// Reset leadership if we had it previously
			setLeader(false)

			// Attempt to become leader
			errChan = make(chan error)
			go func() {
				// Make this a non blocking call so we can check for session close
				errChan <- election.Campaign(ctx, candidateName)
			}()

			select {
			case err = <-errChan:
				if err != nil {
					if errors.Cause(err) == context.Canceled {
						return
					}
					// NOTE: Campaign currently does not return an error if session expires
					log.Errorf("while campaigning for leader: %s", err)
					session.Close()
					goto reconnect
				}
			case <-ctx.Done():
				session.Close()
				return
			case <-session.Done():
				goto reconnect
			}

		observe:
			// If Campaign() returned without error, we are leader
			setLeader(true)

			// Observe changes to leadership
			observe = election.Observe(ctx)
			for {
				select {
				case resp, ok := <-observe:
					if !ok {
						// NOTE: Observe will not close if the session expires, we must
						// watch for session.Done()
						session.Close()
						goto reconnect
					}
					if string(resp.Kvs[0].Value) == candidateName {
						setLeader(true)
					} else {
						// We are not leader
						setLeader(false)
						break
					}
				case <-ctx.Done():
					if isLeader {
						// If resign takes longer than our TTL then lease is expired and we are no
						// longer leader anyway.
						ctx, cancel := context.WithTimeout(context.Background(), time.Duration(TTL)*time.Second)
						if err = election.Resign(ctx); err != nil {
							log.Errorf("while resigning leadership during shutdown: %s", err)
						}
						cancel()
					}
					session.Close()
					return
				case <-session.Done():
					goto reconnect
				}
			}

		reconnect:
			setLeader(false)

			for {
				if err = newSession(ctx, 0); err != nil {
					if errors.Cause(err) == context.Canceled {
						return
					}
					log.Errorf("while creating new session: %s", err)
					tick := time.NewTicker(reconnectBackOff)
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
		resp, err := election.Leader(ctx)
		if err != nil {
			if err != concurrency.ErrElectionNoLeader {
				return nil, err
			}
			time.Sleep(time.Millisecond * 300)
			continue
		}
		// If we are not leader, notify the channel
		if string(resp.Kvs[0].Value) != candidateName {
			leaderChan <- false
		}
		break
	}
	return leaderChan, nil
}

func newSession(ctx context.Context, id int64) error {
	var err error
	session, err = concurrency.NewSession(client, concurrency.WithTTL(TTL),
		concurrency.WithContext(ctx), concurrency.WithLease(etcd.LeaseID(id)))
	if err != nil {
		return err
	}
	election = concurrency.NewElection(session, electionName)
	return nil
}
