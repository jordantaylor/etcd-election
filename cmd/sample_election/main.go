package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	etcd "github.com/coreos/etcd/clientv3"
	election "github.com/jordantaylor/etcd-election/pkg/election"
	log "github.com/sirupsen/logrus"
)

const (
	etcdEndpoint  = "localhost:2379"
	electionName  = "/tst/election_"
	candidateName = "node_0"
)

func main() {
	var (
		client      *etcd.Client
		e           *election.Election
		err         error
		ctx, cancel = context.WithCancel(context.Background())
	)

	log.SetLevel(log.InfoLevel)

	client, err = etcd.New(etcd.Config{Endpoints: []string{etcdEndpoint}})
	if err != nil {
		log.Fatal("failed to create etcd client: ", err)
	}

	e, err = election.New(election.Config{
		Context:       ctx,
		Client:        client,
		ElectionName:  electionName,
		CandidateName: candidateName,
		ResumeLeader:  true,
	})
	if err != nil {
		log.Fatal("failed to create election: ", err)
	}

	leaderChan, err := e.Run()
	if err != nil {
		log.Fatal("problem running election: ", err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT)

	go func() {
		for range c {
			log.Info("resigning leadership and exiting")
			cancel()
		}
	}()

	for leader := range leaderChan {
		log.Info("leader: ", leader)
	}

	cancel()
}
