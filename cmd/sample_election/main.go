package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	election "github.com/jordantaylor/etcd-election/pkg/election"
	log "github.com/sirupsen/logrus"
)

const (
	etcdEndpoint = "localhost:2379"

	electionName   = "election_0"
	candidateName  = "node_0"
	candidateValue = "node_0"
	ttlSec         = 35
	resumeLead     = false
	retrySec       = 2
)

func main() {
	client, err := etcd.New(etcd.Config{Endpoints: []string{etcdEndpoint}})
	if err != nil {
		log.Fatal("failed to create etcd client ", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	e := election.NewElection(ctx, electionName, candidateName, resumeLead, ttlSec, retrySec*time.Second, client)

	leaderChan, err := e.RunElection()
	if err != nil {
		log.Fatal("problem running election ", err)
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
