package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	election "github.com/jortaylor/etcd-election"
	log "github.com/sirupsen/logrus"
)

func main() {
	client, err := etcd.New(etcd.Config{Endpoints: []string{"localhost:2379"}})
	if err != nil {
		log.Fatal("failed to create etcd client ", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	e := election.NewElection(ctx, "election_0", "node_0", false, 35, 2*time.Second, client)

	leaderChan, err := e.runElection()
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
