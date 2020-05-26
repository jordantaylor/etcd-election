package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	etcd "github.com/coreos/etcd/clientv3"
	election "github.com/jordantaylor/etcd-election/pkg/election"
	log "github.com/sirupsen/logrus"
)

func main() {
	var (
		client      *etcd.Client
		e           *election.Election
		err         error
		ctx, cancel = context.WithCancel(context.Background())

		candidateName = flag.String("candidate", "node_0", "name of the candidate to run as")
		electionName  = flag.String("prefix", "/test/election", "election prefix to run for")
		etcdEndpoint  = flag.String("endpoint", "localhost:2379", "URI for etcd instance to use for election")
	)

	flag.Parse()
	log.SetLevel(log.InfoLevel)

	client, err = etcd.New(etcd.Config{Endpoints: []string{*etcdEndpoint}})
	if err != nil {
		log.Fatal("failed to create etcd client: ", err)
	}

	e, err = election.New(election.Config{
		Context:       ctx,
		Client:        client,
		ElectionName:  *electionName,
		CandidateName: *candidateName,
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

	for l := range leaderChan {
		log.Infof("leader update: IsLeader: %+v, URI: %s", l.IsLeader, l.URI)
	}

	cancel()
}
