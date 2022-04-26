package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"github.com/corymonroe-coinbase/hashicorp-benchmark/proto"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	id        = flag.String("id", "", "Node ID used by Raft")
	ip        = flag.String("ip", "localhost", "this nodes ip address")
	nodes     = flag.String("nodes", "localhost", "node ip addresses")
	bootstrap = flag.Bool("bootstrap", false, "Bootstrap a cluster")
)

type EchoRPC struct {
	replicator *raft.Raft
}

func (rpc *EchoRPC) Ping(
	ctx context.Context,
	req *proto.Request,
) (*proto.Request, error) {
	future := rpc.replicator.Apply(req.Payload, 0)
	if err := future.Error(); err != nil {
		return nil, err
	}

	return req, nil
}

type EchoService struct {
}

func (s *EchoService) Apply(log *raft.Log) interface{} {
	return nil
}

func (s *EchoService) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{}, nil
}

func (s *EchoService) Restore(reader io.ReadCloser) error {
	return nil
}

type snapshot struct{}

func (s *snapshot) Persist(sink raft.SnapshotSink) error { return nil }

func (s *snapshot) Release() {}

func main() {
	flag.Parse()

	ctx := context.Background()

	addresses := []string{}
	for i, node := range strings.Split(*nodes, ",") {
		address := node + ":6006" + fmt.Sprintf("%d", i)
		addresses = append(addresses, address)
	}

	grpcaddr := *ip + ":5005" + *id
	raftaddr := *ip + ":6006" + *id
	_, port, err := net.SplitHostPort(grpcaddr)
	if err != nil {
		log.Fatalf("failed to parse address (%q): %v", grpcaddr, err)
	}

	socket, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", port, err)
	}

	service := &EchoService{}

	r, err := NewRaft(ctx, *id, raftaddr, service)
	if err != nil {
		log.Fatalf("failed to start raft: %v", err)
	}

	// Assuming 3 node cluster
	if *bootstrap {
		f := r.BootstrapCluster(
			raft.Configuration{
				Servers: []raft.Server{
					{
						Suffrage: raft.Voter,
						ID:       raft.ServerID("0"),
						Address:  raft.ServerAddress(addresses[0]),
					},
					{
						Suffrage: raft.Voter,
						ID:       raft.ServerID("1"),
						Address:  raft.ServerAddress(addresses[1]),
					},
					{
						Suffrage: raft.Voter,
						ID:       raft.ServerID("2"),
						Address:  raft.ServerAddress(addresses[2]),
					},
				},
			},
		)

		if err := f.Error(); err != nil {
			log.Fatalf("failed to bootstrap cluster: %v", err)
		}
	}

	s := grpc.NewServer()

	reflection.Register(s)

	proto.RegisterPongServer(s, &EchoRPC{replicator: r})
	if err := s.Serve(socket); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func NewRaft(
	ctx context.Context,
	id, address string,
	fsm raft.FSM,
) (*raft.Raft, error) {
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(id)

	advertise, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, err
	}

	transport, err := raft.NewTCPTransport(
		address,
		advertise,
		64,
		5*time.Second,
		log.Writer(),
	)
	if err != nil {
		return nil, err
	}

	r, err := raft.NewRaft(
		c,
		fsm,
		raft.NewInmemStore(),
		raft.NewInmemStore(),
		raft.NewInmemSnapshotStore(),
		transport,
	)

	return r, err
}
