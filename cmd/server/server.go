package main

import (
	"context"
	"flag"
	"log"
	"net"

	"github.com/corymonroe-coinbase/hashicorp-benchmark/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	port = "50055"
)

type EchoServer struct {
}

func (rpc *EchoServer) Ping(
	ctx context.Context,
	req *proto.Request,
) (*proto.Request, error) {
	return req, nil
}

func main() {
	flag.Parse()

	socket, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", port, err)
	}

	s := grpc.NewServer()
	reflection.Register(s)
	proto.RegisterPongServer(s, &EchoServer{})
	if err := s.Serve(socket); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
