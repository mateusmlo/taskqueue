package main

import (
	"log"
	"net"

	"github.com/mateusmlo/taskqueue/internal/helper"
	"github.com/mateusmlo/taskqueue/internal/server"
	pb "github.com/mateusmlo/taskqueue/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	s := server.NewServer()

	tc, err := credentials.NewServerTLSFromFile("cert/server.crt", "cert/server.key")
	if err != nil {
		panic(err)
	}

	grpcServer := grpc.NewServer(grpc.Creds(tc))

	pb.RegisterTaskQueueServer(grpcServer, s)
	pb.RegisterWorkerServiceServer(grpcServer, s)

	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		panic(err)
	}

	helper.SetupGracefulShutdown(grpcServer.GracefulStop, "SERVER")

	log.Println("✅ Server listening on :50051")
	if err := grpcServer.Serve(listener); err != nil {
		panic(err)
	}
}
