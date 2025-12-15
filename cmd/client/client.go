package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/mateusmlo/taskqueue/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	ctx := context.Background()
	tc, err := credentials.NewClientTLSFromFile("cert/server.crt", "localhost")
	if err != nil {
		fmt.Printf("Failed to load TLS credentials: %s\n", err.Error())
		panic(err)
	}

	clientConn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(tc))
	if err != nil {
		fmt.Printf("Failed to connect to server: %s\n", err.Error())
		panic(err)
	}
	defer clientConn.Close()

	taskClient := pb.NewTaskQueueClient(clientConn)

	res, err := taskClient.SubmitTask(ctx, &pb.SubmitTaskRequest{
		Type:       "reverseStr",
		Payload:    []byte("hello world"),
		Priority:   int32(pb.Priority_HIGH),
		MaxRetries: 3,
	})

	if err != nil {
		log.Fatalf("Task failed to submit: %s\n", err.Error())
	}

	taskID := res.TaskId

	ctxDeadline, cancelFunc := context.WithTimeout(ctx, 10*time.Second)

	for {
		fmt.Print("Polling task status...\n")

		taskStatusRes, err := taskClient.GetTaskStatus(ctxDeadline, &pb.GetTaskStatusRequest{
			TaskId: taskID,
		})

		if err != nil {
			log.Fatalf("Failed to get task status: %s\n", err.Error())
		}

		if taskStatusRes.Status == pb.TaskStatus_COMPLETED {
			log.Print("Task completed, fetching result...\n")
			cancelFunc()
			break
		}

		time.Sleep(1 * time.Second)
	}

	taskRes, err := taskClient.GetTaskResult(ctx, &pb.GetTaskResultRequest{TaskId: taskID})
	if err != nil {
		log.Fatalf("Failed to get task result: %s\n", err.Error())
		panic(err)
	}

	fmt.Printf("Task result: %s\n", taskRes.GetResult())
}
