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

// createGRPCConnection creates a gRPC connection with TLS credentials
func createGRPCConnection(certPath, serverName, address string) (*grpc.ClientConn, error) {
	tc, err := credentials.NewClientTLSFromFile(certPath, serverName)
	if err != nil {
		return nil, fmt.Errorf("failed to load TLS credentials: %w", err)
	}

	clientConn, err := grpc.NewClient(address, grpc.WithTransportCredentials(tc))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}

	return clientConn, nil
}

// submitTask submits a task to the task queue and returns the task ID
func submitTask(ctx context.Context, client pb.TaskQueueClient, taskType string, payload []byte, priority pb.Priority, maxRetries int32) (string, error) {
	res, err := client.SubmitTask(ctx, &pb.SubmitTaskRequest{
		Type:       taskType,
		Payload:    payload,
		Priority:   int32(priority),
		MaxRetries: maxRetries,
	})

	if err != nil {
		return "", fmt.Errorf("task failed to submit: %w", err)
	}

	return res.TaskId, nil
}

// pollTaskUntilComplete polls the task status until it completes or times out
func pollTaskUntilComplete(ctx context.Context, client pb.TaskQueueClient, taskID string, pollInterval time.Duration) error {
	for {
		taskStatusRes, err := client.GetTaskStatus(ctx, &pb.GetTaskStatusRequest{
			TaskId: taskID,
		})

		if err != nil {
			return fmt.Errorf("failed to get task status: %w", err)
		}

		if taskStatusRes.Status == pb.TaskStatus_COMPLETED {
			return nil
		}

		if taskStatusRes.Status == pb.TaskStatus_FAILED {
			return fmt.Errorf("task failed")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
			// Continue polling
		}
	}
}

// getTaskResult retrieves the task result
func getTaskResult(ctx context.Context, client pb.TaskQueueClient, taskID string) (*pb.GetTaskResultResponse, error) {
	taskRes, err := client.GetTaskResult(ctx, &pb.GetTaskResultRequest{TaskId: taskID})
	if err != nil {
		return nil, fmt.Errorf("failed to get task result: %w", err)
	}

	return taskRes, nil
}

func main() {
	ctx := context.Background()

	clientConn, err := createGRPCConnection("cert/server.crt", "localhost", "localhost:50051")
	if err != nil {
		fmt.Printf("Connection error: %s\n", err.Error())
		panic(err)
	}
	defer clientConn.Close()

	taskClient := pb.NewTaskQueueClient(clientConn)

	taskID, err := submitTask(ctx, taskClient, "reverseStr", []byte("hello world"), pb.Priority_HIGH, 3)
	if err != nil {
		log.Fatalf("Submit error: %s\n", err.Error())
	}

	fmt.Printf("Task submitted with ID: %s\n", taskID)

	ctxDeadline, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
	defer cancelFunc()

	fmt.Println("Polling task status...")
	err = pollTaskUntilComplete(ctxDeadline, taskClient, taskID, 1*time.Second)
	if err != nil {
		log.Fatalf("Polling error: %s\n", err.Error())
	}

	log.Println("Task completed, fetching result...")

	taskRes, err := getTaskResult(ctx, taskClient, taskID)
	if err != nil {
		log.Fatalf("Get result error: %s\n", err.Error())
	}

	fmt.Printf("Task result: %s\n", taskRes.GetResult())
}
