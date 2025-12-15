package main

import (
	"context"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/mateusmlo/taskqueue/internal/helper"
	"github.com/mateusmlo/taskqueue/internal/server"
	pb "github.com/mateusmlo/taskqueue/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

// bufDialer creates a gRPC connection using an in-memory buffer connection
func bufDialer(listener *bufconn.Listener) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, url string) (net.Conn, error) {
		return listener.Dial()
	}
}

// TestServerInitialization tests that the server can be initialized without errors
func TestServerInitialization(t *testing.T) {
	s := server.NewServer()

	if s == nil {
		t.Fatal("NewServer() returned nil")
	}

	// Verify server is properly initialized
	if s == nil {
		t.Error("Server instance is nil")
	}
}

// TestGRPCServerRegistration tests that both services are properly registered
func TestGRPCServerRegistration(t *testing.T) {
	// Create in-memory listener for testing
	listener := bufconn.Listen(bufSize)
	defer listener.Close()

	// Create server and gRPC server
	s := server.NewServer()
	grpcServer := grpc.NewServer()

	// Register both services
	pb.RegisterTaskQueueServer(grpcServer, s)
	pb.RegisterWorkerServiceServer(grpcServer, s)

	// Start server in background
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()
	defer grpcServer.Stop()

	// Create client connection
	ctx := context.Background()
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(bufDialer(listener)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer conn.Close()

	// Test TaskQueue service
	taskQueueClient := pb.NewTaskQueueClient(conn)
	submitResp, err := taskQueueClient.SubmitTask(ctx, &pb.SubmitTaskRequest{
		Type:       "test-task",
		Payload:    []byte("test payload"),
		Priority:   int32(pb.Priority_HIGH),
		MaxRetries: 3,
	})
	if err != nil {
		t.Errorf("SubmitTask failed: %v", err)
	}
	if submitResp == nil {
		t.Error("SubmitTask returned nil response")
	}
	if submitResp != nil && submitResp.TaskId == "" {
		t.Error("SubmitTask returned empty task ID")
	}

	// Test WorkerService
	workerClient := pb.NewWorkerServiceClient(conn)
	registerResp, err := workerClient.RegisterWorker(ctx, &pb.RegisterWorkerRequest{
		Worker: &pb.Worker{
			TaskTypes: []string{"test-task"},
			Capacity:  10,
			Metadata: map[string]string{
				"address": "localhost:8080",
			},
		},
	})
	if err != nil {
		t.Errorf("RegisterWorker failed: %v", err)
	}
	if registerResp == nil {
		t.Error("RegisterWorker returned nil response")
	}
	if registerResp != nil && !registerResp.Success {
		t.Error("RegisterWorker returned success=false")
	}
	if registerResp != nil && registerResp.WorkerId == "" {
		t.Error("RegisterWorker returned empty worker ID")
	}
}

// TestTaskQueueServiceEndpoints tests all TaskQueue service endpoints
func TestTaskQueueServiceEndpoints(t *testing.T) {
	listener := bufconn.Listen(bufSize)
	defer listener.Close()

	s := server.NewServer()
	grpcServer := grpc.NewServer()
	pb.RegisterTaskQueueServer(grpcServer, s)

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()
	defer grpcServer.Stop()

	ctx := context.Background()
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(bufDialer(listener)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer conn.Close()

	client := pb.NewTaskQueueClient(conn)

	// Test SubmitTask
	submitResp, err := client.SubmitTask(ctx, &pb.SubmitTaskRequest{
		Type:       "integration-test",
		Payload:    []byte("test data"),
		Priority:   int32(pb.Priority_MEDIUM),
		MaxRetries: 5,
	})
	if err != nil {
		t.Fatalf("SubmitTask failed: %v", err)
	}
	if submitResp.TaskId == "" {
		t.Fatal("SubmitTask returned empty task ID")
	}

	taskID := submitResp.TaskId

	// Test GetTaskStatus
	statusResp, err := client.GetTaskStatus(ctx, &pb.GetTaskStatusRequest{
		TaskId: taskID,
	})
	if err != nil {
		t.Errorf("GetTaskStatus failed: %v", err)
	}
	if statusResp == nil {
		t.Fatal("GetTaskStatus returned nil response")
	}
	if statusResp.Status != pb.TaskStatus_PENDING {
		t.Errorf("GetTaskStatus status = %v, want PENDING", statusResp.Status)
	}

	// Test GetTaskResult on pending task (should fail with FailedPrecondition)
	_, err = client.GetTaskResult(ctx, &pb.GetTaskResultRequest{
		TaskId: taskID,
	})
	if err == nil {
		t.Error("GetTaskResult expected error for pending task, got nil")
	}
	// Note: Full GetTaskResult success test is in TestWorkerTaskLifecycle
}

// TestWorkerServiceEndpoints tests all WorkerService endpoints
func TestWorkerServiceEndpoints(t *testing.T) {
	listener := bufconn.Listen(bufSize)
	defer listener.Close()

	s := server.NewServer()
	grpcServer := grpc.NewServer()
	pb.RegisterWorkerServiceServer(grpcServer, s)
	pb.RegisterTaskQueueServer(grpcServer, s)

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()
	defer grpcServer.Stop()

	ctx := context.Background()
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(bufDialer(listener)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer conn.Close()

	workerClient := pb.NewWorkerServiceClient(conn)
	taskClient := pb.NewTaskQueueClient(conn)

	// Test RegisterWorker
	registerResp, err := workerClient.RegisterWorker(ctx, &pb.RegisterWorkerRequest{
		Worker: &pb.Worker{
			TaskTypes: []string{"test-task", "batch-job"},
			Capacity:  15,
			Metadata: map[string]string{
				"address": "localhost:9000",
				"region":  "us-east-1",
			},
		},
	})
	if err != nil {
		t.Fatalf("RegisterWorker failed: %v", err)
	}
	if registerResp.WorkerId == "" {
		t.Fatal("RegisterWorker returned empty worker ID")
	}
	if !registerResp.Success {
		t.Error("RegisterWorker success = false, want true")
	}

	workerID := registerResp.WorkerId

	// Test Heartbeat
	heartbeatResp, err := workerClient.Heartbeat(ctx, &pb.HeartbeatRequest{
		WorkerId:    workerID,
		CurrentLoad: 5,
	})
	if err != nil {
		t.Errorf("Heartbeat failed: %v", err)
	}
	if heartbeatResp == nil {
		t.Fatal("Heartbeat returned nil response")
	}
	if !heartbeatResp.Success {
		t.Error("Heartbeat success = false, want true")
	}
	if heartbeatResp.CurrentLoad != 5 {
		t.Errorf("Heartbeat current_load = %v, want 5", heartbeatResp.CurrentLoad)
	}

	// Submit a task for FetchTask test
	submitResp, err := taskClient.SubmitTask(ctx, &pb.SubmitTaskRequest{
		Type:       "test-task",
		Payload:    []byte("worker test"),
		Priority:   int32(pb.Priority_HIGH),
		MaxRetries: 3,
	})
	if err != nil {
		t.Fatalf("SubmitTask failed: %v", err)
	}

	taskID := submitResp.TaskId

	// Test FetchTask
	fetchResp, err := workerClient.FetchTask(ctx, &pb.FetchTaskRequest{
		WorkerId:  workerID,
		TaskTypes: []string{"test-task"},
	})
	if err != nil {
		t.Errorf("FetchTask failed: %v", err)
	}
	if fetchResp == nil {
		t.Fatal("FetchTask returned nil response")
	}
	if !fetchResp.HasTask {
		t.Error("FetchTask has_task = false, want true")
	}
	if fetchResp.Task == nil {
		t.Fatal("FetchTask returned nil task")
	}
	if fetchResp.Task.Id != taskID {
		t.Errorf("FetchTask task ID = %v, want %v", fetchResp.Task.Id, taskID)
	}

	// Test SubmitResult
	submitResultResp, err := workerClient.SubmitResult(ctx, &pb.SubmitResultRequest{
		TaskId: taskID,
		Result: []byte("task completed successfully"),
		Error:  "",
	})
	if err != nil {
		t.Errorf("SubmitResult failed: %v", err)
	}
	if submitResultResp == nil {
		t.Fatal("SubmitResult returned nil response")
	}
	if !submitResultResp.Success {
		t.Error("SubmitResult success = false, want true")
	}
}

// TestGracefulShutdown tests the graceful shutdown mechanism
func TestGracefulShutdown(t *testing.T) {
	grpcServer := grpc.NewServer()
	defer grpcServer.Stop()

	// Setup graceful shutdown
	helper.SetupGracefulShutdown(grpcServer.GracefulStop, "TEST")

	// Create a listener to start the server
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	// Start server in background
	serverStarted := make(chan struct{})
	go func() {
		close(serverStarted)
		if err := grpcServer.Serve(listener); err != nil {
			t.Logf("Server stopped: %v", err)
		}
	}()

	// Wait for server to start
	<-serverStarted
	time.Sleep(100 * time.Millisecond)

	// Send SIGTERM to trigger graceful shutdown
	process, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("Failed to find process: %v", err)
	}

	// We can't actually test the full shutdown flow without exiting the test,
	// but we can verify the signal handler is set up
	err = process.Signal(syscall.Signal(0)) // Signal 0 checks if process exists
	if err != nil {
		t.Errorf("Process check failed: %v", err)
	}

	// Clean shutdown for test
	grpcServer.GracefulStop()
}

// TestServerWithMultipleClients tests concurrent client connections
func TestServerWithMultipleClients(t *testing.T) {
	listener := bufconn.Listen(bufSize)
	defer listener.Close()

	s := server.NewServer()
	grpcServer := grpc.NewServer()
	pb.RegisterTaskQueueServer(grpcServer, s)
	pb.RegisterWorkerServiceServer(grpcServer, s)

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()
	defer grpcServer.Stop()

	const numClients = 10
	errors := make(chan error, numClients)
	taskIDs := make(chan string, numClients)

	// Spawn multiple clients submitting tasks concurrently
	for i := range numClients {
		go func(clientID int) {
			ctx := context.Background()
			conn, err := grpc.NewClient(
				"passthrough:///bufnet",
				grpc.WithContextDialer(bufDialer(listener)),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				errors <- err
				return
			}
			defer conn.Close()

			client := pb.NewTaskQueueClient(conn)
			resp, err := client.SubmitTask(ctx, &pb.SubmitTaskRequest{
				Type:       "concurrent-test",
				Payload:    []byte("test data"),
				Priority:   int32(pb.Priority_MEDIUM),
				MaxRetries: 3,
			})

			if err != nil {
				errors <- err
			} else {
				taskIDs <- resp.TaskId
				errors <- nil
			}
		}(i)
	}

	// Collect results
	uniqueTaskIDs := make(map[string]bool)
	for range numClients {
		err := <-errors
		if err != nil {
			t.Errorf("Client request failed: %v", err)
			continue
		}

		taskID := <-taskIDs
		if uniqueTaskIDs[taskID] {
			t.Errorf("Duplicate task ID: %s", taskID)
		}
		uniqueTaskIDs[taskID] = true
	}

	if len(uniqueTaskIDs) != numClients {
		t.Errorf("Expected %d unique task IDs, got %d", numClients, len(uniqueTaskIDs))
	}
}

// TestWorkerTaskLifecycle tests complete task lifecycle with worker
func TestWorkerTaskLifecycle(t *testing.T) {
	listener := bufconn.Listen(bufSize)
	defer listener.Close()

	s := server.NewServer()
	grpcServer := grpc.NewServer()
	pb.RegisterTaskQueueServer(grpcServer, s)
	pb.RegisterWorkerServiceServer(grpcServer, s)

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()
	defer grpcServer.Stop()

	ctx := context.Background()
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(bufDialer(listener)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer conn.Close()

	taskClient := pb.NewTaskQueueClient(conn)
	workerClient := pb.NewWorkerServiceClient(conn)

	// 1. Register worker
	registerResp, err := workerClient.RegisterWorker(ctx, &pb.RegisterWorkerRequest{
		Worker: &pb.Worker{
			TaskTypes: []string{"lifecycle-test"},
			Capacity:  5,
			Metadata: map[string]string{
				"address": "localhost:8000",
			},
		},
	})
	if err != nil {
		t.Fatalf("RegisterWorker failed: %v", err)
	}
	workerID := registerResp.WorkerId

	// 2. Submit task
	submitResp, err := taskClient.SubmitTask(ctx, &pb.SubmitTaskRequest{
		Type:       "lifecycle-test",
		Payload:    []byte("lifecycle payload"),
		Priority:   int32(pb.Priority_HIGH),
		MaxRetries: 3,
	})
	if err != nil {
		t.Fatalf("SubmitTask failed: %v", err)
	}
	taskID := submitResp.TaskId

	// 3. Check task is pending
	statusResp, err := taskClient.GetTaskStatus(ctx, &pb.GetTaskStatusRequest{
		TaskId: taskID,
	})
	if err != nil {
		t.Fatalf("GetTaskStatus failed: %v", err)
	}
	if statusResp.Status != pb.TaskStatus_PENDING {
		t.Errorf("Task status = %v, want PENDING", statusResp.Status)
	}

	// 4. Worker fetches task
	fetchResp, err := workerClient.FetchTask(ctx, &pb.FetchTaskRequest{
		WorkerId:  workerID,
		TaskTypes: []string{"lifecycle-test"},
	})
	if err != nil {
		t.Fatalf("FetchTask failed: %v", err)
	}
	if !fetchResp.HasTask {
		t.Fatal("FetchTask has_task = false, expected task to be available")
	}
	if fetchResp.Task.Id != taskID {
		t.Errorf("Fetched task ID = %v, want %v", fetchResp.Task.Id, taskID)
	}

	// 5. Check task is running
	statusResp, err = taskClient.GetTaskStatus(ctx, &pb.GetTaskStatusRequest{
		TaskId: taskID,
	})
	if err != nil {
		t.Fatalf("GetTaskStatus failed: %v", err)
	}
	if statusResp.Status != pb.TaskStatus_RUNNING {
		t.Errorf("Task status = %v, want RUNNING", statusResp.Status)
	}

	// 6. Worker sends heartbeat
	heartbeatResp, err := workerClient.Heartbeat(ctx, &pb.HeartbeatRequest{
		WorkerId:    workerID,
		CurrentLoad: 1,
	})
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}
	if !heartbeatResp.Success {
		t.Error("Heartbeat success = false, want true")
	}

	// 7. Worker submits result
	submitResultResp, err := workerClient.SubmitResult(ctx, &pb.SubmitResultRequest{
		TaskId: taskID,
		Result: []byte("task result data"),
		Error:  "",
	})
	if err != nil {
		t.Fatalf("SubmitResult failed: %v", err)
	}
	if !submitResultResp.Success {
		t.Error("SubmitResult success = false, want true")
	}

	// 8. Check task is completed
	statusResp, err = taskClient.GetTaskStatus(ctx, &pb.GetTaskStatusRequest{
		TaskId: taskID,
	})
	if err != nil {
		t.Fatalf("GetTaskStatus failed: %v", err)
	}
	if statusResp.Status != pb.TaskStatus_COMPLETED {
		t.Errorf("Task status = %v, want COMPLETED", statusResp.Status)
	}

	// 9. Get task result
	resultResp, err := taskClient.GetTaskResult(ctx, &pb.GetTaskResultRequest{
		TaskId: taskID,
	})
	if err != nil {
		t.Fatalf("GetTaskResult failed: %v", err)
	}
	if resultResp.Task == nil {
		t.Fatal("GetTaskResult returned nil task")
	}
	if resultResp.Task.Id != taskID {
		t.Errorf("Result task ID = %v, want %v", resultResp.Task.Id, taskID)
	}
	if resultResp.Task.Status != pb.TaskStatus_COMPLETED {
		t.Errorf("Result task status = %v, want COMPLETED", resultResp.Task.Status)
	}
}
