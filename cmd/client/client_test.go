package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/mateusmlo/taskqueue/internal/server"
	pb "github.com/mateusmlo/taskqueue/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

// setupTestServer creates an in-memory test server for testing
func setupTestServer(t *testing.T) (*bufconn.Listener, *grpc.Server, pb.TaskQueueClient, func()) {
	listener := bufconn.Listen(bufSize)

	s := server.NewServer()
	grpcServer := grpc.NewServer()
	pb.RegisterTaskQueueServer(grpcServer, s)
	pb.RegisterWorkerServiceServer(grpcServer, s)

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, url string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	client := pb.NewTaskQueueClient(conn)

	cleanup := func() {
		conn.Close()
		grpcServer.Stop()
		listener.Close()
	}

	return listener, grpcServer, client, cleanup
}

// TestSubmitTask tests the submitTask function
func TestSubmitTask(t *testing.T) {
	_, _, client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	tests := []struct {
		name        string
		taskType    string
		payload     []byte
		priority    pb.Priority
		maxRetries  int32
		expectError bool
	}{
		{
			name:        "valid task submission",
			taskType:    "test-task",
			payload:     []byte("test payload"),
			priority:    pb.Priority_HIGH,
			maxRetries:  3,
			expectError: false,
		},
		{
			name:        "task with medium priority",
			taskType:    "batch-job",
			payload:     []byte("large payload"),
			priority:    pb.Priority_MEDIUM,
			maxRetries:  5,
			expectError: false,
		},
		{
			name:        "task with low priority",
			taskType:    "background-task",
			payload:     []byte("small payload"),
			priority:    pb.Priority_LOW,
			maxRetries:  1,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			taskID, err := submitTask(ctx, client, tt.taskType, tt.payload, tt.priority, tt.maxRetries)

			if tt.expectError && err == nil {
				t.Error("Expected error but got nil")
			}

			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if !tt.expectError && taskID == "" {
				t.Error("Expected non-empty task ID")
			}

			// Verify task was actually submitted by checking status
			if !tt.expectError && taskID != "" {
				statusResp, err := client.GetTaskStatus(ctx, &pb.GetTaskStatusRequest{
					TaskId: taskID,
				})
				if err != nil {
					t.Errorf("Failed to get task status: %v", err)
				}
				if statusResp.Status != pb.TaskStatus_PENDING {
					t.Errorf("Expected status PENDING, got %v", statusResp.Status)
				}
			}
		})
	}
}

// TestPollTaskUntilComplete tests the polling functionality
func TestPollTaskUntilComplete(t *testing.T) {
	_, _, taskClient, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// Submit a task first
	taskID, err := submitTask(ctx, taskClient, "poll-test", []byte("test"), pb.Priority_HIGH, 3)
	if err != nil {
		t.Fatalf("Failed to submit task: %v", err)
	}

	t.Run("timeout on pending task", func(t *testing.T) {
		// Create a context with very short timeout
		ctxTimeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		err := pollTaskUntilComplete(ctxTimeout, taskClient, taskID, 50*time.Millisecond)
		if err == nil {
			t.Error("Expected timeout error but got nil")
		}
		if err != context.DeadlineExceeded {
			t.Errorf("Expected context.DeadlineExceeded, got %v", err)
		}
	})

	t.Run("invalid task ID", func(t *testing.T) {
		err := pollTaskUntilComplete(ctx, taskClient, "invalid-task-id", 50*time.Millisecond)
		if err == nil {
			t.Error("Expected error for invalid task ID but got nil")
		}
	})
}

// TestPollTaskUntilCompleteSuccess tests successful polling
func TestPollTaskUntilCompleteSuccess(t *testing.T) {
	listener, _, taskClient, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// Create worker client
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, url string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to create worker client: %v", err)
	}
	defer conn.Close()

	workerClient := pb.NewWorkerServiceClient(conn)

	// Register a worker
	registerResp, err := workerClient.RegisterWorker(ctx, &pb.RegisterWorkerRequest{
		Worker: &pb.Worker{
			TaskTypes: []string{"complete-test"},
			Capacity:  5,
			Metadata: map[string]string{
				"address": "localhost:8080",
			},
		},
	})
	if err != nil {
		t.Fatalf("Failed to register worker: %v", err)
	}

	// Submit a task
	taskID, err := submitTask(ctx, taskClient, "complete-test", []byte("test"), pb.Priority_HIGH, 3)
	if err != nil {
		t.Fatalf("Failed to submit task: %v", err)
	}

	// Worker fetches and completes the task in background
	go func() {
		time.Sleep(100 * time.Millisecond)

		// Fetch task
		_, err := workerClient.FetchTask(ctx, &pb.FetchTaskRequest{
			WorkerId:  registerResp.WorkerId,
			TaskTypes: []string{"complete-test"},
		})
		if err != nil {
			t.Logf("FetchTask error: %v", err)
			return
		}

		// Submit result
		_, err = workerClient.SubmitResult(ctx, &pb.SubmitResultRequest{
			TaskId: taskID,
			Result: []byte("completed"),
			Error:  "",
		})
		if err != nil {
			t.Logf("SubmitResult error: %v", err)
		}
	}()

	// Poll for completion
	ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err = pollTaskUntilComplete(ctxTimeout, taskClient, taskID, 50*time.Millisecond)
	if err != nil {
		t.Errorf("Expected successful polling, got error: %v", err)
	}
}

// TestGetTaskResult tests retrieving task results
func TestGetTaskResult(t *testing.T) {
	listener, _, taskClient, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// Create worker client
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, url string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to create worker client: %v", err)
	}
	defer conn.Close()

	workerClient := pb.NewWorkerServiceClient(conn)

	// Register worker
	registerResp, err := workerClient.RegisterWorker(ctx, &pb.RegisterWorkerRequest{
		Worker: &pb.Worker{
			TaskTypes: []string{"result-test"},
			Capacity:  5,
			Metadata: map[string]string{
				"address": "localhost:8080",
			},
		},
	})
	if err != nil {
		t.Fatalf("Failed to register worker: %v", err)
	}

	t.Run("get result of completed task", func(t *testing.T) {
		// Submit task
		taskID, err := submitTask(ctx, taskClient, "result-test", []byte("test"), pb.Priority_HIGH, 3)
		if err != nil {
			t.Fatalf("Failed to submit task: %v", err)
		}

		// Worker fetches and completes task
		_, err = workerClient.FetchTask(ctx, &pb.FetchTaskRequest{
			WorkerId:  registerResp.WorkerId,
			TaskTypes: []string{"result-test"},
		})
		if err != nil {
			t.Fatalf("Failed to fetch task: %v", err)
		}

		expectedResult := []byte("task completed successfully")
		_, err = workerClient.SubmitResult(ctx, &pb.SubmitResultRequest{
			TaskId: taskID,
			Result: expectedResult,
			Error:  "",
		})
		if err != nil {
			t.Fatalf("Failed to submit result: %v", err)
		}

		// Get result
		result, err := getTaskResult(ctx, taskClient, taskID)
		if err != nil {
			t.Errorf("Failed to get task result: %v", err)
		}

		if result == nil {
			t.Fatal("Expected non-nil result")
		}

		if string(result.Result) != string(expectedResult) {
			t.Errorf("Expected result %s, got %s", expectedResult, result.Result)
		}

		// Verify task is completed by checking status
		statusResp, err := taskClient.GetTaskStatus(ctx, &pb.GetTaskStatusRequest{
			TaskId: taskID,
		})
		if err != nil {
			t.Errorf("Failed to get task status: %v", err)
		}
		if statusResp.Status != pb.TaskStatus_COMPLETED {
			t.Errorf("Expected status COMPLETED, got %v", statusResp.Status)
		}
	})

	t.Run("get result of pending task", func(t *testing.T) {
		// Submit task but don't complete it
		taskID, err := submitTask(ctx, taskClient, "result-test", []byte("test"), pb.Priority_HIGH, 3)
		if err != nil {
			t.Fatalf("Failed to submit task: %v", err)
		}

		// Try to get result of pending task
		_, err = getTaskResult(ctx, taskClient, taskID)
		if err == nil {
			t.Error("Expected error when getting result of pending task")
		}
	})

	t.Run("get result with invalid task ID", func(t *testing.T) {
		_, err := getTaskResult(ctx, taskClient, "invalid-task-id")
		if err == nil {
			t.Error("Expected error for invalid task ID")
		}
	})
}

// TestSubmitTaskConcurrent tests concurrent task submissions
func TestSubmitTaskConcurrent(t *testing.T) {
	_, _, client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()
	numTasks := 10
	taskIDs := make(chan string, numTasks)
	errors := make(chan error, numTasks)

	for i := range numTasks {
		go func(taskNum int) {
			taskID, err := submitTask(
				ctx,
				client,
				"concurrent-test",
				[]byte("test payload"),
				pb.Priority_MEDIUM,
				3,
			)
			if err != nil {
				errors <- err
			} else {
				taskIDs <- taskID
				errors <- nil
			}
		}(i)
	}

	// Collect results
	uniqueTaskIDs := make(map[string]bool)
	for range numTasks {
		err := <-errors
		if err != nil {
			t.Errorf("Concurrent task submission failed: %v", err)
			continue
		}

		taskID := <-taskIDs
		if taskID == "" {
			t.Error("Got empty task ID")
			continue
		}

		if uniqueTaskIDs[taskID] {
			t.Errorf("Duplicate task ID: %s", taskID)
		}
		uniqueTaskIDs[taskID] = true
	}

	if len(uniqueTaskIDs) != numTasks {
		t.Errorf("Expected %d unique task IDs, got %d", numTasks, len(uniqueTaskIDs))
	}
}

// TestClientWorkflow tests the complete client workflow
func TestClientWorkflow(t *testing.T) {
	listener, _, taskClient, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// Create worker client
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, url string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to create worker client: %v", err)
	}
	defer conn.Close()

	workerClient := pb.NewWorkerServiceClient(conn)

	// Register worker
	registerResp, err := workerClient.RegisterWorker(ctx, &pb.RegisterWorkerRequest{
		Worker: &pb.Worker{
			TaskTypes: []string{"workflow-test"},
			Capacity:  5,
			Metadata: map[string]string{
				"address": "localhost:8080",
			},
		},
	})
	if err != nil {
		t.Fatalf("Failed to register worker: %v", err)
	}

	// Step 1: Submit task
	taskID, err := submitTask(ctx, taskClient, "workflow-test", []byte("hello world"), pb.Priority_HIGH, 3)
	if err != nil {
		t.Fatalf("Failed to submit task: %v", err)
	}

	if taskID == "" {
		t.Fatal("Expected non-empty task ID")
	}

	// Step 2: Worker processes task in background
	go func() {
		time.Sleep(200 * time.Millisecond)

		fetchResp, err := workerClient.FetchTask(ctx, &pb.FetchTaskRequest{
			WorkerId:  registerResp.WorkerId,
			TaskTypes: []string{"workflow-test"},
		})
		if err != nil {
			t.Logf("FetchTask error: %v", err)
			return
		}

		if !fetchResp.HasTask {
			t.Log("No task available")
			return
		}

		// Simulate task processing
		time.Sleep(100 * time.Millisecond)

		_, err = workerClient.SubmitResult(ctx, &pb.SubmitResultRequest{
			TaskId: taskID,
			Result: []byte("dlrow olleh"),
			Error:  "",
		})
		if err != nil {
			t.Logf("SubmitResult error: %v", err)
		}
	}()

	// Step 3: Poll for completion
	ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err = pollTaskUntilComplete(ctxTimeout, taskClient, taskID, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Failed to poll task: %v", err)
	}

	// Step 4: Get result
	result, err := getTaskResult(ctx, taskClient, taskID)
	if err != nil {
		t.Fatalf("Failed to get task result: %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	if string(result.Result) != "dlrow olleh" {
		t.Errorf("Expected result 'dlrow olleh', got '%s'", result.Result)
	}

	// Verify task completed successfully
	statusResp, err := taskClient.GetTaskStatus(ctx, &pb.GetTaskStatusRequest{
		TaskId: taskID,
	})
	if err != nil {
		t.Errorf("Failed to get task status: %v", err)
	}
	if statusResp.Status != pb.TaskStatus_COMPLETED {
		t.Errorf("Expected status COMPLETED, got %v", statusResp.Status)
	}
}
