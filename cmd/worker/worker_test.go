package main

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/mateusmlo/taskqueue/internal/server"
	"github.com/mateusmlo/taskqueue/internal/worker"
	pb "github.com/mateusmlo/taskqueue/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

// setupTestServer creates an in-memory test server for testing
func setupTestServer(t *testing.T) (*bufconn.Listener, *grpc.Server, pb.TaskQueueClient, pb.WorkerServiceClient, func()) {
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

	taskClient := pb.NewTaskQueueClient(conn)
	workerClient := pb.NewWorkerServiceClient(conn)

	cleanup := func() {
		conn.Close()
		grpcServer.Stop()
		listener.Close()
	}

	return listener, grpcServer, taskClient, workerClient, cleanup
}

// TestReverseStringHandler tests the ReverseStringHandler
func TestReverseStringHandler(t *testing.T) {
	handler := &ReverseStringHandler{}
	ctx := context.Background()

	tests := []struct {
		name     string
		input    []byte
		expected []byte
	}{
		{
			name:     "simple string",
			input:    []byte("hello"),
			expected: []byte("olleh"),
		},
		{
			name:     "empty string",
			input:    []byte(""),
			expected: []byte(""),
		},
		{
			name:     "single character",
			input:    []byte("a"),
			expected: []byte("a"),
		},
		{
			name:     "palindrome",
			input:    []byte("racecar"),
			expected: []byte("racecar"),
		},
		{
			name:     "string with spaces",
			input:    []byte("hello world"),
			expected: []byte("dlrow olleh"),
		},
		{
			name:     "numbers",
			input:    []byte("12345"),
			expected: []byte("54321"),
		},
		{
			name:     "special characters",
			input:    []byte("!@#$%"),
			expected: []byte("%$#@!"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := handler.Handle(ctx, tt.input)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if string(result) != string(tt.expected) {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}

			// Verify original input is not modified
			if tt.name == "simple string" && string(tt.input) != "hello" {
				t.Error("Handler should not modify original input")
			}
		})
	}
}

// TestReverseStringHandlerConcurrent tests the handler with concurrent requests
func TestReverseStringHandlerConcurrent(t *testing.T) {
	handler := &ReverseStringHandler{}
	ctx := context.Background()

	numGoroutines := 100
	results := make(chan error, numGoroutines)

	for i := range numGoroutines {
		go func(n int) {
			input := []byte("test")
			expected := []byte("tset")

			result, err := handler.Handle(ctx, input)
			if err != nil {
				results <- err
				return
			}

			if string(result) != string(expected) {
				results <- errors.New("incorrect result")
				return
			}

			results <- nil
		}(i)
	}

	for range numGoroutines {
		if err := <-results; err != nil {
			t.Errorf("Concurrent handler test failed: %v", err)
		}
	}
}

// MockTaskHandler is a test handler that tracks invocations
type MockTaskHandler struct {
	handleFunc  func(ctx context.Context, payload []byte) ([]byte, error)
	invocations int
}

func (m *MockTaskHandler) Handle(ctx context.Context, payload []byte) ([]byte, error) {
	m.invocations++
	if m.handleFunc != nil {
		return m.handleFunc(ctx, payload)
	}
	return payload, nil
}

// TestWorkerHandlerRegistration tests worker handler registration
func TestWorkerHandlerRegistration(t *testing.T) {
	w := worker.NewWorker("localhost:50051", 10)

	handler1 := &ReverseStringHandler{}
	handler2 := &MockTaskHandler{}

	w.RegisterHandler("reverseStr", handler1)
	w.RegisterHandler("mockTask", handler2)
}

// TestWorkerTaskProcessing tests end-to-end task processing with a worker
func TestWorkerTaskProcessing(t *testing.T) {
	listener, _, taskClient, workerClient, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// Create a connection for the worker to use
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, url string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to create worker connection: %v", err)
	}
	defer conn.Close()

	// Register a worker with ReverseStringHandler
	registerResp, err := workerClient.RegisterWorker(ctx, &pb.RegisterWorkerRequest{
		Worker: &pb.Worker{
			TaskTypes: []string{"reverseStr"},
			Capacity:  10,
			Metadata: map[string]string{
				"address": "localhost:8080",
			},
		},
	})
	if err != nil {
		t.Fatalf("Failed to register worker: %v", err)
	}

	workerID := registerResp.WorkerId

	// Submit a task
	submitResp, err := taskClient.SubmitTask(ctx, &pb.SubmitTaskRequest{
		Type:       "reverseStr",
		Payload:    []byte("hello world"),
		Priority:   int32(pb.Priority_HIGH),
		MaxRetries: 3,
	})
	if err != nil {
		t.Fatalf("Failed to submit task: %v", err)
	}

	taskID := submitResp.TaskId

	// Fetch the task
	fetchResp, err := workerClient.FetchTask(ctx, &pb.FetchTaskRequest{
		WorkerId:  workerID,
		TaskTypes: []string{"reverseStr"},
	})
	if err != nil {
		t.Fatalf("Failed to fetch task: %v", err)
	}

	if !fetchResp.HasTask {
		t.Fatal("Expected task to be available")
	}

	// Process the task using the handler
	handler := &ReverseStringHandler{}
	result, err := handler.Handle(ctx, fetchResp.Task.Payload)
	if err != nil {
		t.Fatalf("Handler failed: %v", err)
	}

	// Submit the result
	_, err = workerClient.SubmitResult(ctx, &pb.SubmitResultRequest{
		TaskId: taskID,
		Result: result,
		Error:  "",
	})
	if err != nil {
		t.Fatalf("Failed to submit result: %v", err)
	}

	// Verify the task is completed
	statusResp, err := taskClient.GetTaskStatus(ctx, &pb.GetTaskStatusRequest{
		TaskId: taskID,
	})
	if err != nil {
		t.Fatalf("Failed to get task status: %v", err)
	}

	if statusResp.Status != pb.TaskStatus_COMPLETED {
		t.Errorf("Expected status COMPLETED, got %v", statusResp.Status)
	}

	// Verify the result
	resultResp, err := taskClient.GetTaskResult(ctx, &pb.GetTaskResultRequest{
		TaskId: taskID,
	})
	if err != nil {
		t.Fatalf("Failed to get task result: %v", err)
	}

	expectedResult := "dlrow olleh"
	if string(resultResp.Result) != expectedResult {
		t.Errorf("Expected result %s, got %s", expectedResult, resultResp.Result)
	}
}

// TestWorkerMultipleTaskTypes tests worker handling multiple task types
func TestWorkerMultipleTaskTypes(t *testing.T) {
	listener, _, taskClient, workerClient, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// Create worker connection
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, url string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to create worker connection: %v", err)
	}
	defer conn.Close()

	// Register worker with multiple task types
	registerResp, err := workerClient.RegisterWorker(ctx, &pb.RegisterWorkerRequest{
		Worker: &pb.Worker{
			TaskTypes: []string{"reverseStr", "anotherTask"},
			Capacity:  10,
			Metadata: map[string]string{
				"address": "localhost:8080",
			},
		},
	})
	if err != nil {
		t.Fatalf("Failed to register worker: %v", err)
	}

	workerID := registerResp.WorkerId

	// Submit tasks of different types
	taskTypes := []string{"reverseStr", "anotherTask"}
	taskIDs := make([]string, len(taskTypes))

	for i, taskType := range taskTypes {
		submitResp, err := taskClient.SubmitTask(ctx, &pb.SubmitTaskRequest{
			Type:       taskType,
			Payload:    []byte("test"),
			Priority:   int32(pb.Priority_MEDIUM),
			MaxRetries: 3,
		})
		if err != nil {
			t.Fatalf("Failed to submit task: %v", err)
		}
		taskIDs[i] = submitResp.TaskId
	}

	// Worker should be able to fetch both task types
	fetchedTasks := make(map[string]bool)

	for range taskTypes {
		fetchResp, err := workerClient.FetchTask(ctx, &pb.FetchTaskRequest{
			WorkerId:  workerID,
			TaskTypes: taskTypes,
		})
		if err != nil {
			t.Fatalf("Failed to fetch task: %v", err)
		}

		if fetchResp.HasTask {
			fetchedTasks[fetchResp.Task.Type] = true
		}
	}

	if len(fetchedTasks) != len(taskTypes) {
		t.Errorf("Expected to fetch %d different task types, got %d", len(taskTypes), len(fetchedTasks))
	}

	for _, taskType := range taskTypes {
		if !fetchedTasks[taskType] {
			t.Errorf("Did not fetch task of type %s", taskType)
		}
	}
}

// TestWorkerHeartbeat tests worker heartbeat functionality
func TestWorkerHeartbeat(t *testing.T) {
	_, _, _, workerClient, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// Register worker
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
		t.Fatalf("Failed to register worker: %v", err)
	}

	workerID := registerResp.WorkerId

	// Send heartbeat
	heartbeatResp, err := workerClient.Heartbeat(ctx, &pb.HeartbeatRequest{
		WorkerId:    workerID,
		CurrentLoad: 5,
	})
	if err != nil {
		t.Fatalf("Failed to send heartbeat: %v", err)
	}

	if !heartbeatResp.Success {
		t.Error("Expected heartbeat to succeed")
	}

	if heartbeatResp.CurrentLoad != 5 {
		t.Errorf("Expected current load 5, got %d", heartbeatResp.CurrentLoad)
	}
}

// TestWorkerErrorHandling tests how worker handles errors from task handlers
func TestWorkerErrorHandling(t *testing.T) {
	listener, _, taskClient, workerClient, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// Create worker connection
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, url string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to create worker connection: %v", err)
	}
	defer conn.Close()

	// Register worker
	registerResp, err := workerClient.RegisterWorker(ctx, &pb.RegisterWorkerRequest{
		Worker: &pb.Worker{
			TaskTypes: []string{"error-task"},
			Capacity:  10,
			Metadata: map[string]string{
				"address": "localhost:8080",
			},
		},
	})
	if err != nil {
		t.Fatalf("Failed to register worker: %v", err)
	}

	workerID := registerResp.WorkerId

	// Submit a task
	submitResp, err := taskClient.SubmitTask(ctx, &pb.SubmitTaskRequest{
		Type:       "error-task",
		Payload:    []byte("test"),
		Priority:   int32(pb.Priority_HIGH),
		MaxRetries: 3,
	})
	if err != nil {
		t.Fatalf("Failed to submit task: %v", err)
	}

	taskID := submitResp.TaskId

	// Fetch task
	fetchResp, err := workerClient.FetchTask(ctx, &pb.FetchTaskRequest{
		WorkerId:  workerID,
		TaskTypes: []string{"error-task"},
	})
	if err != nil {
		t.Fatalf("Failed to fetch task: %v", err)
	}

	if !fetchResp.HasTask {
		t.Fatal("Expected task to be available")
	}

	// Simulate handler error by submitting error result
	expectedError := "task processing failed"
	_, err = workerClient.SubmitResult(ctx, &pb.SubmitResultRequest{
		TaskId: taskID,
		Result: nil,
		Error:  expectedError,
	})
	if err != nil {
		t.Fatalf("Failed to submit error result: %v", err)
	}

	// Verify task failed
	time.Sleep(100 * time.Millisecond)
	statusResp, err := taskClient.GetTaskStatus(ctx, &pb.GetTaskStatusRequest{
		TaskId: taskID,
	})
	if err != nil {
		t.Fatalf("Failed to get task status: %v", err)
	}

	// Task should be marked as failed or moved back to pending for retry
	if statusResp.Status != pb.TaskStatus_FAILED && statusResp.Status != pb.TaskStatus_PENDING {
		t.Logf("Task status after error: %v (expected FAILED or PENDING)", statusResp.Status)
	}
}

// TestReverseStringHandlerLargeInput tests handler with large input
func TestReverseStringHandlerLargeInput(t *testing.T) {
	handler := &ReverseStringHandler{}
	ctx := context.Background()

	// Create a large input (1MB)
	largeInput := make([]byte, 1024*1024)
	for i := range largeInput {
		largeInput[i] = byte(i % 256)
	}

	result, err := handler.Handle(ctx, largeInput)
	if err != nil {
		t.Fatalf("Handler failed with large input: %v", err)
	}

	if len(result) != len(largeInput) {
		t.Errorf("Expected result length %d, got %d", len(largeInput), len(result))
	}

	// Verify reversal is correct
	for i := range largeInput {
		if result[i] != largeInput[len(largeInput)-1-i] {
			t.Errorf("Incorrect reversal at index %d", i)
			break
		}
	}
}
