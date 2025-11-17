package worker

import (
	"context"
	"errors"
	"testing"
	"time"
)

// MockHandler is a mock implementation of TaskHandler for testing
type MockHandler struct {
	handleFunc func(ctx context.Context, payload []byte) ([]byte, error)
}

func (m *MockHandler) Handle(ctx context.Context, payload []byte) ([]byte, error) {
	if m.handleFunc != nil {
		return m.handleFunc(ctx, payload)
	}
	return payload, nil
}

func TestNewWorker(t *testing.T) {
	tests := []struct {
		name       string
		serverAddr string
		capacity   int
	}{
		{
			name:       "basic worker",
			serverAddr: "localhost:8080",
			capacity:   5,
		},
		{
			name:       "high capacity worker",
			serverAddr: "localhost:8080",
			capacity:   10,
		},
		{
			name:       "single capacity worker",
			serverAddr: "localhost:8080",
			capacity:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			worker := NewWorker(tt.serverAddr, tt.capacity)

			if worker == nil {
				t.Fatal("NewWorker returned nil")
			}

			if worker.serverAddr != tt.serverAddr {
				t.Errorf("expected serverAddr %s, got %s", tt.serverAddr, worker.serverAddr)
			}

			if worker.capacity != tt.capacity {
				t.Errorf("expected capacity %d, got %d", tt.capacity, worker.capacity)
			}

			if worker.handlers == nil {
				t.Error("handlers map not initialized")
			}

			if worker.ctx == nil {
				t.Error("context not initialized")
			}

			if worker.cancel == nil {
				t.Error("cancel function not initialized")
			}

			if worker.currentLoad != 0 {
				t.Errorf("expected currentLoad 0, got %d", worker.currentLoad)
			}

			// Verify handlers map is initialized empty
			if len(worker.handlers) != 0 {
				t.Errorf("expected empty handlers map, got %d handlers", len(worker.handlers))
			}
		})
	}
}

func TestRegisterHandler(t *testing.T) {
	worker := NewWorker("localhost:8080", 5)

	handler1 := &MockHandler{}
	handler2 := &MockHandler{}

	worker.RegisterHandler("task1", handler1)
	worker.RegisterHandler("task2", handler2)

	if worker.handlers["task1"] != handler1 {
		t.Error("handler1 not registered correctly")
	}

	if worker.handlers["task2"] != handler2 {
		t.Error("handler2 not registered correctly")
	}
}

func TestGetCurrentLoad(t *testing.T) {
	worker := NewWorker("localhost:8080", 5)

	// Initial load should be 0
	if worker.getCurrentLoad() != 0 {
		t.Errorf("expected initial load 0, got %d", worker.getCurrentLoad())
	}

	// Manually set load to test getter
	worker.currentLoad = 3
	if worker.getCurrentLoad() != 3 {
		t.Errorf("expected load 3, got %d", worker.getCurrentLoad())
	}
}

func TestIncrementLoad(t *testing.T) {
	worker := NewWorker("localhost:8080", 5)

	if worker.getCurrentLoad() != 0 {
		t.Errorf("expected initial load 0, got %d", worker.getCurrentLoad())
	}

	worker.incrementLoad()
	if worker.getCurrentLoad() != 1 {
		t.Errorf("expected load 1, got %d", worker.getCurrentLoad())
	}

	worker.incrementLoad()
	if worker.getCurrentLoad() != 2 {
		t.Errorf("expected load 2, got %d", worker.getCurrentLoad())
	}

	worker.incrementLoad()
	if worker.getCurrentLoad() != 3 {
		t.Errorf("expected load 3, got %d", worker.getCurrentLoad())
	}
}

func TestDecrementLoad(t *testing.T) {
	worker := NewWorker("localhost:8080", 5)

	// Set initial load
	worker.currentLoad = 3

	worker.decrementLoad()
	if worker.getCurrentLoad() != 2 {
		t.Errorf("expected load 2, got %d", worker.getCurrentLoad())
	}

	worker.decrementLoad()
	if worker.getCurrentLoad() != 1 {
		t.Errorf("expected load 1, got %d", worker.getCurrentLoad())
	}

	worker.decrementLoad()
	if worker.getCurrentLoad() != 0 {
		t.Errorf("expected load 0, got %d", worker.getCurrentLoad())
	}

	// Decrementing below 0 should keep it at 0
	worker.decrementLoad()
	if worker.getCurrentLoad() != 0 {
		t.Errorf("expected load to stay at 0, got %d", worker.getCurrentLoad())
	}
}

func TestIncrementDecrementLoadConcurrency(t *testing.T) {
	worker := NewWorker("localhost:8080", 100)

	done := make(chan bool)

	// Simulate concurrent increments
	for i := 0; i < 50; i++ {
		go func() {
			worker.incrementLoad()
			done <- true
		}()
	}

	// Wait for all increments
	for i := 0; i < 50; i++ {
		<-done
	}

	if worker.getCurrentLoad() != 50 {
		t.Errorf("expected load 50 after increments, got %d", worker.getCurrentLoad())
	}

	// Simulate concurrent decrements
	for i := 0; i < 30; i++ {
		go func() {
			worker.decrementLoad()
			done <- true
		}()
	}

	// Wait for all decrements
	for i := 0; i < 30; i++ {
		<-done
	}

	if worker.getCurrentLoad() != 20 {
		t.Errorf("expected load 20 after decrements, got %d", worker.getCurrentLoad())
	}
}

func TestBuildRegisterRequest(t *testing.T) {
	worker := NewWorker("localhost:8080", 5)
	worker.RegisterHandler("task1", &MockHandler{})
	worker.RegisterHandler("task2", &MockHandler{})

	req := worker.buildRegisterRequest()

	if req == nil {
		t.Fatal("buildRegisterRequest returned nil")
	}

	if req.Worker == nil {
		t.Fatal("Worker in request is nil")
	}

	if len(req.Worker.TaskTypes) != 2 {
		t.Errorf("expected 2 task types, got %d", len(req.Worker.TaskTypes))
	}

	if req.Worker.Capacity != 5 {
		t.Errorf("expected capacity 5, got %d", req.Worker.Capacity)
	}

	if req.Worker.Metadata == nil {
		t.Fatal("Metadata is nil")
	}

	if req.Worker.Metadata["address"] != "localhost:8080" {
		t.Errorf("expected address localhost:8080, got %s", req.Worker.Metadata["address"])
	}
}

func TestBuildFetchTasksRequest(t *testing.T) {
	worker := NewWorker("localhost:8080", 5)
	worker.RegisterHandler("task1", &MockHandler{})
	worker.RegisterHandler("task2", &MockHandler{})
	worker.id = "worker-123"

	req := worker.buildFetchTasksRequest()

	if req == nil {
		t.Fatal("buildFetchTasksRequest returned nil")
	}

	if req.WorkerId != "worker-123" {
		t.Errorf("expected WorkerId worker-123, got %s", req.WorkerId)
	}

	if len(req.TaskTypes) != 2 {
		t.Errorf("expected 2 task types, got %d", len(req.TaskTypes))
	}
}

func TestBuildHeartbeatRequest(t *testing.T) {
	worker := NewWorker("localhost:8080", 5)
	worker.id = "worker-123"
	worker.currentLoad = 3

	req := worker.buildHeartbeatRequest()

	if req == nil {
		t.Fatal("buildHeartbeatRequest returned nil")
	}

	if req.WorkerId != "worker-123" {
		t.Errorf("expected WorkerId worker-123, got %s", req.WorkerId)
	}

	if req.CurrentLoad != 3 {
		t.Errorf("expected CurrentLoad 3, got %d", req.CurrentLoad)
	}
}

func TestGetTaskHandler(t *testing.T) {
	worker := NewWorker("localhost:8080", 5)

	expectedPayload := []byte("test payload")
	expectedResult := []byte("test result")

	mockHandler := &MockHandler{
		handleFunc: func(ctx context.Context, payload []byte) ([]byte, error) {
			if string(payload) != string(expectedPayload) {
				t.Errorf("expected payload %s, got %s", expectedPayload, payload)
			}
			return expectedResult, nil
		},
	}

	worker.RegisterHandler("task1", mockHandler)

	taskHandler := worker.getTaskHandler(mockHandler)

	if taskHandler == nil {
		t.Fatal("getTaskHandler returned nil")
	}

	// Note: We can't fully test the task handler without mocking the gRPC client
	// but we can verify it returns a function
}

func TestGetTaskHandlerWithError(t *testing.T) {
	worker := NewWorker("localhost:8080", 5)

	expectedError := errors.New("handler error")

	mockHandler := &MockHandler{
		handleFunc: func(ctx context.Context, payload []byte) ([]byte, error) {
			return nil, expectedError
		},
	}

	taskHandler := worker.getTaskHandler(mockHandler)

	if taskHandler == nil {
		t.Fatal("getTaskHandler returned nil")
	}
}

func TestStop(t *testing.T) {
	worker := NewWorker("localhost:8080", 5)

	// Start a goroutine that should be cancelled by Stop
	done := make(chan bool)
	go func() {
		<-worker.ctx.Done()
		done <- true
	}()

	worker.Stop()

	// Wait for context cancellation with timeout
	select {
	case <-done:
		// Success - context was cancelled
	case <-time.After(1 * time.Second):
		t.Fatal("Context was not cancelled within timeout")
	}
}

func TestHandlersInitializedEmpty(t *testing.T) {
	worker := NewWorker("localhost:8080", 5)

	// Verify handlers map is initialized but empty
	if worker.handlers == nil {
		t.Fatal("handlers map should be initialized")
	}

	if len(worker.handlers) != 0 {
		t.Errorf("handlers map should be empty, got %d handlers", len(worker.handlers))
	}
}

func TestRegisterHandlerOverwrite(t *testing.T) {
	worker := NewWorker("localhost:8080", 5)

	handler1 := &MockHandler{}
	handler2 := &MockHandler{}

	worker.RegisterHandler("task1", handler1)
	if worker.handlers["task1"] != handler1 {
		t.Error("handler1 not registered correctly")
	}

	// Overwrite with handler2
	worker.RegisterHandler("task1", handler2)
	if worker.handlers["task1"] != handler2 {
		t.Error("handler2 not registered correctly after overwrite")
	}
}

func TestWorkerContextCancellation(t *testing.T) {
	worker := NewWorker("localhost:8080", 5)

	if worker.ctx == nil {
		t.Fatal("worker context is nil")
	}

	if worker.cancel == nil {
		t.Fatal("worker cancel function is nil")
	}

	// Initially context should not be cancelled
	select {
	case <-worker.ctx.Done():
		t.Fatal("Context should not be cancelled initially")
	default:
		// Good - context is not cancelled
	}

	// Cancel the context
	worker.cancel()

	// Now context should be cancelled
	select {
	case <-worker.ctx.Done():
		// Good - context is cancelled
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Context was not cancelled")
	}
}

func TestLoadMutexProtection(t *testing.T) {
	worker := NewWorker("localhost:8080", 100)

	// Test that concurrent reads and writes don't cause race conditions
	done := make(chan bool)
	iterations := 100

	// Concurrent readers
	for i := 0; i < iterations; i++ {
		go func() {
			_ = worker.getCurrentLoad()
			done <- true
		}()
	}

	// Concurrent writers (increments)
	for i := 0; i < iterations; i++ {
		go func() {
			worker.incrementLoad()
			done <- true
		}()
	}

	// Concurrent writers (decrements)
	for i := 0; i < iterations; i++ {
		go func() {
			worker.decrementLoad()
			done <- true
		}()
	}

	// Wait for all operations
	for i := 0; i < iterations*3; i++ {
		<-done
	}

	// Just verify we can read the final load without panic
	finalLoad := worker.getCurrentLoad()
	if finalLoad < 0 {
		t.Errorf("final load should not be negative, got %d", finalLoad)
	}
}

func TestMockHandlerDefaultBehavior(t *testing.T) {
	handler := &MockHandler{}
	payload := []byte("test")

	result, err := handler.Handle(context.Background(), payload)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if string(result) != string(payload) {
		t.Errorf("expected result %s, got %s", payload, result)
	}
}

func TestMockHandlerCustomBehavior(t *testing.T) {
	expectedError := errors.New("custom error")
	handler := &MockHandler{
		handleFunc: func(ctx context.Context, payload []byte) ([]byte, error) {
			return nil, expectedError
		},
	}

	result, err := handler.Handle(context.Background(), []byte("test"))

	if err != expectedError {
		t.Errorf("expected error %v, got %v", expectedError, err)
	}

	if result != nil {
		t.Errorf("expected nil result, got %v", result)
	}
}

func TestWorkerHandlerRegistration(t *testing.T) {
	worker := NewWorker("localhost:8080", 5)

	// Register multiple handlers
	taskTypes := []string{"type1", "type2", "type3"}
	for _, taskType := range taskTypes {
		worker.RegisterHandler(taskType, &MockHandler{})
	}

	// Verify all handlers are registered
	if len(worker.handlers) != len(taskTypes) {
		t.Errorf("expected %d handlers, got %d", len(taskTypes), len(worker.handlers))
	}

	for _, taskType := range taskTypes {
		if _, exists := worker.handlers[taskType]; !exists {
			t.Errorf("handler for task type %s should be registered", taskType)
		}
	}
}
