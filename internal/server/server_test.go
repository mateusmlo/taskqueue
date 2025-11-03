package internal

import (
	"context"
	"testing"
	"time"

	"github.com/mateusmlo/taskqueue/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNewServer(t *testing.T) {
	s := NewServer()

	if s == nil {
		t.Fatal("NewServer returned nil")
	}

	if s.tasks == nil {
		t.Error("tasks map not initialized")
	}

	if s.pendingQueues == nil {
		t.Error("pendingQueues map not initialized")
	}

	if s.workers == nil {
		t.Error("workers map not initialized")
	}

	if s.ctx == nil {
		t.Error("context not initialized")
	}

	if s.cancel == nil {
		t.Error("cancel function not initialized")
	}

	// Verify maps are empty
	if len(s.tasks) != 0 {
		t.Errorf("tasks map should be empty, got %d items", len(s.tasks))
	}

	if len(s.workers) != 0 {
		t.Errorf("workers map should be empty, got %d items", len(s.workers))
	}

	// Clean up context
	s.cancel()
}

func TestTask_toProtoTask(t *testing.T) {
	now := time.Now()
	startedAt := now.Add(1 * time.Minute)
	completedAt := now.Add(5 * time.Minute)

	tests := []struct {
		name string
		task *Task
		want *proto.Task
	}{
		{
			name: "task with all fields set",
			task: &Task{
				ID:          "task-123",
				Type:        "image-processing",
				Payload:     []byte("test payload"),
				Priority:    HIGH,
				Status:      COMPLETED,
				RetryCount:  2,
				MaxRetries:  5,
				CreatedAt:   now,
				StartedAt:   &startedAt,
				CompletedAt: &completedAt,
			},
			want: &proto.Task{
				Id:          "task-123",
				Type:        "image-processing",
				Payload:     []byte("test payload"),
				Priority:    proto.Priority_HIGH,
				Status:      proto.TaskStatus_COMPLETED,
				RetryCount:  2,
				MaxRetries:  5,
				CreatedAt:   timestamppb.New(now),
				StartedAt:   timestamppb.New(startedAt),
				CompletedAt: timestamppb.New(completedAt),
			},
		},
		{
			name: "pending task without optional timestamps",
			task: &Task{
				ID:          "task-456",
				Type:        "data-export",
				Payload:     []byte("{}"),
				Priority:    MEDIUM,
				Status:      PENDING,
				RetryCount:  0,
				MaxRetries:  3,
				CreatedAt:   now,
				StartedAt:   nil,
				CompletedAt: nil,
			},
			want: &proto.Task{
				Id:          "task-456",
				Type:        "data-export",
				Payload:     []byte("{}"),
				Priority:    proto.Priority_MEDIUM,
				Status:      proto.TaskStatus_PENDING,
				RetryCount:  0,
				MaxRetries:  3,
				CreatedAt:   timestamppb.New(now),
				StartedAt:   nil,
				CompletedAt: nil,
			},
		},
		{
			name: "running task with only startedAt",
			task: &Task{
				ID:          "task-789",
				Type:        "email-send",
				Payload:     nil,
				Priority:    LOW,
				Status:      RUNNING,
				RetryCount:  1,
				MaxRetries:  10,
				CreatedAt:   now,
				StartedAt:   &startedAt,
				CompletedAt: nil,
			},
			want: &proto.Task{
				Id:          "task-789",
				Type:        "email-send",
				Payload:     nil,
				Priority:    proto.Priority_LOW,
				Status:      proto.TaskStatus_RUNNING,
				RetryCount:  1,
				MaxRetries:  10,
				CreatedAt:   timestamppb.New(now),
				StartedAt:   timestamppb.New(startedAt),
				CompletedAt: nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.task.toProtoTask()

			if got.Id != tt.want.Id {
				t.Errorf("Id = %v, want %v", got.Id, tt.want.Id)
			}
			if got.Type != tt.want.Type {
				t.Errorf("Type = %v, want %v", got.Type, tt.want.Type)
			}
			if string(got.Payload) != string(tt.want.Payload) {
				t.Errorf("Payload = %v, want %v", got.Payload, tt.want.Payload)
			}
			if got.Priority != tt.want.Priority {
				t.Errorf("Priority = %v, want %v", got.Priority, tt.want.Priority)
			}
			if got.Status != tt.want.Status {
				t.Errorf("Status = %v, want %v", got.Status, tt.want.Status)
			}
			if got.RetryCount != tt.want.RetryCount {
				t.Errorf("RetryCount = %v, want %v", got.RetryCount, tt.want.RetryCount)
			}
			if got.MaxRetries != tt.want.MaxRetries {
				t.Errorf("MaxRetries = %v, want %v", got.MaxRetries, tt.want.MaxRetries)
			}

			// Check timestamps
			if !got.CreatedAt.AsTime().Equal(tt.want.CreatedAt.AsTime()) {
				t.Errorf("CreatedAt = %v, want %v", got.CreatedAt.AsTime(), tt.want.CreatedAt.AsTime())
			}

			if (got.StartedAt == nil) != (tt.want.StartedAt == nil) {
				t.Errorf("StartedAt nil mismatch: got %v, want %v", got.StartedAt, tt.want.StartedAt)
			} else if got.StartedAt != nil && !got.StartedAt.AsTime().Equal(tt.want.StartedAt.AsTime()) {
				t.Errorf("StartedAt = %v, want %v", got.StartedAt.AsTime(), tt.want.StartedAt.AsTime())
			}

			if (got.CompletedAt == nil) != (tt.want.CompletedAt == nil) {
				t.Errorf("CompletedAt nil mismatch: got %v, want %v", got.CompletedAt, tt.want.CompletedAt)
			} else if got.CompletedAt != nil && !got.CompletedAt.AsTime().Equal(tt.want.CompletedAt.AsTime()) {
				t.Errorf("CompletedAt = %v, want %v", got.CompletedAt.AsTime(), tt.want.CompletedAt.AsTime())
			}
		})
	}
}

func TestServer_SubmitTask(t *testing.T) {
	s := NewServer()
	defer s.cancel()
	ctx := context.Background()

	tests := []struct {
		name    string
		req     *proto.SubmitTaskRequest
		wantErr bool
	}{
		{
			name: "valid task submission",
			req: &proto.SubmitTaskRequest{
				Type:       "test-task",
				Payload:    []byte("test data"),
				Priority:   int32(proto.Priority_HIGH),
				MaxRetries: 3,
			},
			wantErr: false,
		},
		{
			name: "task with empty payload",
			req: &proto.SubmitTaskRequest{
				Type:       "empty-task",
				Payload:    []byte{},
				Priority:   int32(proto.Priority_MEDIUM),
				MaxRetries: 5,
			},
			wantErr: false,
		},
		{
			name: "low priority task",
			req: &proto.SubmitTaskRequest{
				Type:       "batch-job",
				Payload:    []byte("large dataset"),
				Priority:   int32(proto.Priority_LOW),
				MaxRetries: 10,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := s.SubmitTask(ctx, tt.req)

			if (err != nil) != tt.wantErr {
				t.Errorf("SubmitTask() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if resp == nil {
					t.Fatal("SubmitTask() returned nil response")
				}

				if resp.TaskId == "" {
					t.Error("SubmitTask() returned empty task ID")
				}

				// Verify task was stored
				s.tasksMux.RLock()
				task, exists := s.tasks[resp.TaskId]
				s.tasksMux.RUnlock()

				if !exists {
					t.Error("Task was not stored in server.tasks")
				}

				if task.Type != tt.req.Type {
					t.Errorf("Task.Type = %v, want %v", task.Type, tt.req.Type)
				}

				if string(task.Payload) != string(tt.req.Payload) {
					t.Errorf("Task.Payload = %v, want %v", task.Payload, tt.req.Payload)
				}

				if task.Priority != Priority(tt.req.Priority) {
					t.Errorf("Task.Priority = %v, want %v", task.Priority, tt.req.Priority)
				}

				if task.Status != PENDING {
					t.Errorf("Task.Status = %v, want PENDING", task.Status)
				}

				if task.MaxRetries != int(tt.req.MaxRetries) {
					t.Errorf("Task.MaxRetries = %v, want %v", task.MaxRetries, tt.req.MaxRetries)
				}

				if task.RetryCount != 0 {
					t.Errorf("Task.RetryCount = %v, want 0", task.RetryCount)
				}

				// Verify task was added to pending queue
				s.queuesMux.RLock()
				queue := s.pendingQueues[task.Priority]
				s.queuesMux.RUnlock()

				found := false
				for _, qTask := range queue {
					if qTask.ID == task.ID {
						found = true
						break
					}
				}

				if !found {
					t.Error("Task was not added to pending queue")
				}
			}
		})
	}
}

func TestServer_GetTaskStatus(t *testing.T) {
	s := NewServer()
	defer s.cancel()
	ctx := context.Background()

	// Submit a test task
	submitReq := &proto.SubmitTaskRequest{
		Type:       "test-task",
		Payload:    []byte("test"),
		Priority:   int32(proto.Priority_HIGH),
		MaxRetries: 3,
	}
	submitResp, err := s.SubmitTask(ctx, submitReq)
	if err != nil {
		t.Fatalf("Failed to submit test task: %v", err)
	}

	tests := []struct {
		name       string
		taskID     string
		wantStatus proto.TaskStatus
		wantErr    bool
		wantCode   codes.Code
	}{
		{
			name:       "get status of existing task",
			taskID:     submitResp.TaskId,
			wantStatus: proto.TaskStatus_PENDING,
			wantErr:    false,
		},
		{
			name:     "get status of non-existent task",
			taskID:   "non-existent-id",
			wantErr:  true,
			wantCode: codes.NotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &proto.GetTaskStatusRequest{
				TaskId: tt.taskID,
			}

			resp, err := s.GetTaskStatus(ctx, req)

			if tt.wantErr {
				if err == nil {
					t.Error("GetTaskStatus() expected error, got nil")
					return
				}

				st, ok := status.FromError(err)
				if !ok {
					t.Error("Error is not a gRPC status error")
					return
				}

				if st.Code() != tt.wantCode {
					t.Errorf("GetTaskStatus() error code = %v, want %v", st.Code(), tt.wantCode)
				}
			} else {
				if err != nil {
					t.Errorf("GetTaskStatus() unexpected error = %v", err)
					return
				}

				if resp == nil {
					t.Fatal("GetTaskStatus() returned nil response")
				}

				if resp.Status != tt.wantStatus {
					t.Errorf("GetTaskStatus() status = %v, want %v", resp.Status, tt.wantStatus)
				}
			}
		})
	}
}

func TestServer_GetTaskResult(t *testing.T) {
	s := NewServer()
	defer s.cancel()
	ctx := context.Background()

	// Submit a test task
	submitReq := &proto.SubmitTaskRequest{
		Type:       "test-task",
		Payload:    []byte("test"),
		Priority:   int32(proto.Priority_HIGH),
		MaxRetries: 3,
	}
	submitResp, err := s.SubmitTask(ctx, submitReq)
	if err != nil {
		t.Fatalf("Failed to submit test task: %v", err)
	}

	// Mark task as completed for one test
	s.tasksMux.Lock()
	completedTask := s.tasks[submitResp.TaskId]
	completedTask.Status = COMPLETED
	now := time.Now()
	completedTask.CompletedAt = &now
	s.tasksMux.Unlock()

	// Create another pending task
	submitReq2 := &proto.SubmitTaskRequest{
		Type:       "pending-task",
		Payload:    []byte("pending"),
		Priority:   int32(proto.Priority_MEDIUM),
		MaxRetries: 2,
	}
	submitResp2, err := s.SubmitTask(ctx, submitReq2)
	if err != nil {
		t.Fatalf("Failed to submit second test task: %v", err)
	}

	tests := []struct {
		name     string
		taskID   string
		wantErr  bool
		wantCode codes.Code
	}{
		{
			name:    "get result of completed task",
			taskID:  submitResp.TaskId,
			wantErr: false,
		},
		{
			name:     "get result of pending task",
			taskID:   submitResp2.TaskId,
			wantErr:  true,
			wantCode: codes.FailedPrecondition,
		},
		{
			name:     "get result of non-existent task",
			taskID:   "non-existent-id",
			wantErr:  true,
			wantCode: codes.NotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &proto.GetTaskResultRequest{
				TaskId: tt.taskID,
			}

			resp, err := s.GetTaskResult(ctx, req)

			if tt.wantErr {
				if err == nil {
					t.Error("GetTaskResult() expected error, got nil")
					return
				}

				st, ok := status.FromError(err)
				if !ok {
					t.Error("Error is not a gRPC status error")
					return
				}

				if st.Code() != tt.wantCode {
					t.Errorf("GetTaskResult() error code = %v, want %v", st.Code(), tt.wantCode)
				}
			} else {
				if err != nil {
					t.Errorf("GetTaskResult() unexpected error = %v", err)
					return
				}

				if resp == nil {
					t.Fatal("GetTaskResult() returned nil response")
				}

				if resp.Task == nil {
					t.Fatal("GetTaskResult() returned nil task")
				}

				if resp.Task.Id != tt.taskID {
					t.Errorf("GetTaskResult() task ID = %v, want %v", resp.Task.Id, tt.taskID)
				}

				if resp.Task.Status != proto.TaskStatus_COMPLETED {
					t.Errorf("GetTaskResult() task status = %v, want COMPLETED", resp.Task.Status)
				}
			}
		})
	}
}

func TestServer_ConcurrentSubmitTask(t *testing.T) {
	s := NewServer()
	defer s.cancel()
	ctx := context.Background()

	// Test concurrent task submissions
	const numGoroutines = 100
	results := make(chan *proto.SubmitTaskResponse, numGoroutines)
	errors := make(chan error, numGoroutines)

	for i := range numGoroutines {
		go func(idx int) {
			req := &proto.SubmitTaskRequest{
				Type:       "concurrent-task",
				Payload:    []byte("test"),
				Priority:   int32(proto.Priority_MEDIUM),
				MaxRetries: 3,
			}
			resp, err := s.SubmitTask(ctx, req)
			if err != nil {
				errors <- err
			} else {
				results <- resp
			}
		}(i)
	}

	// Collect results
	taskIDs := make(map[string]bool)
	for range numGoroutines {
		select {
		case resp := <-results:
			if taskIDs[resp.TaskId] {
				t.Errorf("Duplicate task ID generated: %s", resp.TaskId)
			}
			taskIDs[resp.TaskId] = true
		case err := <-errors:
			t.Errorf("Concurrent SubmitTask failed: %v", err)
		}
	}

	// Verify all tasks were stored
	s.tasksMux.RLock()
	storedCount := len(s.tasks)
	s.tasksMux.RUnlock()

	if storedCount != numGoroutines {
		t.Errorf("Expected %d tasks stored, got %d", numGoroutines, storedCount)
	}
}

func TestServer_ConcurrentGetTaskStatus(t *testing.T) {
	s := NewServer()
	defer s.cancel()
	ctx := context.Background()

	// Submit a task
	submitReq := &proto.SubmitTaskRequest{
		Type:       "test-task",
		Payload:    []byte("test"),
		Priority:   int32(proto.Priority_HIGH),
		MaxRetries: 3,
	}
	submitResp, err := s.SubmitTask(ctx, submitReq)
	if err != nil {
		t.Fatalf("Failed to submit test task: %v", err)
	}

	// Concurrently read task status
	const numGoroutines = 100
	errors := make(chan error, numGoroutines)

	for range numGoroutines {
		go func() {
			req := &proto.GetTaskStatusRequest{
				TaskId: submitResp.TaskId,
			}
			_, err := s.GetTaskStatus(ctx, req)
			if err != nil {
				errors <- err
			} else {
				errors <- nil
			}
		}()
	}

	// Check no errors occurred
	for range numGoroutines {
		if err := <-errors; err != nil {
			t.Errorf("Concurrent GetTaskStatus failed: %v", err)
		}
	}
}
