package server

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
	completedTask.Result = []byte("task completed successfully")
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

				if resp.Result == nil {
					t.Fatal("GetTaskResult() returned nil task")
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

func TestServer_RegisterWorker(t *testing.T) {
	s := NewServer()
	defer s.cancel()
	ctx := context.Background()

	tests := []struct {
		name    string
		worker  *proto.Worker
		wantErr bool
	}{
		{
			name: "valid worker registration",
			worker: &proto.Worker{
				TaskTypes: []string{"image-processing", "data-export"},
				Capacity:  10,
				Metadata: map[string]string{
					"address": "localhost:8080",
					"region":  "us-west-1",
				},
			},
			wantErr: false,
		},
		{
			name: "worker with single task type",
			worker: &proto.Worker{
				TaskTypes: []string{"email-send"},
				Capacity:  5,
				Metadata: map[string]string{
					"address": "localhost:8081",
				},
			},
			wantErr: false,
		},
		{
			name: "worker with high capacity",
			worker: &proto.Worker{
				TaskTypes: []string{"batch-job", "report-gen"},
				Capacity:  100,
				Metadata: map[string]string{
					"address": "localhost:8082",
					"type":    "high-capacity",
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &proto.RegisterWorkerRequest{
				Worker: tt.worker,
			}

			resp, err := s.RegisterWorker(ctx, req)

			if (err != nil) != tt.wantErr {
				t.Errorf("RegisterWorker() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if resp == nil {
					t.Fatal("RegisterWorker() returned nil response")
				}

				if !resp.Success {
					t.Error("RegisterWorker() success = false, want true")
				}

				if resp.WorkerId == "" {
					t.Error("RegisterWorker() returned empty worker ID")
				}

				// Verify worker was stored
				s.workersMux.RLock()
				worker, exists := s.workers[resp.WorkerId]
				s.workersMux.RUnlock()

				if !exists {
					t.Error("Worker was not stored in server.workers")
				}

				if worker.ID != resp.WorkerId {
					t.Errorf("Worker.ID = %v, want %v", worker.ID, resp.WorkerId)
				}

				if worker.Capacity != int(tt.worker.Capacity) {
					t.Errorf("Worker.Capacity = %v, want %v", worker.Capacity, tt.worker.Capacity)
				}

				if worker.CurrentLoad != 0 {
					t.Errorf("Worker.CurrentLoad = %v, want 0", worker.CurrentLoad)
				}

				if len(worker.TaskTypes) != len(tt.worker.TaskTypes) {
					t.Errorf("Worker.TaskTypes length = %v, want %v", len(worker.TaskTypes), len(tt.worker.TaskTypes))
				}
			}
		})
	}
}

func TestServer_Heartbeat(t *testing.T) {
	s := NewServer()
	defer s.cancel()
	ctx := context.Background()

	// Register a worker first
	registerReq := &proto.RegisterWorkerRequest{
		Worker: &proto.Worker{
			TaskTypes: []string{"test-task"},
			Capacity:  10,
			Metadata: map[string]string{
				"address": "localhost:8080",
			},
		},
	}
	registerResp, err := s.RegisterWorker(ctx, registerReq)
	if err != nil {
		t.Fatalf("Failed to register worker: %v", err)
	}

	tests := []struct {
		name        string
		workerID    string
		currentLoad int32
		wantErr     bool
		wantCode    codes.Code
	}{
		{
			name:        "valid heartbeat",
			workerID:    registerResp.WorkerId,
			currentLoad: 5,
			wantErr:     false,
		},
		{
			name:        "heartbeat with zero load",
			workerID:    registerResp.WorkerId,
			currentLoad: 0,
			wantErr:     false,
		},
		{
			name:        "heartbeat with max load",
			workerID:    registerResp.WorkerId,
			currentLoad: 10,
			wantErr:     false,
		},
		{
			name:        "heartbeat from non-existent worker",
			workerID:    "non-existent-worker",
			currentLoad: 3,
			wantErr:     true,
			wantCode:    codes.NotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Record time before heartbeat
			timeBefore := time.Now()

			req := &proto.HeartbeatRequest{
				WorkerId:    tt.workerID,
				CurrentLoad: tt.currentLoad,
			}

			resp, err := s.Heartbeat(ctx, req)

			if tt.wantErr {
				if err == nil {
					t.Error("Heartbeat() expected error, got nil")
					return
				}

				st, ok := status.FromError(err)
				if !ok {
					t.Error("Error is not a gRPC status error")
					return
				}

				if st.Code() != tt.wantCode {
					t.Errorf("Heartbeat() error code = %v, want %v", st.Code(), tt.wantCode)
				}
			} else {
				if err != nil {
					t.Errorf("Heartbeat() unexpected error = %v", err)
					return
				}

				if resp == nil {
					t.Fatal("Heartbeat() returned nil response")
				}

				if !resp.Success {
					t.Error("Heartbeat() success = false, want true")
				}

				if resp.CurrentLoad != tt.currentLoad {
					t.Errorf("Heartbeat() current_load = %v, want %v", resp.CurrentLoad, tt.currentLoad)
				}

				// Verify worker's last heartbeat was updated
				s.workersMux.RLock()
				worker := s.workers[tt.workerID]
				s.workersMux.RUnlock()

				if worker.LastHeartbeat.Before(timeBefore) {
					t.Error("Worker's LastHeartbeat was not updated")
				}

				if worker.CurrentLoad != int(tt.currentLoad) {
					t.Errorf("Worker.CurrentLoad = %v, want %v", worker.CurrentLoad, tt.currentLoad)
				}
			}
		})
	}
}

func TestServer_FetchTask(t *testing.T) {
	s := NewServer()
	defer s.cancel()
	ctx := context.Background()

	// Register a worker
	registerReq := &proto.RegisterWorkerRequest{
		Worker: &proto.Worker{
			TaskTypes: []string{"image-processing", "data-export"},
			Capacity:  10,
			Metadata: map[string]string{
				"address": "localhost:8080",
			},
		},
	}
	registerResp, err := s.RegisterWorker(ctx, registerReq)
	if err != nil {
		t.Fatalf("Failed to register worker: %v", err)
	}

	// Submit tasks with different priorities
	highPriorityTask, _ := s.SubmitTask(ctx, &proto.SubmitTaskRequest{
		Type:       "image-processing",
		Payload:    []byte("high priority"),
		Priority:   int32(proto.Priority_HIGH),
		MaxRetries: 3,
	})

	mediumPriorityTask, _ := s.SubmitTask(ctx, &proto.SubmitTaskRequest{
		Type:       "data-export",
		Payload:    []byte("medium priority"),
		Priority:   int32(proto.Priority_MEDIUM),
		MaxRetries: 3,
	})

	lowPriorityTask, _ := s.SubmitTask(ctx, &proto.SubmitTaskRequest{
		Type:       "image-processing",
		Payload:    []byte("low priority"),
		Priority:   int32(proto.Priority_LOW),
		MaxRetries: 3,
	})

	tests := []struct {
		name        string
		workerID    string
		taskTypes   []string
		wantHasTask bool
		wantTaskID  string
		wantErr     bool
		wantCode    codes.Code
		setupFunc   func()
	}{
		{
			name:        "fetch high priority task",
			workerID:    registerResp.WorkerId,
			taskTypes:   []string{"image-processing", "data-export"},
			wantHasTask: true,
			wantTaskID:  highPriorityTask.TaskId,
			wantErr:     false,
		},
		{
			name:        "fetch medium priority task after high priority consumed",
			workerID:    registerResp.WorkerId,
			taskTypes:   []string{"data-export"},
			wantHasTask: true,
			wantTaskID:  mediumPriorityTask.TaskId,
			wantErr:     false,
		},
		{
			name:        "fetch low priority task",
			workerID:    registerResp.WorkerId,
			taskTypes:   []string{"image-processing"},
			wantHasTask: true,
			wantTaskID:  lowPriorityTask.TaskId,
			wantErr:     false,
		},
		{
			name:        "no tasks available for worker task types",
			workerID:    registerResp.WorkerId,
			taskTypes:   []string{"email-send"},
			wantHasTask: false,
			wantErr:     false,
		},
		{
			name:        "worker not found",
			workerID:    "non-existent-worker",
			taskTypes:   []string{"image-processing"},
			wantHasTask: false,
			wantErr:     true,
			wantCode:    codes.NotFound,
		},
		{
			name:        "worker at capacity cannot fetch tasks",
			workerID:    registerResp.WorkerId,
			taskTypes:   []string{"image-processing"},
			wantHasTask: false,
			wantErr:     false,
			setupFunc: func() {
				// Set worker to full capacity
				s.workersMux.Lock()
				s.workers[registerResp.WorkerId].CurrentLoad = 10
				s.workersMux.Unlock()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setupFunc != nil {
				tt.setupFunc()
			}

			req := &proto.FetchTaskRequest{
				WorkerId:  tt.workerID,
				TaskTypes: tt.taskTypes,
			}

			resp, err := s.FetchTask(ctx, req)

			if tt.wantErr {
				if err == nil {
					t.Error("FetchTask() expected error, got nil")
					return
				}

				st, ok := status.FromError(err)
				if !ok {
					t.Error("Error is not a gRPC status error")
					return
				}

				if st.Code() != tt.wantCode {
					t.Errorf("FetchTask() error code = %v, want %v", st.Code(), tt.wantCode)
				}
			} else {
				if err != nil {
					t.Errorf("FetchTask() unexpected error = %v", err)
					return
				}

				if resp == nil {
					t.Fatal("FetchTask() returned nil response")
				}

				if resp.HasTask != tt.wantHasTask {
					t.Errorf("FetchTask() has_task = %v, want %v", resp.HasTask, tt.wantHasTask)
				}

				if tt.wantHasTask {
					if resp.Task == nil {
						t.Fatal("FetchTask() returned nil task when has_task is true")
					}

					if resp.Task.Id != tt.wantTaskID {
						t.Errorf("FetchTask() task ID = %v, want %v", resp.Task.Id, tt.wantTaskID)
					}

					// Verify task status was updated to RUNNING
					s.tasksMux.RLock()
					task := s.tasks[resp.Task.Id]
					s.tasksMux.RUnlock()

					if task.Status != RUNNING {
						t.Errorf("Task status = %v, want RUNNING", task.Status)
					}

					if task.StartedAt == nil {
						t.Error("Task.StartedAt is nil")
					}

					if task.WorkerID != tt.workerID {
						t.Errorf("Task.WorkerID = %v, want %v", task.WorkerID, tt.workerID)
					}
				}
			}
		})
	}
}

func TestServer_SubmitResult(t *testing.T) {
	s := NewServer()
	defer s.cancel()
	ctx := context.Background()

	// Register a worker
	registerReq := &proto.RegisterWorkerRequest{
		Worker: &proto.Worker{
			TaskTypes: []string{"test-task"},
			Capacity:  10,
			Metadata: map[string]string{
				"address": "localhost:8080",
			},
		},
	}
	registerResp, err := s.RegisterWorker(ctx, registerReq)
	if err != nil {
		t.Fatalf("Failed to register worker: %v", err)
	}

	// Submit and fetch a task for success test
	submitReq := &proto.SubmitTaskRequest{
		Type:       "test-task",
		Payload:    []byte("test"),
		Priority:   int32(proto.Priority_HIGH),
		MaxRetries: 3,
	}
	submitResp, _ := s.SubmitTask(ctx, submitReq)

	fetchReq := &proto.FetchTaskRequest{
		WorkerId:  registerResp.WorkerId,
		TaskTypes: []string{"test-task"},
	}
	s.FetchTask(ctx, fetchReq)

	// Submit another task for retry test
	retryTaskResp, _ := s.SubmitTask(ctx, &proto.SubmitTaskRequest{
		Type:       "test-task",
		Payload:    []byte("retry test"),
		Priority:   int32(proto.Priority_HIGH),
		MaxRetries: 3,
	})
	s.FetchTask(ctx, fetchReq)

	// Submit task for max retries test
	maxRetriesTaskResp, _ := s.SubmitTask(ctx, &proto.SubmitTaskRequest{
		Type:       "test-task",
		Payload:    []byte("max retries test"),
		Priority:   int32(proto.Priority_HIGH),
		MaxRetries: 2,
	})
	s.FetchTask(ctx, fetchReq)

	// Manually set retry count to simulate previous failures
	s.tasksMux.Lock()
	s.tasks[maxRetriesTaskResp.TaskId].RetryCount = 1
	s.tasksMux.Unlock()

	tests := []struct {
		name     string
		taskID   string
		result   []byte
		error    string
		wantErr  bool
		wantCode codes.Code
		verify   func(t *testing.T, taskID string)
	}{
		{
			name:    "successful task completion",
			taskID:  submitResp.TaskId,
			result:  []byte("success result"),
			error:   "",
			wantErr: false,
			verify: func(t *testing.T, taskID string) {
				s.tasksMux.RLock()
				task := s.tasks[taskID]
				s.tasksMux.RUnlock()

				if task.Status != COMPLETED {
					t.Errorf("Task status = %v, want COMPLETED", task.Status)
				}

				if string(task.Result) != "success result" {
					t.Errorf("Task result = %v, want 'success result'", string(task.Result))
				}

				if task.CompletedAt == nil {
					t.Error("Task.CompletedAt is nil")
				}

				if task.Error != "" {
					t.Errorf("Task.Error = %v, want empty string", task.Error)
				}
			},
		},
		{
			name:    "task failure with retry available",
			taskID:  retryTaskResp.TaskId,
			result:  nil,
			error:   "processing failed",
			wantErr: false,
			verify: func(t *testing.T, taskID string) {
				s.tasksMux.RLock()
				task := s.tasks[taskID]
				s.tasksMux.RUnlock()

				if task.Status != PENDING {
					t.Errorf("Task status = %v, want PENDING (for retry)", task.Status)
				}

				if task.RetryCount != 1 {
					t.Errorf("Task retry count = %v, want 1", task.RetryCount)
				}

				if task.Error != "processing failed" {
					t.Errorf("Task error = %v, want 'processing failed'", task.Error)
				}

				if task.StartedAt != nil {
					t.Error("Task.StartedAt should be nil after retry reset")
				}

				if task.CompletedAt != nil {
					t.Error("Task.CompletedAt should be nil after retry reset")
				}

				// Verify task was re-added to queue
				s.queuesMux.RLock()
				queue := s.pendingQueues[task.Priority]
				s.queuesMux.RUnlock()

				found := false
				for _, qTask := range queue {
					if qTask.ID == taskID {
						found = true
						break
					}
				}

				if !found {
					t.Error("Task was not re-added to pending queue for retry")
				}
			},
		},
		{
			name:     "task failure exceeding max retries",
			taskID:   maxRetriesTaskResp.TaskId,
			result:   nil,
			error:    "fatal error",
			wantErr:  true,
			wantCode: codes.DeadlineExceeded,
			verify: func(t *testing.T, taskID string) {
				s.tasksMux.RLock()
				task := s.tasks[taskID]
				s.tasksMux.RUnlock()

				if task.Status != FAILED {
					t.Errorf("Task status = %v, want FAILED", task.Status)
				}

				if task.RetryCount != 2 {
					t.Errorf("Task retry count = %v, want 2", task.RetryCount)
				}
			},
		},
		{
			name:     "submit result for non-existent task",
			taskID:   "non-existent-task",
			result:   []byte("result"),
			error:    "",
			wantErr:  true,
			wantCode: codes.NotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &proto.SubmitResultRequest{
				TaskId: tt.taskID,
				Result: tt.result,
				Error:  tt.error,
			}

			resp, err := s.SubmitResult(ctx, req)

			if tt.wantErr {
				if err == nil {
					t.Error("SubmitResult() expected error, got nil")
					return
				}

				st, ok := status.FromError(err)
				if !ok {
					t.Error("Error is not a gRPC status error")
					return
				}

				if st.Code() != tt.wantCode {
					t.Errorf("SubmitResult() error code = %v, want %v", st.Code(), tt.wantCode)
				}
			} else {
				if err != nil {
					t.Errorf("SubmitResult() unexpected error = %v", err)
					return
				}

				if resp == nil {
					t.Fatal("SubmitResult() returned nil response")
				}

				if !resp.Success {
					t.Error("SubmitResult() success = false, want true")
				}
			}

			if tt.verify != nil {
				tt.verify(t, tt.taskID)
			}
		})
	}
}

func TestServer_UtilityFunctions(t *testing.T) {
	t.Run("appendTaskToQueue", func(t *testing.T) {
		s := NewServer()
		defer s.cancel()

		highTask := &Task{ID: "high-1", Priority: HIGH, Status: PENDING}
		mediumTask := &Task{ID: "medium-1", Priority: MEDIUM, Status: PENDING}
		lowTask := &Task{ID: "low-1", Priority: LOW, Status: PENDING}

		s.appendTaskToQueue(highTask)
		s.appendTaskToQueue(mediumTask)
		s.appendTaskToQueue(lowTask)

		s.queuesMux.RLock()
		defer s.queuesMux.RUnlock()

		if len(s.pendingQueues[HIGH]) != 1 {
			t.Errorf("HIGH queue length = %v, want 1", len(s.pendingQueues[HIGH]))
		}

		if len(s.pendingQueues[MEDIUM]) != 1 {
			t.Errorf("MEDIUM queue length = %v, want 1", len(s.pendingQueues[MEDIUM]))
		}

		if len(s.pendingQueues[LOW]) != 1 {
			t.Errorf("LOW queue length = %v, want 1", len(s.pendingQueues[LOW]))
		}

		if s.pendingQueues[HIGH][0].ID != "high-1" {
			t.Errorf("HIGH queue task ID = %v, want 'high-1'", s.pendingQueues[HIGH][0].ID)
		}
	})

	t.Run("incrementCurrentLoad", func(t *testing.T) {
		s := NewServer()
		defer s.cancel()

		// Create a worker
		workerID := "test-worker-1"
		s.workers[workerID] = &WorkerInfo{
			ID:          workerID,
			Capacity:    10,
			CurrentLoad: 5,
		}

		s.incrementCurrentLoad(workerID)

		s.workersMux.RLock()
		load := s.workers[workerID].CurrentLoad
		s.workersMux.RUnlock()

		if load != 6 {
			t.Errorf("Worker current load = %v, want 6", load)
		}

		// Test with non-existent worker (should not panic)
		s.incrementCurrentLoad("non-existent")
	})

	t.Run("decrementCurrentLoad", func(t *testing.T) {
		s := NewServer()
		defer s.cancel()

		// Create a worker
		workerID := "test-worker-2"
		s.workers[workerID] = &WorkerInfo{
			ID:          workerID,
			Capacity:    10,
			CurrentLoad: 5,
		}

		s.decrementCurrentLoad(workerID)

		s.workersMux.RLock()
		load := s.workers[workerID].CurrentLoad
		s.workersMux.RUnlock()

		if load != 4 {
			t.Errorf("Worker current load = %v, want 4", load)
		}

		// Test with non-existent worker (should not panic)
		s.decrementCurrentLoad("non-existent")
	})

	t.Run("findTask", func(t *testing.T) {
		s := NewServer()
		defer s.cancel()

		testTask := &Task{ID: "test-task-1", Type: "test"}
		s.tasks["test-task-1"] = testTask

		// Test finding existing task
		found, err := s.findTask("test-task-1")
		if err != nil {
			t.Errorf("findTask() unexpected error = %v", err)
		}
		if found.ID != "test-task-1" {
			t.Errorf("findTask() task ID = %v, want 'test-task-1'", found.ID)
		}

		// Test finding non-existent task
		_, err = s.findTask("non-existent")
		if err == nil {
			t.Error("findTask() expected error for non-existent task, got nil")
		}

		st, ok := status.FromError(err)
		if !ok {
			t.Error("Error is not a gRPC status error")
		}
		if st.Code() != codes.NotFound {
			t.Errorf("findTask() error code = %v, want NotFound", st.Code())
		}
	})

	t.Run("findWorker", func(t *testing.T) {
		s := NewServer()
		defer s.cancel()

		testWorker := &WorkerInfo{ID: "test-worker-1"}
		s.workers["test-worker-1"] = testWorker

		// Test finding existing worker
		found, err := s.findWorker("test-worker-1")
		if err != nil {
			t.Errorf("findWorker() unexpected error = %v", err)
		}
		if found.ID != "test-worker-1" {
			t.Errorf("findWorker() worker ID = %v, want 'test-worker-1'", found.ID)
		}

		// Test finding non-existent worker
		_, err = s.findWorker("non-existent")
		if err == nil {
			t.Error("findWorker() expected error for non-existent worker, got nil")
		}

		st, ok := status.FromError(err)
		if !ok {
			t.Error("Error is not a gRPC status error")
		}
		if st.Code() != codes.NotFound {
			t.Errorf("findWorker() error code = %v, want NotFound", st.Code())
		}
	})
}

func TestServer_ConcurrentWorkerOperations(t *testing.T) {
	s := NewServer()
	defer s.cancel()
	ctx := context.Background()

	// Test concurrent worker registrations
	const numWorkers = 50
	results := make(chan *proto.RegisterWorkerResponse, numWorkers)
	errors := make(chan error, numWorkers)

	for i := range numWorkers {
		go func(idx int) {
			req := &proto.RegisterWorkerRequest{
				Worker: &proto.Worker{
					TaskTypes: []string{"test-task"},
					Capacity:  10,
					Metadata: map[string]string{
						"address": "localhost:8080",
					},
				},
			}
			resp, err := s.RegisterWorker(ctx, req)
			if err != nil {
				errors <- err
			} else {
				results <- resp
			}
		}(i)
	}

	// Collect results
	workerIDs := make(map[string]bool)
	for range numWorkers {
		select {
		case resp := <-results:
			if workerIDs[resp.WorkerId] {
				t.Errorf("Duplicate worker ID generated: %s", resp.WorkerId)
			}
			workerIDs[resp.WorkerId] = true
		case err := <-errors:
			t.Errorf("Concurrent RegisterWorker failed: %v", err)
		}
	}

	// Verify all workers were stored
	s.workersMux.RLock()
	storedCount := len(s.workers)
	s.workersMux.RUnlock()

	if storedCount != numWorkers {
		t.Errorf("Expected %d workers stored, got %d", numWorkers, storedCount)
	}
}
