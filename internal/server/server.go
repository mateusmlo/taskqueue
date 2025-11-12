package server

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/mateusmlo/taskqueue/internal/worker"
	"github.com/mateusmlo/taskqueue/proto"
)

type Priority int
type TaskStatus int

const (
	HIGH Priority = iota
	MEDIUM
	LOW
)

const (
	PENDING TaskStatus = iota
	RUNNING
	COMPLETED
	FAILED
)

// Server struct implements the TaskQueue and WorkerService gRPC servers
type Server struct {
	tasks    map[string]*Task
	tasksMux sync.RWMutex

	pendingQueues map[Priority][]*Task
	queuesMux     sync.RWMutex

	workers    map[string]*worker.Worker
	workersMux sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc

	proto.UnimplementedTaskQueueServer
	proto.UnimplementedWorkerServiceServer
}

// Task represents a unit of work in the task queue system
type Task struct {
	ID          string
	Type        string
	Payload     []byte
	Priority    Priority
	Status      TaskStatus
	RetryCount  int
	MaxRetries  int
	CreatedAt   time.Time
	StartedAt   *time.Time
	CompletedAt *time.Time
	Result      []byte
	Error       string
	WorkerID    string
}

// NewServer initializes and returns a new Server instance
func NewServer() *Server {
	ctx, cancel := context.WithCancel(context.Background())

	return &Server{
		tasks:         make(map[string]*Task),
		pendingQueues: make(map[Priority][]*Task),
		workers:       make(map[string]*worker.Worker),
		ctx:           ctx,
		cancel:        cancel,
	}
}

// toProtoTask converts internal Task to proto.Task
func (t *Task) toProtoTask() *proto.Task {
	protoTask := &proto.Task{
		Id:         t.ID,
		Type:       t.Type,
		Payload:    t.Payload,
		Priority:   proto.Priority(t.Priority),
		MaxRetries: int32(t.MaxRetries),
		RetryCount: int32(t.RetryCount),
		CreatedAt:  timestamppb.New(t.CreatedAt),
		Status:     proto.TaskStatus(t.Status),
	}

	// Handle optional timestamp fields
	if t.StartedAt != nil {
		protoTask.StartedAt = timestamppb.New(*t.StartedAt)
	}
	if t.CompletedAt != nil {
		protoTask.CompletedAt = timestamppb.New(*t.CompletedAt)
	}

	return protoTask
}

// SubmitTask handles task submission requests
func (s *Server) SubmitTask(ctx context.Context, req *proto.SubmitTaskRequest) (*proto.SubmitTaskResponse, error) {
	uuid, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	taskID := uuid.String()

	newTask := &Task{
		ID:         taskID,
		Type:       req.Type,
		Payload:    req.Payload,
		Priority:   Priority(req.Priority),
		Status:     PENDING,
		RetryCount: 0,
		MaxRetries: int(req.MaxRetries),
		CreatedAt:  time.Now(),
	}

	s.tasksMux.Lock()
	defer s.tasksMux.Unlock()

	s.tasks[taskID] = newTask

	s.appendTaskToQueue(newTask)

	return &proto.SubmitTaskResponse{TaskId: newTask.ID}, nil
}

// GetTaskStatus retrieves the status of a task by its ID
func (s *Server) GetTaskStatus(ctx context.Context, req *proto.GetTaskStatusRequest) (*proto.GetTaskStatusResponse, error) {
	task, err := s.findTask(req.TaskId)
	if err != nil {
		return nil, err
	}

	return &proto.GetTaskStatusResponse{Status: proto.TaskStatus(task.Status)}, nil
}

// GetTaskResult retrieves the result of a completed task by its ID
func (s *Server) GetTaskResult(ctx context.Context, req *proto.GetTaskResultRequest) (*proto.GetTaskResultResponse, error) {
	task, err := s.findTask(req.TaskId)
	if err != nil {
		return nil, err
	}

	if task.Status != COMPLETED {
		return nil, status.Errorf(codes.FailedPrecondition, "task %s not completed yet", req.TaskId)
	}

	return &proto.GetTaskResultResponse{Task: task.toProtoTask()}, nil
}

// RegisterWorker handles worker registration requests
func (s *Server) RegisterWorker(ctx context.Context, req *proto.RegisterWorkerRequest) (*proto.RegisterWorkerResponse, error) {
	var newWorker worker.Worker
	newWorker.FromProtoWorker(req.Worker)

	s.workersMux.Lock()
	defer s.workersMux.Unlock()

	s.workers[newWorker.ID] = &newWorker

	return &proto.RegisterWorkerResponse{WorkerId: newWorker.ID, Success: true}, nil
}

// Heartbeat processes heartbeat messages from workers
func (s *Server) Heartbeat(ctx context.Context, req *proto.HeartbeatRequest) (*proto.HeartbeatResponse, error) {
	worker, err := s.findWorker(req.WorkerId)
	if err != nil {
		return nil, err
	}

	worker.LastHeartbeat = time.Now()
	worker.CurrentLoad = int(req.CurrentLoad)

	return &proto.HeartbeatResponse{Success: true, CurrentLoad: int32(worker.CurrentLoad)}, nil
}

// SubmitResult processes the result submission from workers
func (s *Server) SubmitResult(ctx context.Context, req *proto.SubmitResultRequest) (*proto.SubmitResultResponse, error) {
	task, err := s.findTask(req.TaskId)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	task.CompletedAt = &now

	defer s.decrementCurrentLoad(task.WorkerID)

	if req.Error != "" {
		task.Error = req.Error
		task.RetryCount++

		if task.RetryCount < task.MaxRetries {
			task.Status = PENDING
			task.StartedAt = nil
			task.CompletedAt = nil

			s.appendTaskToQueue(task)
		} else {
			task.Status = FAILED

			return nil, status.Errorf(codes.DeadlineExceeded, "task %s failed after maximum retries: %s", req.TaskId, req.Error)
		}
	} else {
		task.Status = COMPLETED
		task.Result = req.Result
	}

	return &proto.SubmitResultResponse{Success: true, Result: req.Result}, nil
}

func (s *Server) FetchTask(ctx context.Context, req *proto.FetchTaskRequest) (*proto.FetchTaskResponse, error) {
	worker, err := s.findWorker(req.WorkerId)
	if err != nil {
		return nil, err
	}

	if worker.CurrentLoad >= worker.Capacity {
		return &proto.FetchTaskResponse{HasTask: false}, nil
	}

	s.queuesMux.Lock()
	defer s.queuesMux.Unlock()

	for priority := HIGH; priority <= LOW; priority++ {
		queue := s.pendingQueues[priority]
		for i, task := range queue {
			if slices.Contains(req.TaskTypes, task.Type) {
				// Remove task from queue
				s.pendingQueues[priority] = append(queue[:i], queue[i+1:]...)

				// Update task status
				now := time.Now()

				s.tasksMux.Lock()
				task.Status = RUNNING
				task.StartedAt = &now
				task.WorkerID = worker.ID
				s.tasksMux.Unlock()

				s.incrementCurrentLoad(worker.ID)

				return &proto.FetchTaskResponse{Task: task.toProtoTask(), HasTask: true}, nil
			}
		}
	}

	return &proto.FetchTaskResponse{HasTask: false}, nil
}

// Util functions

// appendTaskToQueue appends a task back to the pending queue based on its priority
func (s *Server) appendTaskToQueue(task *Task) {
	s.queuesMux.Lock()
	defer s.queuesMux.Unlock()

	s.pendingQueues[task.Priority] = append(s.pendingQueues[task.Priority], task)
}

// decrementCurrentLoad decreases the current load of the specified worker
func (s *Server) decrementCurrentLoad(workerID string) {
	s.workersMux.Lock()
	defer s.workersMux.Unlock()

	if worker, exists := s.workers[workerID]; exists {
		worker.CurrentLoad--
	}
}

// incrementCurrentLoad increases the current load of the specified worker
func (s *Server) incrementCurrentLoad(workerID string) {
	s.workersMux.Lock()
	defer s.workersMux.Unlock()

	if worker, exists := s.workers[workerID]; exists {
		worker.CurrentLoad++
	}
}

// findTask retrieves a task by its ID, returning an error if not found
func (s *Server) findTask(taskID string) (*Task, error) {
	s.tasksMux.RLock()
	defer s.tasksMux.RUnlock()

	task, exists := s.tasks[taskID]
	if !exists {
		return nil, status.Errorf(codes.NotFound, "task %s not found", taskID)
	}

	return task, nil
}

// findWorker retrieves a worker by its ID, returning an error if not found
func (s *Server) findWorker(workerID string) (*worker.Worker, error) {
	s.workersMux.RLock()
	defer s.workersMux.RUnlock()

	worker, exists := s.workers[workerID]
	if !exists {
		return nil, status.Errorf(codes.NotFound, "worker %s not registered", workerID)
	}

	return worker, nil
}
