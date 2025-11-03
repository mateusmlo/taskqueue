package internal

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

type Server struct {
	tasks    map[string]*Task
	tasksMux sync.RWMutex

	pendingQueues map[Priority][]*Task
	queuesMux     sync.RWMutex

	workers    map[string]*Worker
	workersMux sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc

	proto.UnimplementedTaskQueueServer
	proto.UnimplementedWorkerServiceServer
}

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

type Worker struct {
	ID            string
	Address       string
	RegisteredAt  time.Time
	LastHeartbeat time.Time
	TaskTypes     []string
	Capacity      int
	CurrentLoad   int
}

func NewServer() *Server {
	ctx, cancel := context.WithCancel(context.Background())

	return &Server{
		tasks:         make(map[string]*Task),
		pendingQueues: make(map[Priority][]*Task),
		workers:       make(map[string]*Worker),
		ctx:           ctx,
		cancel:        cancel,
	}
}

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

	s.queuesMux.Lock()
	defer s.queuesMux.Unlock()

	s.pendingQueues[newTask.Priority] = append(s.pendingQueues[newTask.Priority], newTask)

	return &proto.SubmitTaskResponse{TaskId: newTask.ID}, nil
}

func (s *Server) GetTaskStatus(ctx context.Context, req *proto.GetTaskStatusRequest) (*proto.GetTaskStatusResponse, error) {
	s.tasksMux.RLock()
	defer s.tasksMux.RUnlock()

	task, exists := s.tasks[req.TaskId]
	if !exists {
		return nil, status.Errorf(codes.NotFound, "task %s not found", req.TaskId)
	}

	return &proto.GetTaskStatusResponse{Status: proto.TaskStatus(task.Status)}, nil
}
