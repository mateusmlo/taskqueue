package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/mateusmlo/taskqueue/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Worker struct {
	serverAddr string
	conn       *grpc.ClientConn
	client     proto.WorkerServiceClient

	id       string
	capacity int

	handlers    map[string]TaskHandler
	currentLoad int
	loadMux     sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type TaskHandler interface {
	Handle(ctx context.Context, payload []byte) ([]byte, error)
}

// NewWorker creates a new Worker instance.
func NewWorker(serverAddr string, capacity int) *Worker {
	ctx, cancel := context.WithCancel(context.Background())

	return &Worker{
		serverAddr: serverAddr,
		capacity:   capacity,
		handlers:   make(map[string]TaskHandler),
		ctx:        ctx,
		cancel:     cancel,
	}
}

// RegisterHandler registers a task handler for a specific task type.
func (w *Worker) RegisterHandler(taskType string, handler TaskHandler) {
	w.handlers[taskType] = handler
}

// Start connects the worker to the server and begins processing tasks.
func (w *Worker) Start() error {
	tcr, err := credentials.NewClientTLSFromFile("./cert/server.crt", "localhost")
	if err != nil {
		return err
	}

	conn, err := grpc.NewClient(w.serverAddr, grpc.WithTransportCredentials(tcr))
	if err != nil {
		return err
	}

	w.conn = conn
	w.client = proto.NewWorkerServiceClient(w.conn)

	if err := w.register(); err != nil {
		w.conn.Close()
		return err
	}

	w.wg.Add(2)
	go w.heartbeatLoop()
	go w.fetchLoop()

	return nil
}

// Stop stops the worker and cleans up resources.
func (w *Worker) Stop() {
	w.cancel()
	w.wg.Wait()

	if w.conn != nil {
		if err := w.conn.Close(); err != nil {
			log.Printf("Error closing gRPC connection: %v", err)
		}

		w.conn = nil
	}
}

// heartbeatLoop sends periodic heartbeat messages to the server.
func (w *Worker) heartbeatLoop() {
	defer w.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			req := w.buildHeartbeatRequest()
			_, err := w.client.Heartbeat(w.ctx, req)
			if err != nil {
				log.Printf("Worker heartbeat error: %v", err)
			}
		case <-w.ctx.Done():
			return
		}
	}
}

// fetchLoop continuously fetches tasks from the server and processes them.
func (w *Worker) fetchLoop() {
	defer w.wg.Done()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			req := w.buildFetchTasksRequest()
			res, err := w.client.FetchTask(w.ctx, req)
			if err != nil {
				log.Printf("Worker fetch task error: %v", err)
				continue
			}

			if !res.HasTask {
				continue
			}

			handler, exists := w.handlers[res.Task.Type]
			if !exists {
				log.Printf("No handler registered for task type: %s", res.Task.Type)
				continue
			}

			w.incrementLoad()

			handleTask := w.getTaskHandler(handler)

			go handleTask(res.Task)
		case <-w.ctx.Done():
			return
		}
	}
}

// getTaskHandler returns a function that processes a task using the provided handler.
func (w *Worker) getTaskHandler(handler TaskHandler) func(task *proto.Task) {
	return func(task *proto.Task) {
		defer w.decrementLoad()

		result, err := handler.Handle(w.ctx, task.Payload)
		submitReq := &proto.SubmitResultRequest{
			TaskId: task.Id,
		}
		if err != nil {
			submitReq.Error = err.Error()
			submitReq.Result = nil
		} else {
			submitReq.Error = ""
			submitReq.Result = result
		}

		_, err = w.client.SubmitResult(w.ctx, submitReq)
		if err != nil {
			log.Printf("Error submitting task result: %v", err)
		}
	}
}

// getCurrentLoad safely retrieves the current load of the worker.
func (w *Worker) getCurrentLoad() int32 {
	w.loadMux.RLock()
	defer w.loadMux.RUnlock()

	return int32(w.currentLoad)
}

// incrementLoad safely increments the current load of the worker.
func (w *Worker) incrementLoad() {
	w.loadMux.Lock()
	defer w.loadMux.Unlock()

	w.currentLoad++
}

// decrementLoad safely decrements the current load of the worker.
func (w *Worker) decrementLoad() {
	w.loadMux.Lock()
	defer w.loadMux.Unlock()

	if w.currentLoad > 0 {
		w.currentLoad--
	}
}

// register registers the worker with the server and obtains a worker ID.
func (w *Worker) register() error {
	req := w.buildRegisterRequest()

	res, err := w.client.RegisterWorker(w.ctx, req)
	if err != nil {
		return err
	}

	w.id = res.WorkerId
	return nil
}

// buildRegisterRequest constructs the RegisterWorkerRequest message.
func (w *Worker) buildRegisterRequest() *proto.RegisterWorkerRequest {
	taskTypes := make([]string, 0, len(w.handlers))
	for taskType := range w.handlers {
		taskTypes = append(taskTypes, taskType)
	}

	return &proto.RegisterWorkerRequest{
		Worker: &proto.Worker{
			TaskTypes: taskTypes,
			Capacity:  int32(w.capacity),
			Metadata: map[string]string{
				"address": w.serverAddr,
			},
		},
	}
}

// buildFetchTasksRequest constructs the FetchTaskRequest message.
func (w *Worker) buildFetchTasksRequest() *proto.FetchTaskRequest {
	taskTypes := make([]string, 0, len(w.handlers))
	for taskType := range w.handlers {
		taskTypes = append(taskTypes, taskType)
	}

	return &proto.FetchTaskRequest{
		WorkerId:  w.id,
		TaskTypes: taskTypes,
	}
}

// buildHeartbeatRequest constructs the HeartbeatRequest message.
func (w *Worker) buildHeartbeatRequest() *proto.HeartbeatRequest {
	return &proto.HeartbeatRequest{
		WorkerId:    w.id,
		CurrentLoad: w.getCurrentLoad(),
	}
}

func (w *Worker) GetWorkerID() string {
	return fmt.Sprintf("Worker:%s", w.id)
}
