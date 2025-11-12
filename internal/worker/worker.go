package worker

import (
	"time"

	"github.com/google/uuid"
	"github.com/mateusmlo/taskqueue/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Worker struct {
	ID            string
	Address       string
	RegisteredAt  time.Time
	LastHeartbeat time.Time
	TaskTypes     []string
	Capacity      int
	CurrentLoad   int
	Metadata      map[string]string
}

// FromProtoWorker initializes a Worker instance from a proto.Worker message (server generates ID)
func (w *Worker) FromProtoWorker(pw *proto.Worker) error {
	uuid, err := uuid.NewV7()
	if err != nil {
		return status.Errorf(codes.Internal, "failed to generate worker UUID: %v", err)
	}

	w.ID = uuid.String()
	w.TaskTypes = pw.TaskTypes
	w.Address = pw.Metadata["address"]
	w.Capacity = int(pw.Capacity)
	w.CurrentLoad = 0
	w.Metadata = pw.Metadata
	w.RegisteredAt = time.Now()
	w.LastHeartbeat = time.Now()

	return nil
}
