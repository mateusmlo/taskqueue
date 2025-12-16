package server

import (
	"time"

	"github.com/google/uuid"
	"github.com/mateusmlo/taskqueue/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// WorkerInfo tracks a registered worker's information
type WorkerInfo struct {
	ID            string
	Address       string
	TaskTypes     []string
	Capacity      int
	CurrentLoad   int
	RegisteredAt  time.Time
	LastHeartbeat time.Time
	Metadata      map[string]string
}

func (wi *WorkerInfo) FromProtoWorker(pw *proto.Worker) error {
	uuid, err := uuid.NewV7()
	if err != nil {
		return status.Errorf(codes.Internal, "failed to generate worker UUID: %v", err)
	}

	wi.ID = uuid.String()
	wi.TaskTypes = pw.TaskTypes
	wi.Address = pw.Metadata["address"]
	wi.Capacity = int(pw.Capacity)
	wi.CurrentLoad = 0
	wi.Metadata = pw.Metadata
	wi.RegisteredAt = time.Now()
	wi.LastHeartbeat = time.Now()

	return nil
}
