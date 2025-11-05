package worker

import (
	"time"

	"github.com/mateusmlo/taskqueue/proto"
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

func (w *Worker) FromProtoWorker(pw *proto.Worker) {
	w.ID = pw.WorkerId
	w.TaskTypes = pw.TaskTypes
	w.Address = pw.Metadata["address"]
	w.Capacity = int(pw.Capacity)
	w.LastHeartbeat = time.UnixMilli(int64(pw.LastHeartbeat))
	w.CurrentLoad = int(pw.Capacity)
	w.Metadata = pw.Metadata
}
