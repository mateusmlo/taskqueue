package main

import (
	"context"
	"slices"

	"github.com/mateusmlo/taskqueue/internal/helper"
	"github.com/mateusmlo/taskqueue/internal/worker"
)

// An example task handler that just reverses a provided string
type ReverseStringHandler struct {
}

// Handle reverses the input string
func (rh *ReverseStringHandler) Handle(ctx context.Context, payload []byte) ([]byte, error) {
	s := slices.Clone(payload)
	slices.Reverse(s)

	return s, nil
}

func main() {
	worker := worker.NewWorker("localhost:50051", 10)

	worker.RegisterHandler("reverseStr", &ReverseStringHandler{})

	if err := worker.Start(); err != nil {
		panic(err)
	}

	helper.SetupGracefulShutdown(worker.Stop, worker.GetWorkerID())

	// Keep the main goroutine running until shutdown signal received
	select {}
}
