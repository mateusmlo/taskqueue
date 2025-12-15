package helper

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

type EventCaller int

func SetupGracefulShutdown(shutdownFn func(), caller string) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-c

		log.Printf("🔌 Shutting down %s...", caller)
		shutdownFn()
		os.Exit(0)
	}()
}
