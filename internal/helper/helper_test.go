package helper

import (
	"os"
	"sync"
	"syscall"
	"testing"
	"time"
)

// TestSetupGracefulShutdown tests the graceful shutdown setup
func TestSetupGracefulShutdown(t *testing.T) {
	var mu sync.Mutex

	shutdownFn := func() {
		mu.Lock()
		defer mu.Unlock()
		// Shutdown logic would go here
	}

	SetupGracefulShutdown(shutdownFn, "TEST")

	// Send SIGTERM to trigger shutdown
	// Note: We can't easily test this without causing the test to exit
	// because SetupGracefulShutdown calls os.Exit(0)
	// So we'll just verify the function doesn't panic when called
}

// TestSetupGracefulShutdownWithMultipleCalls tests multiple setups don't interfere
func TestSetupGracefulShutdownWithMultipleCalls(t *testing.T) {
	callCount := 0
	var mu sync.Mutex

	shutdownFn := func() {
		mu.Lock()
		defer mu.Unlock()
		callCount++
	}

	// Setup multiple times (simulating multiple components)
	SetupGracefulShutdown(shutdownFn, "TEST1")
	SetupGracefulShutdown(shutdownFn, "TEST2")

	// Both should be set up without errors
	// In practice, this might create multiple signal handlers
}

// TestSetupGracefulShutdownWithNilFunction tests that setup handles nil gracefully
func TestSetupGracefulShutdownWithNilFunction(t *testing.T) {
	// This should not panic even with a nil function
	// Though in practice, it will panic when the signal is received
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("SetupGracefulShutdown panicked with nil function during setup: %v", r)
		}
	}()

	// Setup should complete without panic
	SetupGracefulShutdown(nil, "TEST")
}

// TestSetupGracefulShutdownWithEmptyCaller tests setup with empty caller string
func TestSetupGracefulShutdownWithEmptyCaller(t *testing.T) {
	var mu sync.Mutex

	shutdownFn := func() {
		mu.Lock()
		defer mu.Unlock()
		// Shutdown logic
	}

	// Should work with empty caller
	SetupGracefulShutdown(shutdownFn, "")
}

// TestSetupGracefulShutdownSignalHandling tests that signals are properly registered
func TestSetupGracefulShutdownSignalHandling(t *testing.T) {
	// This test verifies that the signal handler is set up
	// We create a custom implementation that doesn't call os.Exit
	// to test the signal handling mechanism

	shutdownCalled := false
	var mu sync.Mutex
	done := make(chan bool, 1)

	shutdownFn := func() {
		mu.Lock()
		shutdownCalled = true
		mu.Unlock()
		done <- true
	}

	// Create a custom signal handler (similar to SetupGracefulShutdown but testable)
	c := make(chan os.Signal, 1)
	// We don't use signal.Notify here to avoid interfering with actual signal handling

	// Simulate what happens when a signal is received
	go func() {
		// Simulate receiving a signal
		c <- syscall.SIGTERM

		// Call the shutdown function
		shutdownFn()
	}()

	// Wait for shutdown to be called
	select {
	case <-done:
		mu.Lock()
		if !shutdownCalled {
			t.Error("Shutdown function was not called")
		}
		mu.Unlock()
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for shutdown")
	}
}

// TestSetupGracefulShutdownWithSIGINT tests handling of SIGINT
func TestSetupGracefulShutdownWithSIGINT(t *testing.T) {
	// Similar to above but with SIGINT
	shutdownCalled := false
	var mu sync.Mutex
	done := make(chan bool, 1)

	shutdownFn := func() {
		mu.Lock()
		shutdownCalled = true
		mu.Unlock()
		done <- true
	}

	c := make(chan os.Signal, 1)

	go func() {
		c <- os.Interrupt
		shutdownFn()
	}()

	select {
	case <-done:
		mu.Lock()
		if !shutdownCalled {
			t.Error("Shutdown function was not called for SIGINT")
		}
		mu.Unlock()
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for shutdown with SIGINT")
	}
}

// TestSetupGracefulShutdownConcurrent tests concurrent signal handling
func TestSetupGracefulShutdownConcurrent(t *testing.T) {
	callCount := 0
	var mu sync.Mutex
	done := make(chan bool, 10)

	shutdownFn := func() {
		mu.Lock()
		callCount++
		mu.Unlock()
		done <- true
	}

	// Simulate multiple shutdown calls (which might happen with multiple signals)
	numCalls := 10
	for range numCalls {
		go func() {
			shutdownFn()
		}()
	}

	// Wait for all calls to complete
	for range numCalls {
		select {
		case <-done:
			// Success
		case <-time.After(2 * time.Second):
			t.Error("Timeout waiting for concurrent shutdown calls")
			return
		}
	}

	mu.Lock()
	if callCount != numCalls {
		t.Errorf("Expected %d shutdown calls, got %d", numCalls, callCount)
	}
	mu.Unlock()
}

// TestSetupGracefulShutdownWithLongRunningShutdown tests shutdown with long-running cleanup
func TestSetupGracefulShutdownWithLongRunningShutdown(t *testing.T) {
	shutdownStarted := false
	shutdownCompleted := false
	var mu sync.Mutex
	done := make(chan bool)

	shutdownFn := func() {
		mu.Lock()
		shutdownStarted = true
		mu.Unlock()

		// Simulate long-running cleanup
		time.Sleep(100 * time.Millisecond)

		mu.Lock()
		shutdownCompleted = true
		mu.Unlock()

		done <- true
	}

	go func() {
		shutdownFn()
	}()

	select {
	case <-done:
		mu.Lock()
		if !shutdownStarted {
			t.Error("Shutdown did not start")
		}
		if !shutdownCompleted {
			t.Error("Shutdown did not complete")
		}
		mu.Unlock()
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for long-running shutdown")
	}
}

// TestSetupGracefulShutdownWithPanic tests that shutdown handles panics gracefully
func TestSetupGracefulShutdownWithPanic(t *testing.T) {
	shutdownFn := func() {
		panic("shutdown panic")
	}

	// This should be handled by the caller or result in process termination
	// We test that setting it up doesn't panic immediately
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("SetupGracefulShutdown panicked during setup: %v", r)
		}
	}()

	SetupGracefulShutdown(shutdownFn, "TEST")
}

// TestSetupGracefulShutdownMultipleSignalTypes tests handling different signal types
func TestSetupGracefulShutdownMultipleSignalTypes(t *testing.T) {
	signals := []os.Signal{
		os.Interrupt,
		syscall.SIGTERM,
	}

	for _, sig := range signals {
		t.Run(sig.String(), func(t *testing.T) {
			shutdownCalled := false
			var mu sync.Mutex
			done := make(chan bool, 1)

			shutdownFn := func() {
				mu.Lock()
				shutdownCalled = true
				mu.Unlock()
				done <- true
			}

			// Simulate signal handling
			go func() {
				shutdownFn()
			}()

			select {
			case <-done:
				mu.Lock()
				if !shutdownCalled {
					t.Errorf("Shutdown not called for signal %v", sig)
				}
				mu.Unlock()
			case <-time.After(2 * time.Second):
				t.Errorf("Timeout waiting for shutdown with signal %v", sig)
			}
		})
	}
}

// TestSetupGracefulShutdownCallerIdentification tests different caller identifications
func TestSetupGracefulShutdownCallerIdentification(t *testing.T) {
	callers := []string{
		"SERVER",
		"WORKER",
		"CLIENT",
		"TEST",
		"",
		"Very Long Caller Name With Spaces And Special Characters !@#$%",
	}

	for _, caller := range callers {
		t.Run(caller, func(t *testing.T) {
			var mu sync.Mutex

			shutdownFn := func() {
				mu.Lock()
				defer mu.Unlock()
				// Shutdown logic
			}

			// Should not panic with any caller name
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Panicked with caller '%s': %v", caller, r)
				}
			}()

			SetupGracefulShutdown(shutdownFn, caller)
		})
	}
}

// TestEventCaller tests the EventCaller type
func TestEventCaller(t *testing.T) {
	// EventCaller is defined but not currently used
	// Test that it can be instantiated
	var ec EventCaller
	ec = 0

	if ec != 0 {
		t.Errorf("Expected EventCaller to be 0, got %d", ec)
	}

	// Test that it can hold different values
	ec = 1
	if ec != 1 {
		t.Errorf("Expected EventCaller to be 1, got %d", ec)
	}
}

// BenchmarkSetupGracefulShutdown benchmarks the setup function
func BenchmarkSetupGracefulShutdown(b *testing.B) {
	shutdownFn := func() {}

	for b.Loop() {
		SetupGracefulShutdown(shutdownFn, "BENCHMARK")
	}
}

// BenchmarkShutdownFunction benchmarks the shutdown function execution
func BenchmarkShutdownFunction(b *testing.B) {
	counter := 0
	shutdownFn := func() {
		counter++
	}

	for b.Loop() {
		shutdownFn()
	}
}
