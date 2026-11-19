//go:build racedemo

// Race detector demo. The racedemo build tag keeps it out of normal runs,
// so `go test -race ./...` and CI stay green.
//
// Run it with:
//
//	go test -race -tags racedemo -run TestRaceDemo -v ./internal/core/service/
//
// Expected: TestRaceDemo_PlainCounter fails with "WARNING: DATA RACE",
// TestRaceDemo_AtomicCounter passes.
package service

import (
	"sync"
	"sync/atomic"
	"testing"
)

// Two batch-insert goroutines both report how many rows they saved,
// the same job PipelineService.successCount does.
const (
	demoWorkers = 2
	demoBatches = 1000
)

// BROKEN: a plain int64 is read-modify-written by two goroutines with no
// synchronization. `count++` is load, add, store, so increments can be lost.
func TestRaceDemo_PlainCounter(t *testing.T) {
	var successCount int64
	var wg sync.WaitGroup

	for range demoWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range demoBatches {
				successCount++ // DATA RACE: unsynchronized write from multiple goroutines
			}
		}()
	}
	wg.Wait()

	t.Logf("successCount = %d (expected %d)", successCount, demoWorkers*demoBatches)
}

// FIXED: atomic.Int64, which is what PipelineService actually uses.
func TestRaceDemo_AtomicCounter(t *testing.T) {
	var successCount atomic.Int64
	var wg sync.WaitGroup

	for range demoWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range demoBatches {
				successCount.Add(1)
			}
		}()
	}
	wg.Wait()

	if got := successCount.Load(); got != demoWorkers*demoBatches {
		t.Fatalf("successCount = %d, want %d", got, demoWorkers*demoBatches)
	}
}
