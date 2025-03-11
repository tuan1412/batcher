package batcher

import (
	"sync"
	"testing"
	"time"
)

func TestBatcherBasic(t *testing.T) {
	processed := make(chan []string, 10)

	config := Config[string]{
		MaxBatchSize:         5,
		BatchTimeout:         100 * time.Millisecond,
		MaxConcurrentBatches: 2,
		PendingWorkCapacity:  20,
		Flush: func(batch []string) {
			processed <- batch
		},
	}

	b := New(config)

	if err := b.Start(); err != nil {
		t.Fatalf("Failed to start batcher: %v", err)
	}

	items := []string{"item1", "item2", "item3", "item4", "item5"}
	for _, item := range items {
		b.Add(item)
	}

	select {
	case batch := <-processed:
		if len(batch) != 5 {
			t.Errorf("Expected batch size of 5, got %d", len(batch))
		}
		for i, item := range batch {
			if item != items[i] {
				t.Errorf("Expected item %s at position %d, got %s", items[i], i, item)
			}
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Timeout waiting for batch to be processed")
	}

	if err := b.Stop(); err != nil {
		t.Fatalf("Failed to stop batcher: %v", err)
	}
}

func TestBatcherTimeout(t *testing.T) {
	processed := make(chan []string, 10)

	config := Config[string]{
		MaxBatchSize:         10,
		BatchTimeout:         50 * time.Millisecond,
		MaxConcurrentBatches: 2,
		PendingWorkCapacity:  20,
		Flush: func(batch []string) {
			processed <- batch
		},
	}

	b := New(config)

	if err := b.Start(); err != nil {
		t.Fatalf("Failed to start batcher: %v", err)
	}

	items := []string{"item1", "item2", "item3"}
	for _, item := range items {
		b.Add(item)
	}

	select {
	case batch := <-processed:
		if len(batch) != 3 {
			t.Errorf("Expected batch size of 3, got %d", len(batch))
		}
		for i, item := range batch {
			if item != items[i] {
				t.Errorf("Expected item %s at position %d, got %s", items[i], i, item)
			}
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Timeout waiting for batch to be processed")
	}

	if err := b.Stop(); err != nil {
		t.Fatalf("Failed to stop batcher: %v", err)
	}
}

func TestBatcherConcurrency(t *testing.T) {
	var mu sync.Mutex
	batches := make([][]int, 0)

	// Track active and max concurrent batches
	active := 0
	maxConcurrent := 0

	// Use a wait group to ensure we wait for all batch processing to complete
	var wg sync.WaitGroup

	config := Config[int]{
		MaxBatchSize:         10,
		BatchTimeout:         50 * time.Millisecond,
		MaxConcurrentBatches: 3,
		PendingWorkCapacity:  100,
		Flush: func(batch []int) {
			mu.Lock()
			active++
			if active > maxConcurrent {
				maxConcurrent = active
			}
			mu.Unlock()

			// Simulate processing time
			time.Sleep(100 * time.Millisecond)

			mu.Lock()
			batches = append(batches, batch)
			active--
			mu.Unlock()

			wg.Done()
		},
	}

	b := New(config)

	if err := b.Start(); err != nil {
		t.Fatalf("Failed to start batcher: %v", err)
	}

	expectedBatches := 5
	wg.Add(expectedBatches)

	for i := 0; i < 50; i++ {
		b.Add(i)
	}

	wg.Wait()

	if err := b.Stop(); err != nil {
		t.Fatalf("Failed to stop batcher: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(batches) != expectedBatches {
		t.Errorf("Expected exactly %d batches, got %d", expectedBatches, len(batches))
	}

	if maxConcurrent != 3 {
		t.Errorf("Expected max concurrent batches to be 3, got %d", maxConcurrent)
	}

	total := 0
	for _, batch := range batches {
		total += len(batch)
	}

	if total != 50 {
		t.Errorf("Expected 50 total items processed, got %d", total)
	}
}

func TestBatcherInvalidConfig(t *testing.T) {
	config := Config[string]{
		MaxBatchSize:         0,
		BatchTimeout:         0,
		MaxConcurrentBatches: 2,
		PendingWorkCapacity:  20,
		Flush: func(batch []string) {
		},
	}

	b := New(config)

	err := b.Start()
	if err == nil {
		t.Error("Expected error when starting batcher with invalid config, got nil")
	}
}

func TestBatcherMaxPendingBlocking(t *testing.T) {
	// Control channels to coordinate the test
	flushStarted := make(chan bool, 1)
	allowFlush := make(chan bool, 1)
	addCompleted := make(chan bool, 1)

	config := Config[int]{
		MaxBatchSize:         2,         // Small batch size
		BatchTimeout:         time.Hour, // Long timeout to ensure batching by size only
		MaxConcurrentBatches: 1,         // Single worker
		PendingWorkCapacity:  1,         // Only one pending batch allowed
		Flush: func(batch []int) {
			// Signal that flush has started
			flushStarted <- true
			// Wait for test to allow flush to complete
			<-allowFlush
		},
	}

	b := New(config)
	if err := b.Start(); err != nil {
		t.Fatalf("Failed to start batcher: %v", err)
	}

	// Add first batch (2 items) - this will start processing
	b.Add(1)
	b.Add(2)

	// Wait for first batch to start processing
	select {
	case <-flushStarted:
		// Good - flush started
	case <-time.After(100 * time.Millisecond):
		t.Fatal("First batch did not start processing")
	}

	// Add second batch (2 items) but not flush
	b.Add(3)
	b.Add(4)

	// Fill pending capacity
	b.Add(5)

	// After pending capacity is filled, add another item, check that it blocks
	// Try to add another item in a goroutine - should block
	go func() {
		b.Add(6)
		addCompleted <- true // Signal we completed the add
	}()

	// Verify the add is blocked
	select {
	case <-addCompleted:
		t.Error("Add did not block when max pending was reached")
	case <-time.After(100 * time.Millisecond):

	}

	// Allow first batch to complete
	allowFlush <- true

	// Wait for first batch to complete and second batch to start
	select {
	case <-flushStarted:
		// Good - second batch started
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Second batch did not start processing")
	}

	// Allow second batch to complete
	allowFlush <- true

	// Wait for blocked Add to complete
	select {
	case <-addCompleted:
		// Good - Add completed after batch was processed
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Add remained blocked after batch was processed")
	}

	// Allow final batch to complete
	allowFlush <- true

	// Clean shutdown
	b.Stop()
}
