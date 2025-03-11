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

func TestBatcherPendingWorkCapacity(t *testing.T) {
	// Create channels to control and track batch processing
	releaseProcessing := make(chan struct{})

	// Set up configuration with small MaxConcurrentBatches and PendingWorkCapacity
	config := Config[int]{
		MaxBatchSize:         5,
		BatchTimeout:         50 * time.Millisecond,
		MaxConcurrentBatches: 2, // Only allow 2 concurrent batches
		PendingWorkCapacity:  3, // Very small capacity to test blocking
		Flush: func(batch []int) {
			t.Logf("Flushing batch: %v", batch)
			<-releaseProcessing
			t.Logf("Released batch: %v", batch)
		},
	}

	b := New(config)

	if err := b.Start(); err != nil {
		t.Fatalf("Failed to start batcher: %v", err)
	}

	for i := 0; i < 15; i++ {
		t.Logf("Added item %d", i) // These should be queued in the work channel
		b.Add(i)
	}

	// Create a channel to signal if Add blocks
	addBlocked := make(chan bool, 1)

	// Try to add one more item, which should block because we've reached capacity
	go func() {
		// Give a timeout to detect if Add blocks
		timer := time.NewTimer(500 * time.Millisecond) // Longer timeout for more reliability
		done := make(chan struct{})

		go func() {
			b.Add(999)
			t.Log("Add(999) returned") // This should block because the channel is full
			close(done)
		}()

		select {
		case <-done:
			addBlocked <- false // Add didn't block
		case <-timer.C:
			addBlocked <- true // Add blocked as expected
		}
	}()

	// Give some time for the goroutine to start and attempt to add
	time.Sleep(100 * time.Millisecond)

	// Check if Add blocked
	blocked := <-addBlocked
	if !blocked {
		t.Error("Expected Add to block when PendingWorkCapacity is reached, but it didn't")
	}

	releaseProcessing <- struct{}{}
	releaseProcessing <- struct{}{}
	releaseProcessing <- struct{}{}
	releaseProcessing <- struct{}{}

	// Clean up
	t.Log("Stopping batcher")
	if err := b.Stop(); err != nil {
		t.Fatalf("Failed to stop batcher: %v", err)
	}

	// Make sure all batches are processed
	close(releaseProcessing)
}

func BenchmarkBatcher(b *testing.B) {
	config := Config[int]{
		MaxBatchSize:         100,
		BatchTimeout:         50 * time.Millisecond,
		MaxConcurrentBatches: 4,
		PendingWorkCapacity:  1000,
		Flush: func(batch []int) {
		},
	}

	batcher := New(config)
	if err := batcher.Start(); err != nil {
		b.Fatalf("Failed to start batcher: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		batcher.Add(i)
	}

	b.StopTimer()

	if err := batcher.Stop(); err != nil {
		b.Fatalf("Failed to stop batcher: %v", err)
	}
}
