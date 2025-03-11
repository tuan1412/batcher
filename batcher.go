package batcher

import (
	"errors"
	"sync"
	"time"
)

var errZeroBoth = errors.New(
	"batcher: MaxBatchSize and BatchTimeout can't both be zero",
)

type waitGroup interface {
	Add(delta int)
	Done()
	Wait()
}

type Config[T any] struct {
	MaxBatchSize         uint
	BatchTimeout         time.Duration
	MaxConcurrentBatches uint
	PendingWorkCapacity  uint
	Flush                func(batch []T)
}

type Batcher[T any] struct {
	Config[T]
	work      chan T
	workGroup waitGroup
}

func New[T any](config Config[T]) *Batcher[T] {
	return &Batcher[T]{
		Config: config,
	}
}

func (c *Batcher[T]) Add(item T) {
	c.work <- item
}

func (c *Batcher[T]) Start() error {
	if int64(c.BatchTimeout) == 0 && c.MaxBatchSize == 0 {
		return errZeroBoth
	}

	if c.MaxConcurrentBatches == 0 {
		c.workGroup = &sync.WaitGroup{}
	} else {
		c.workGroup = NewLimitGroup(c.MaxConcurrentBatches + 1)
	}

	c.work = make(chan T, c.PendingWorkCapacity)
	c.workGroup.Add(1)
	go c.worker()
	return nil
}

func (c *Batcher[T]) Stop() error {
	close(c.work)
	c.workGroup.Wait()
	return nil
}

func (c *Batcher[T]) worker() {
	defer c.workGroup.Done()
	batch := make([]T, 0, c.MaxBatchSize)
	var count uint
	var batchTimer *time.Timer
	var batchTimeout <-chan time.Time
	send := func() {
		c.workGroup.Add(1)
		go func(batch []T) {
			defer c.workGroup.Done()
			c.Flush(batch)
		}(batch)

		batch = make([]T, 0, c.MaxBatchSize)
		count = 0
		if batchTimer != nil {
			batchTimer.Stop()
		}
	}
	recv := func(item T, open bool) bool {
		if !open {
			if count != 0 {
				send()
			}
			return true
		}
		batch = append(batch, item)
		count++
		if c.MaxBatchSize != 0 && count >= c.MaxBatchSize {
			send()
		} else if int64(c.BatchTimeout) != 0 && count == 1 {
			batchTimer = time.NewTimer(c.BatchTimeout)
			batchTimeout = batchTimer.C
		}
		return false
	}
	for {
		select {
		case item, open := <-c.work:
			if recv(item, open) {
				return
			}
		default:
			select {
			case item, open := <-c.work:
				if recv(item, open) {
					return
				}
			case <-batchTimeout:
				send()
			}
		}
	}
}
