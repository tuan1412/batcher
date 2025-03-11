# Batcher

A Go package that provides efficient batching capabilities for processing items in groups rather than individually. Built with Go 1.22 and leverages generics for type safety.

This library was inspired by https://github.com/facebookarchive/muster 

## Features

- Generic implementation that works with any data type
- Configurable batch size and timeout
- Concurrent batch processing with limits
- Graceful shutdown
- Efficient memory usage

## Installation

```bash
go get -u github.com/tuan1412/batcher
```

## Usage

### Basic Example

```go
package main

import (
	"fmt"
	"time"

	"github.com/yourusername/batcher"
)

func main() {
	config := batcher.Config[string]{
		MaxBatchSize:         10,           
		BatchTimeout:         time.Second,  
		MaxConcurrentBatches: 5,            
		PendingWorkCapacity:  100,          
		Flush: func(batch []string) {
			fmt.Printf("Processing batch of %d items: %v\n", len(batch), batch)
		},
	}

	b := batcher.New(config)
	
	if err := b.Start(); err != nil {
		panic(err)
	}
	
	for i := 0; i < 25; i++ {
		b.Add(fmt.Sprintf("item-%d", i))
	}
	
	time.Sleep(3 * time.Second)
	
	if err := b.Stop(); err != nil {
		panic(err)
	}
}
```

### Configuration Options

The `Config` struct provides several options to fine-tune the batcher's behavior:

- `MaxBatchSize`: Maximum number of items in a batch before it's processed
- `BatchTimeout`: Maximum time to wait before processing a non-full batch
- `MaxConcurrentBatches`: Limit on how many batches can be processed concurrently
- `PendingWorkCapacity`: Size of the channel buffer for pending items
- `Flush`: Function that processes a batch of items

## How It Works

1. The batcher collects items via the `Add` method
2. Items are batched based on size (`MaxBatchSize`) or time (`BatchTimeout`)
3. When a batch is ready, it's processed by the `Flush` function
4. The batcher manages concurrency using a `MaxConcurrentBatches`

## License

This project is licensed under the MIT License. 