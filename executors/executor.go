package executors

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type executorStatus int32

const (
	running executorStatus = 1
	stopped executorStatus = 0
)

type ExecutorStats struct {
	ExpectedIterations uint64 `json:"expectedIterations"`
	ExecutedIterations uint64 `json:"executedIterations"`
	ExpectedRequests uint64 `json:"expectedRequests"`
	ExecutedRequests uint64 `json:"executedRequests"`
	Errors uint64 `json:"errors"`
}

type iterationStats struct {
	requestCount uint64
	errorCount uint64
}

type Executor interface {
	Run(ctx context.Context, fn func() error) (ExecutorStats, error)
	Stop() error
}

func calculateExpectedIterations(duration time.Duration) uint64 {
	return uint64(duration.Seconds())
}

func calculateExpectedRequests(duration time.Duration, requestPerSeccond uint64) uint64 {
	return uint64(duration.Seconds()) * requestPerSeccond
}

func exitSignalsHandler(ctx context.Context, executor Executor) {
	signals := make(chan os.Signal, 1)
  signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
  
	// Block until a signal is received.
	for {
		select {
			case <-ctx.Done():
				return
			case <-signals:
				executor.Stop()
		}
	}
}

func runIteration(ctx context.Context, requestPerSeccond uint64, fn func() error) iterationStats {
	wg := sync.WaitGroup{}

	requestCount := uint64(0)
	errorCount := uint64(0)

	LOOP:
		for i := 0; i < int(requestPerSeccond); i++ {
			select {
				case <-ctx.Done():
					break LOOP
				default:
					atomic.AddUint64(&requestCount, 1)
					wg.Add(1)
					go func(errorCount *uint64) {
						defer wg.Done()
						if err := fn(); err != nil {
							atomic.AddUint64(errorCount, 1)
						}
					}(&errorCount)
			}
		}
	
	wg.Wait()

	return iterationStats{
		requestCount: requestCount,
		errorCount: errorCount,
	}
}
