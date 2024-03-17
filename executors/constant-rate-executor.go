package executors

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type constantRateExecutor struct {
	duration time.Duration
	requestPerSeccond uint64
	status executorStatus
	mu sync.Mutex
}

func (e *constantRateExecutor) Run(ctx context.Context, fn func() error) (ExecutorStats, error) {
	e.mu.Lock()
	if e.status == running {
		defer e.mu.Unlock()
		return ExecutorStats{}, errors.New("executor is already running")
	}
	e.status = running
	e.mu.Unlock()
	
	defer func() {
		e.mu.Lock()
		e.status = stopped
		e.mu.Unlock()
	}()

	// Create a new context to handle exit signals
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Handle exit signals
	go exitSignalsHandler(ctx, e)	

	expectedIterations := calculateExpectedIterations(e.duration)
	expectedRequests := calculateExpectedRequests(e.duration, e.requestPerSeccond)

	ticker := time.NewTicker(1 * time.Second)

	var wg sync.WaitGroup
	var startedIterations, requestCount, errorCount uint64

	LOOP:
		for {
			e.mu.Lock()
			if (ctx.Err() != nil || e.status == stopped || e.isCompleted(startedIterations, expectedIterations)) {
				defer e.mu.Unlock()
				break LOOP
			}
			e.mu.Unlock()
			select {
				case <-ctx.Done():
					break LOOP
				case <-ticker.C:
					wg.Add(1)
					atomic.AddUint64(&startedIterations, 1)
					go func () {
						defer wg.Done()
						iterationStats := runIteration(ctx, e.requestPerSeccond, fn)
						atomic.AddUint64(&requestCount, iterationStats.requestCount)
						atomic.AddUint64(&errorCount, iterationStats.errorCount)
					}()
			}
		}
	
	wg.Wait()

	return ExecutorStats{
		ExpectedIterations: expectedIterations,
		ExecutedIterations: startedIterations,
		ExpectedRequests: expectedRequests,
		ExecutedRequests: requestCount,
		Errors: errorCount,
	}, nil
}

func (e *constantRateExecutor) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.status == stopped {
		return errors.New("executor is already stopped")
	}

	e.status = stopped
	
	return nil
}

func (e *constantRateExecutor) isCompleted(startedIterations uint64, expectedIterations uint64) bool {
	return startedIterations >= expectedIterations
}

func NewFixedExecutor(duration time.Duration, requestPerSeccond uint64) Executor {
	return &constantRateExecutor{
		duration: duration,
		requestPerSeccond: requestPerSeccond,
		status: stopped,
		mu: sync.Mutex{},
	}
}

