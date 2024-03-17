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
	startedIterations uint64
	requestCount uint64
	errorCount uint64
	wg sync.WaitGroup
}

func (e *constantRateExecutor) Run(ctx context.Context, fn func(chan bool) error) (ExecutorStats, error) {
	e.mu.Lock()
	if e.status == running {
		defer e.mu.Unlock()
		return ExecutorStats{}, errors.New("executor is already running")
	}
	// Reset stats
	e.status = running
	e.mu.Unlock()
	
	defer e.resetState()

	// Create a new context to handle exit signals
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Handle exit signals
	go exitSignalsHandler(ctx, e)	

	expectedIterations := calculateExpectedIterations(e.duration)
	expectedRequests := calculateExpectedRequests(e.duration, e.requestPerSeccond)

	e.run(ctx, fn, make(chan bool), expectedIterations)
	
	e.wg.Wait()

	return ExecutorStats{
		ExpectedIterations: expectedIterations,
		ExecutedIterations: e.startedIterations,
		ExpectedRequests: expectedRequests,
		ExecutedRequests: e.requestCount,
		Errors: e.errorCount,
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

func (e *constantRateExecutor) resetState() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.startedIterations = 0
	e.requestCount = 0
	e.errorCount = 0
	e.wg = sync.WaitGroup{}
}

func (e *constantRateExecutor) run(ctx context.Context, fn func(chan bool) error, completedChannel chan bool, expectedIterations uint64) error {
	
	e.mu.Lock()
	if (ctx.Err() != nil || e.status == stopped || e.startedIterations >= expectedIterations) {
		defer e.mu.Unlock()
		return nil
	}
	e.mu.Unlock()
	
	select {
		case <-ctx.Done():
			return nil
		case <-completedChannel:
			return nil
		default:
			e.wg.Add(1)
			atomic.AddUint64(&e.startedIterations, 1)
			go func () {
				defer e.wg.Done()
				iterationStats := runIteration(ctx, e.requestPerSeccond, func() error { return fn(completedChannel) })
				atomic.AddUint64(&e.requestCount, iterationStats.requestCount)
				atomic.AddUint64(&e.errorCount, iterationStats.errorCount)
			}()
	}

	time.Sleep(1 * time.Second)

	return e.run(ctx, fn, completedChannel, expectedIterations)
}

func NewFixedExecutor(duration time.Duration, requestPerSeccond uint64) Executor {
	return &constantRateExecutor{
		duration: duration,
		requestPerSeccond: requestPerSeccond,
		status: stopped,
		mu: sync.Mutex{},
	}
}

