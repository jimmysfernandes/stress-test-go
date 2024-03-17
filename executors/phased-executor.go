package executors

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Stage struct {
	Duration time.Duration
	RequestPerSeccond uint64
}

type phasedExecutor struct {
	stages []Stage
	status executorStatus
	mu sync.Mutex
}

func (e *phasedExecutor) Run(ctx context.Context, fn func(chan bool) error) (ExecutorStats, error) {
	// Check if the executor is already running
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
	
	expectedIterations, expectedRequests := e.calculateExpectations(e.stages)
	
	var wg sync.WaitGroup
	var requestCount, errorCount uint64
	completedChannel := make(chan bool)

	fmt.Println("Starting executor...")

	LOOP:
		for _, stage := range e.stages {			
			e.mu.Lock()
			if (ctx.Err() != nil || e.status == stopped) {
				defer e.mu.Unlock()
				break LOOP
			}
			e.mu.Unlock()
			
			select {
				case <-completedChannel:
					break LOOP
				default:
					wg.Add(1)
					go func (ctx context.Context, s Stage) {
						defer wg.Done()
						iterationStats := e.runStage(ctx, s, func() error { return fn(completedChannel) })
						atomic.AddUint64(&requestCount, iterationStats.requestCount)
						atomic.AddUint64(&errorCount, iterationStats.errorCount)
					}(ctx, stage)
					
					time.Sleep(stage.Duration)		
			}
		}

	wg.Wait()

	return ExecutorStats{
		ExpectedIterations: expectedIterations,
		ExpectedRequests: expectedRequests,
		ExecutedIterations: expectedIterations,
		ExecutedRequests: requestCount,
		Errors: errorCount,
	}, nil
}

func (e *phasedExecutor) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.status == stopped {
		return errors.New("executor is already stopped")
	}

	e.status = stopped
	
	return nil
}

func (e *phasedExecutor) calculateExpectations(stages []Stage) (uint64, uint64) {
	expectedIterations := uint64(0)
	expectedRequests := uint64(0)
	for _, stage := range stages {
		durationsInMs := uint64(stage.Duration.Seconds())
		expectedIterations += durationsInMs
		expectedRequests += stage.RequestPerSeccond * durationsInMs
	}
	return expectedIterations, expectedRequests
}

func (e *phasedExecutor) runStage(ctx context.Context, stage Stage, fn func() error) iterationStats {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	
	expectedIterations := calculateExpectedIterations(stage.Duration)

	var wg sync.WaitGroup
	var requestCount, errorCount, startedIterations uint64

	LOOP:
		for {
			if startedIterations == uint64(expectedIterations) {
				break LOOP
			}
			
			select {
				case <-ctx.Done():
					break LOOP
				case <-ticker.C:
					wg.Add(1)
					atomic.AddUint64(&startedIterations, 1)
					go func () {
						defer wg.Done()
						iterationStats := runIteration(ctx, stage.RequestPerSeccond, fn)
						atomic.AddUint64(&requestCount, iterationStats.requestCount)
						atomic.AddUint64(&errorCount, iterationStats.errorCount)
					}()
			}
		}

	wg.Wait()

	return iterationStats{
		requestCount: requestCount,
		errorCount: errorCount,
	}
}

func NewStagedExecutor(stages []Stage) Executor {
	return &phasedExecutor{
		stages: stages,
		status: stopped,
		mu: sync.Mutex{},
	}
}
