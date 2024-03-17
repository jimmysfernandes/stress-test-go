package executors

import (
	"context"
	"errors"
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
	startedIterations uint64
	requestCount uint64
	errorCount uint64
	wg sync.WaitGroup
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
	
	defer e.resetState()

	// Create a new context to handle exit signals
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Handle exit signals
	go exitSignalsHandler(ctx, e)	
	
	expectedIterations, expectedRequests := e.calculateExpectations(e.stages)

	e.run(ctx, fn, make(chan bool), expectedIterations, 0)	

	e.wg.Wait()

	return ExecutorStats{
		ExpectedIterations: expectedIterations,
		ExecutedIterations: e.startedIterations,
		ExpectedRequests: expectedRequests,
		ExecutedRequests: e.requestCount,
		Errors: e.errorCount,
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

func (e *phasedExecutor) resetState() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.startedIterations = 0
	e.requestCount = 0
	e.errorCount = 0
	e.wg = sync.WaitGroup{}
}

func (e *phasedExecutor) run(ctx context.Context, fn func(chan bool) error, completedChannel chan bool, expectedIterations uint64, stageNumber int) error {

	e.mu.Lock()
	if (ctx.Err() != nil || e.status == stopped) {
		return nil
	}
	e.mu.Unlock()

	
	if stageNumber > (len(e.stages) - 1) {
		return nil
	}

	stage := e.stages[stageNumber]
	e.wg.Add(1)
	go func (ctx context.Context, s Stage) {
	defer e.wg.Done()
		iterationStats := e.runStage(ctx, s, func() error { return fn(completedChannel) })
		atomic.AddUint64(&e.requestCount, iterationStats.requestCount)
		atomic.AddUint64(&e.errorCount, iterationStats.errorCount)
	}(ctx, stage)

	time.Sleep(stage.Duration)

	return e.run(ctx, fn, completedChannel, expectedIterations, stageNumber + 1)
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
