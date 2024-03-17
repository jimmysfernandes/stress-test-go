package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jimmysfernandes/stress-test/executors"
)

func main() {
	fn := func () error {
		time.Sleep(1 * time.Second)
		fmt.Println("Execution task done")
		return nil
	}

	executor := executors.NewFixedExecutor(
		20 * time.Second,
		2,
	)

	// executor.Execute(context.Background(), fn)

	// stages := []executors.Stage{
	// 	{ Duration: 5 * time.Second, RequestPerSeccond: 2 },
	// 	{ Duration: 5 * time.Second, RequestPerSeccond: 4 },
	// 	{ Duration: 5 * time.Second, RequestPerSeccond: 4 },
	// }

	// executor := executors.NewStagedExecutor(stages)

	wg := sync.WaitGroup{}
	wg.Add(1)
	
	go func() {
		defer wg.Done()
		
		if states, err := executor.Run(context.Background(), fn); err != nil {
			fmt.Println("Error: ", err)
		} else {
			fmt.Println("Expected Iterations: ", states.ExpectedIterations)
			fmt.Println("Expected Requests: ", states.ExpectedRequests)
			fmt.Println("Executed Iterations: ", states.ExecutedIterations)
			fmt.Println("Executed Requests: ", states.ExecutedRequests)
			fmt.Println("Errors: ", states.Errors)
		}
	}()

	time.Sleep(6 * time.Second)
	
	fmt.Println("Stopping executor...")

	// executor.Stop()

	wg.Wait()
}
