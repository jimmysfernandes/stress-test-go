package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/jimmysfernandes/stress-test/executors"
)

func main() {
	fn := func (completed chan bool) error {
		time.Sleep(1 * time.Second)
		code := rand.Intn(100)
		if code  > 90 {
			fmt.Println("Error: ", code, " ", time.Now().Format("15:04:05"))
			if (code % 2 == 0) {
				completed <- true
			}
			return fmt.Errorf("Error")
		}
		fmt.Println("Success: ", code, " ", time.Now().Format("15:04:05"))
		return nil
	}

	executor := executors.NewFixedExecutor(60 * time.Second, 2)

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

	wg.Wait()
}
