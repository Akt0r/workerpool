package workerpool

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func TestSequentialWorkV(t *testing.T) {
	t.Parallel()
	taskQueue := make(chan *Task)
	outChan := make(chan TaskResult)
	durationTests := [...]struct{ delay, total int }{{1, 1}, {1, 2}, {1, 3}}
	w := Worker{
		Name: "1",
	}
	go w.Listen(taskQueue)

	start := time.Now()
	for _, testCase := range durationTests {
		delay := testCase.delay
		taskF := func() TaskResult {
			time.Sleep(time.Duration(delay) * time.Second)
			return fmt.Sprintf("%v second delay", delay)
		}
		go func() {
			task := &Task{
				Task:   taskF,
				Result: outChan,
			}
			taskQueue <- task
		}()
	}

	for i, testCase := range durationTests {
		select {
		case result := <-outChan:
			t.Log(result)
			duration := int(math.Round(time.Since(start).Seconds()))
			if testCase.total != duration {
				t.Errorf("Run %v: expected %v got %v", i, testCase.total, duration)
			}
		}
	}
}
