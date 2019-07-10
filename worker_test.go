package workerpool

import (
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"
)

func TestSequentialWorkV(t *testing.T) {
	t.Parallel()
	taskQueue := make(chan *Task)
	outChan := make(chan string)
	durationTests := [...]struct{ delay, total int }{{1, 1}, {1, 2}, {1, 3}}
	w := Worker{
		Name: "1",
	}
	go w.Listen(taskQueue)

	start := time.Now()
	for _, testCase := range durationTests {
		delay := testCase.delay
		taskF := func() string {
			time.Sleep(time.Duration(delay) * time.Second)
			return fmt.Sprintf("%v second delay", delay)
		}
		go func() {
			task := &Task{
				Task:       taskF,
				TaskResult: outChan,
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

func TestSimultaneousWorkV(t *testing.T) {
	t.Parallel()
	workChan := make(chan *Task)
	outChan := make(chan string)

	durationTests := [...]struct{ delay, total int }{{2, 2}, {2, 2}, {2, 2}}
	for i := 0; i < len(durationTests); i++ {
		worker := Worker{
			Name: strconv.Itoa(i),
		}
		go worker.Listen(workChan)
	}

	start := time.Now()
	for _, testCase := range durationTests {
		delay := testCase.delay
		taskF := func() string {
			var start = time.Now()
			time.Sleep(time.Duration(delay) * time.Second)
			return fmt.Sprintf("%v", start.Format("15:04:05"))
		}
		go func() {
			task := &Task{
				Task:       taskF,
				TaskResult: outChan,
			}
			workChan <- task
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
