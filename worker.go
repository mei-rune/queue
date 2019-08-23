package queue

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/RichardKnop/machinery/v1/log"
	"github.com/opentracing/opentracing-go"
	"github.com/runner-mei/log"
)

// Worker represents a single worker process
type Worker struct {
	runStatus int32
	reserved1 int32
	closeWait sync.WaitGroup

	tasks    list.List
	taskLock sync.Mutex

	logger      log.Logger
	id          string
	broker      *Broker
	concurrency int

	ErrorHandler    func(ctx context.Context, task *Task, err error)
	PreTaskHandler  func(ctx context.Context, task *Task)
	PostTaskHandler func(ctx context.Context, task *Task)
}

func (worker *Worker) Start(concurrency int) error {
	if !atomic.CompareAndSwapInt32(&worker.runStatus, 0, 1) {
		return errors.New("already start")
	}

	return nil
}

func (worker *Worker) Close() error {
	if !atomic.CompareAndSwapInt32(&worker.runStatus, 1, 2) {
		return nil
	}

	worker.closeWait.Wait()
	return nil
}

// LaunchAsync is a non blocking version of Launch
func (worker *Worker) runLoop(thread int) error {
	defer worker.closeWait.Done()

	broker := worker.broker
	cfg := broker.GetConfig()

	logger := worker.logger.With(log.Int("worker.thread", thread))

	errorCount := 0
	for atomic.LoadInt32(&worker.runStatus) != 1 {
		task, err := broker.Fetch(worker.id)
		if err != nil {
			errorCount++
			if errorCount%100 <= 3 {
				worker.logger.Warn("Broker failed with error", log.Int("error_count", errorCount), log.Error(err))
			}
			continue
		}
		errorCount = 0

		if task != nil {
			if task.Context == nil {
				task.Context = context.Background()
			}
			if task.Logger == nil {
				task.Logger = logger
			}
			worker.doTask(thread, task)
		} else {
			if thread == 0 {
				worker.heartbeat()
			}
		}
	}
}

func (worker *Worker) heartbeat() {
	list := func() []TaskID {
		worker.taskLock.Lock()
		defer worker.taskLock.Unlock()

		var list = make([]TaskID, 0, worker.tasks.Len())
		for it := worker.tasks.Front(); it != nil; it = it.Next() {
			list = append(list, it.Value.(*Task).ID)
		}
		return list
	}()

	// Update task state to FAILURE
	if err := worker.broker.Report(ctx, &ReportMessage{
		WorkerID: worker.id,
		Type:     ReportHeartbeat,
		Message:  list,
	}); err != nil {
		worker.logger.Warn("heartbeat failure", log.Error(err))
	}
}

// Process handles received tasks and triggers success/error callbacks
func (worker *Worker) doTask(thread int, task *Task) {
	defer func() {
		if o := recover(); o != nil {
			worker.taskFailure(task, fmt.Errorf("PANIC: %v\r\n%s", o, debug.Stack()))
		}

		if task.element != nil {
			worker.taskLock.Lock()
			worker.tasks.Remove(task.element)
			worker.taskLock.Unlock()
		}
	}()

	taskSpan := StartSpanFromTask(task)

	span.SetTag("worker.thread", thread)
	span.SetTag("worker.id", worker.id)

	task.Context = opentracing.ContextWithSpan(task.Context, taskSpan)
	task.Logger = log.Span(task.Logger, taskSpan).
		With(log.String("task.id", task.ID),
			log.String("task.method", task.Method),
			log.String("task.name", task.Name))

	worker.taskLock.Lock()
	task.element = worker.tasks.PushBack(task)
	worker.taskLock.Unlock()

	if !worker.taskAccepted(task) {
		return
	}

	runnable, err := ToRunnable(task)
	if err != nil {
		worker.taskFailure(task, err)
		return
	}

	// Call the task
	taskResult, taskErr := runnable.Run()
	if taskErr != nil {
		worker.taskFailure(task, taskErr)
		return
	}

	worker.taskSucceeded(task, taskResult)
}

// taskAccepted updates the task state to accepted
func (worker *Worker) taskAccepted(ctx context.Context, task *Task) bool {
	// Update task state to RECEIVED
	if err = worker.broker.Report(context.Background(), &ReportEvent{
		WorkerID: worker.id,
		TaskID:   task.ID,
		Type:     ReportAccepted,
	}); err != nil {
		task.Logger.Warn("report status failure", log.Error(err))
		return false
	}

	if worker.preTaskHandler != nil {
		worker.preTaskHandler(ctx, task)
	} else {
		task.Logger.Info("task accepted")
	}
	return true
}

// taskSucceeded updates the task state to succeeded
func (worker *Worker) taskSucceeded(ctx context.Context, task *Task, result interface{}) {
	// Update task state to SUCCESS
	if err := worker.broker.Report(ctx, &ReportEvent{
		WorkerID: worker.id,
		TaskID:   task.ID,
		Type:     ReportSucceeded,
		Data:     result,
	}); err != nil {
		task.Logger.Warn("report status failure", log.Error(err))
	}

	if worker.postTaskHandler != nil {
		worker.postTaskHandler(ctx, task)
	} else {
		task.Logger.Info("task succeeded")
	}
}

// taskFailed updates the task state to failure
func (worker *Worker) taskFailure(ctx context.Context, task *Task, taskErr error) {
	// Update task state to FAILURE
	if err := worker.broker.Report(ctx, &ReportEvent{
		WorkerID: worker.id,
		TaskID:   task.ID,
		Type:     ReportFailure,
		Data:     taskErr.Error(),
	}); err != nil {
		task.Logger.Warn("report status failure", log.Error(err))
	}

	if worker.errorHandler != nil {
		worker.errorHandler(ctx, task, taskErr)
	} else {
		task.Logger.Warn("task failure", log.Error(taskErr))
	}
	return nil
}

func NewWorker(id string, broker *Broker, logger log.Logger) *Worker {
	return &Worker{
		tasks:  list.New(),
		logger: logger.With(log.String("worker.id", id)),
		id:     id,
		broker: broker,
	}
}

// // retryTask decrements RetryCount counter and republishes the task to the queue
// func (worker *Worker) taskRetry(signature *tasks.Signature) error {
// 	// Update task state to RETRY
// 	if err := worker.server.GetBackend().SetStateRetry(signature); err != nil {
// 		return fmt.Errorf("Set state to 'retry' for task %s returned error: %s", signature.UUID, err)
// 	}

// 	// Decrement the retry counter, when it reaches 0, we won't retry again
// 	signature.RetryCount--

// 	// Increase retry timeout
// 	signature.RetryTimeout = retry.FibonacciNext(signature.RetryTimeout)

// 	// Delay task by signature.RetryTimeout seconds
// 	eta := time.Now().UTC().Add(time.Second * time.Duration(signature.RetryTimeout))
// 	signature.ETA = &eta

// 	log.WARNING.Printf("Task %s failed. Going to retry in %d seconds.", signature.UUID, signature.RetryTimeout)

// 	// Send the task back to the queue
// 	_, err := worker.server.SendTask(signature)
// 	return err
// }

// // taskRetryIn republishes the task to the queue with ETA of now + retryIn.Seconds()
// func (worker *Worker) retryTaskIn(ctx context.Context, task *Task, retryIn time.Duration) error {
// 	// Update task state to RETRY
// 	if err := worker.server.GetBackend().SetStateRetry(signature); err != nil {
// 		return fmt.Errorf("Set state to 'retry' for task %s returned error: %s", signature.UUID, err)
// 	}

// 	// Delay task by retryIn duration
// 	eta := time.Now().UTC().Add(retryIn)
// 	signature.ETA = &eta

// 	log.WARNING.Printf("Task %s failed. Going to retry in %.0f seconds.", signature.UUID, retryIn.Seconds())

// 	// Send the task back to the queue
// 	_, err := worker.server.SendTask(signature)
// 	return err
// }
