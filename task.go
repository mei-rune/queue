package queue

import (
	"container/list"
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	opentracing_ext "github.com/opentracing/opentracing-go/ext"
	"github.com/runner-mei/errors"
	"github.com/runner-mei/log"
)

// opentracing tags
var (
	TaskQueueTag = opentracing.Tag{Key: string(opentracing_ext.Component), Value: "taskqueue"}
)

type Headers struct {
	TraceContext opentracing.TextMapCarrier `json:"trace_context"`
	WorkerID     WorkerID                   `json:"worker_id"`
	RoutingKey   string                     `json:"routing_key"`
	Immutable    bool                       `json:"immutable"`
	RetryCount   int                        `json:"retry_count"`
	RetryTimeout int                        `json:"retry_timeout"`
}

type WorkerID string

type TaskID int64

func (id TaskID) String() string {
	return strconv.FormatInt(int64(id), 10)
}

// Task represents a single task invocation
type Task struct {
	element *list.Element   `json:"-"`
	Context context.Context `json:"-"`
	Logger  log.Logger      `json:"-"`
	ID      TaskID          `json:"id"`
	Title   string          `json:"title"`
	Headers Headers         `json:"headers"`
	Method  string          `json:"method"`
	Args    interface{}     `json:"args"`
}

// AsyncTaskResult represents a task result
type AsyncTaskResult interface {
	ID() TaskID
	State() (TaskStatus, error)
	Cancel() (bool, error)
	Get(ctx context.Context, value interface{}) error
}

type TaskStatus int32

const (
	TaskStatusInit TaskStatus = iota
	TaskStatusRunning
	TaskStatusFailure
	TaskStatusSucceeded
)

// TaskState represents a state of a task
type TaskState struct {
	State   TaskStatus    `json:"state"`
	Result  interface{}   `json:"result"`
	Error   *errors.Error `json:"error"`
	EndedAt time.Time     `json:"ended_at"`
}

func (state *TaskState) IsCompleted() bool {
	return state.State == TaskStatusFailure || state.State == TaskStatusSucceeded
}

type Runnable interface {
	Run(ctx context.Context) (interface{}, error)
}

type TaskFactory interface {
	Create(task *Task) (Runnable, error)
}

var (
	factories     = map[string]TaskFactory{}
	factoriesLock sync.Mutex
)

func RegisterTaskFactory(method string, factory TaskFactory) {
	factoriesLock.Lock()
	defer factoriesLock.Unlock()
	if old := factories[method]; old != nil {
		panic(errors.New("method '" + method + "' is already exists"))
	}
	factories[method] = factory
}

func ToRunnable(task *Task) (Runnable, error) {
	factoriesLock.Lock()
	factory := factories[task.Method]
	factoriesLock.Unlock()

	if factory != nil {
		return nil, errors.New("method '" + task.Method + "' of '" + task.Title + "' isnot exists")
	}
	return factory.Create(task)
}

func IsRegistered(method string) bool {
	factoriesLock.Lock()
	factory := factories[method]
	factoriesLock.Unlock()
	return factory != nil
}

type RunnableFunc func(task *Task) (Runnable, error)

func (cb RunnableFunc) Creat(task *Task) (Runnable, error) {
	return cb(task)
}

type RunnableTask struct {
	task     *Task
	runnable Runnable
}

func StartSpanFromTask(task *Task) opentracing.Span {
	// Try to extract the span context from the carrier.
	spanContext, err := opentracing.GlobalTracer().Extract(opentracing.TextMap, task.Headers.TraceContext)
	var span opentracing.Span
	if spanContext == nil {
		span = opentracing.StartSpan(
			task.Name,
			opentracing_ext.SpanKindConsumer,
			TaskQueueTag,
		)
	} else {
		span = opentracing.StartSpan(
			task.Name,
			opentracing.FollowsFrom(spanContext),
			opentracing_ext.SpanKindConsumer,
			TaskQueueTag,
		)
	}

	span.SetTag("task.id", task.ID)
	span.SetTag("task.name", task.Name)
	span.SetTag("task.method", task.Method)

	// Log any error but don't fail
	if err != nil {
		span.LogFields(opentracing_log.Error(err))
	}
	return span
}
