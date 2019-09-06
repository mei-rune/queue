package queue

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/runner-mei/log"
)

type BackgroundTask struct {
	TableName struct{}    `json:"-" xorm:"tpt_background_queues"`
	ID        int64       `json:"id" xorm:"id autoincr pk"`
	Title     string      `json:"title" xorm:"title notnull unique"`
	RouteKey  string      `json:"route_key" xorm:"route_key"`
	Type      string      `json:"type" xorm:"type notnull"`
	Args      interface{} `json:"args" xorm:"args jsonb"`

	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at"`
}

type TaskSummary struct {
	TableName struct{}  `json:"-" xorm:"tpt_background_queues"`
	ID        TaskID    `json:"id" xorm:"id autoincr pk"`
	Title     string    `json:"title" xorm:"title notnull unique"`
	RouteKey  string    `json:"route_key" xorm:"route_key"`
	Type      string    `json:"type" xorm:"type notnull"`
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at"`
}

// Backend - a common interface for all result backends
type Backend interface {
	Create(ctx context.Context, task *Task) (TaskID, error)
	Delete(ctx context.Context, id TaskID)
	Fetch(ctx context.Context, workerID WorkerID, consumerTag string) (*Task, error)
	SetStateAccepted(ctx context.Context, id TaskID) error
	SetStateSuccess(ctx context.Context, id TaskID, result interface{}) error
	SetStateFailure(ctx context.Context, id TaskID, failureMessage string) error
	GetState(ctx context.Context, id TaskID) (*TaskState, error)
	GetResult(ctx context.Context, id TaskID) (interface{}, error)

	GetSummaries(queue string) ([]TaskSummary, error)
}

type Pubsub interface {
	Pub(ctx context.Context, taskID TaskID) error
	Sub(ctx context.Context, cb func(taskID TaskID)) error
}

type Settings interface {
	GetConfig(ctx context.Context, workerID WorkerID) (*WorkerConfig, error)
	SetConfig(ctx context.Context, workerID WorkerID, cfg *WorkerConfig) error
}

type Server struct {
	logger   log.Logger
	settings Settings

	taskQueue   Pubsub
	resultQueue Pubsub

	backend Backend
	waits   sync.Map

	workersLock sync.Mutex
	workers     list.List
}

func (srv *Server) GetConfig(ctx context.Context, workerID WorkerID) (*WorkerConfig, error) {
	return srv.settings.GetConfig(ctx, workerID)
	// var cfg WorkerConfig
	// err := ds_client.ReadSettings(ctx, srv.settings, "taskqueue.", &cfg)
	// return &cfg, err
}

func (srv *Server) SetConfig(ctx context.Context, workerID WorkerID, cfg *WorkerConfig) error {
	return srv.settings.SetConfig(ctx, workerID, cfg)
	// return ds_client.SaveSettings(ctx, srv.settings, "taskqueue.", cfg)
}

func (srv *Server) Send(ctx context.Context, task *Task) (TaskID, error) {
	id, err := srv.backend.Create(ctx, task)
	if err != nil {
		return 0, err
	}
	if err := srv.taskQueue.Pub(ctx, id); err != nil {
		return 0, err
	}
	return id, nil
}

func (srv *Server) SendAsync(ctx context.Context, task *Task) (AsyncTaskResult, error) {
	id, err := srv.Send(ctx, task)
	if err != nil {
		return 0, err
	}

	result := &asyncResult{taskID: id, srv: srv, c: make(chan *asyncResult, 1)}
	srv.waits.Store(id, result)
	return result, nil
}

func (srv *Server) cancel(id TaskID) error {
	err := srv.backend.Delete(nil, id)
	if err != nil {
		return err
	}
	srv.waits.Delete(id)
	return nil
}

func (srv *Server) Report(ctx context.Context, event *ReportEvent) error {
	switch event.Type {
	case ReportAccepted:
		return srv.backend.SetStateAccepted(event.TaskID)
	case ReportSucceeded:
		return srv.backend.SetStateSuccess(event.TaskID, event.Data)
	case ReportFailure:
		return srv.backend.SetStateFailure(event.TaskID, fmt.Sprint(event.Data))
	case ReportHeartbeat:
	case ReportLogRecord:
	default:
		srv.logger.Warn("unknow event type", log.Any("event", event))
	}
}

type subClient struct {
	element     *list.Element
	workerID    WorkerID
	consumerTag string
	c           chan TaskID
}

func (srv *Server) OnTask(taskID TaskID) {
	srv.workersLock.Lock()
	defer srv.workersLock.Unlock()

	for it := srv.workers.Front(); it != nil; it = it.Next() {
		sc := it.Value.(*subClient)
		sc.c <- taskID
	}
}

func (srv *Server) OnResult(taskID TaskID) {

	o := srv.waits.Load(taskID)
	if o == nil {
		return
	}

}

func (srv *Server) Fetch(ctx context.Context, workerID WorkerID, consumerTag string) (*Task, error) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	rc := &subClient{
		workerID:    workerID,
		consumerTag: consumerTag,
		c:           make(chan TaskID, 1),
	}

	srv.workersLock.Lock()
	rc.element = srv.workers.InsertAfter(rc)
	srv.workersLock.Unlock()

	defer func() {
		srv.workersLock.Lock()
		srv.workers.Remove(rc.element)
		rc.element = nil
		srv.workersLock.Unlock()
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-rc.c:
		fallthrough
	case <-ticker.C:
		task, err := srv.backend.Fetch(ctx, rc.workerID, rc.consumerTag)
		if err != nil {
			return nil, err
		}
		if task != nil {
			return task, nil
		}
	}
}
