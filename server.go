




import (
	"context"
	"time"

	"github.com/runner-mei/log"
	"github.com/three-plus-three/modules/as"
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
	// Setting / getting task state
	Create(task *Task) (TaskID, error)
	SetStateAccepted(id TaskID) error
	SetStateSuccess(id TaskID, result interface{}) error
	SetStateFailure(id TaskID, failureMessage string) error
	GetState(id TaskID) (TaskStatus, error)
	GetResult(id TaskID) (interface{}, error)

	GetSummaries(queue string) ([]TaskSummary, error)
}

type Queue interface {
	Push(ctx context.Context, task *Task) error
	Pop(ctx context.Context, consumerTag string) (*Task, error)
}

type Server struct {
	logger   log.Logger
	settings ds_client.SettingsImpl

	queue   Queue
	backend Backend
}

func (srv *Server) GetConfig(ctx context.Context, workerID string) (*WorkerConfig, error) {
	var cfg WorkerConfig
	err := ds_client.ReadSettings(ctx, srv.settings, "taskqueue.", &cfg)
	return &cfg, err
}

func (srv *Server) SetConfig(ctx context.Context, workerID string, cfg *WorkerConfig) error {
	return ds_client.SaveSettings(ctx, srv.settings, "taskqueue.", cfg)
}

func (srv *Server) Send(ctx context.Context, task *Task) (AsyncTaskResult, error) {
	id, err := srv.backend.Create(ctx, task)
	if err != nil {
		return nil, err
	}
	task.ID = id
	if err := srv.queue.Push(task); err != nil {
		return nil, err
	}

	return &asyncResult{task: task, srv: srv}, nil
}

func (srv *Server) Report(ctx context.Context, event *ReportEvent) error {
	switch event.Type {
	case ReportAccepted:
		return srv.backend.SetStateAccepted(event.TaskID)
	case ReportSucceeded:
		return srv.backend.SetStateSuccess(event.TaskID, event.Data)
	case ReportFailure:
		return srv.backend.SetStateFailure(event.TaskID, as.StringWithDefault(event.Data, ""))
	case ReportHeartbeat:
	case ReportLogRecord:
	default:
		srv.logger.Warn("unknow event type", log.Any("event", event))
	}
}

func (srv *Server) Fetch(ctx context.Context, consumerTag string) (*Task, error) {
	return srv.queue.Pop(ctx, consumerTag)
}


