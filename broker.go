package queue

import "context"

// ReportType a report message type
type ReportType int

const (
	ReportAccepted ReportType = iota
	ReportHeartbeat
	ReportLogRecord
	ReportSucceeded
	ReportFailure
)

const (
	MsgAccepted  = "accepted"
	MsgHeartbeat = "heartbeat"
	MsgSucceeded = "succeeded"
)

// ReportEvent a report message playload
type ReportEvent struct {
	WorkerID WorkerID    `json:"worker_id"`
	TaskID   TaskID      `json:"task_id"`
	Type     ReportType  `json:"status"`
	Data     interface{} `json:"data"`
}

type WorkerConfig struct{}

type Broker interface {
	// @http.GET(path="/settings/:workerID")
	GetConfig(ctx context.Context, workerID string) (*WorkerConfig, error)
	// @http.PUT(path="/settings/:workerID", data="cfg")
	SetConfig(ctx context.Context, workerID string, cfg *WorkerConfig) error

	// @http.POST(path="", data="task")
	Send(ctx context.Context, task *Task) (AsyncTaskResult, error)

	// @http.POST(path="/report", data="event")
	Report(ctx context.Context, event *ReportEvent) error
	// @http.GET(path="")
	Fetch(ctx context.Context, workerID string) (*Task, error)
}
