// Package workflow defines domain types for the local workflow scheduler engine:
// workflow definitions, runs, task runs, trigger sources, states and reason codes.
package workflow

import "time"

// TriggerSource identifies how a workflow run is triggered.
type TriggerSource string

const (
	TriggerManual TriggerSource = "manual"
	TriggerTimer  TriggerSource = "timer"
	TriggerEvent  TriggerSource = "event"
)

func (s TriggerSource) String() string {
	switch s {
	case TriggerManual, TriggerTimer, TriggerEvent:
		return string(s)
	default:
		return "unknown:" + string(s)
	}
}

// ConcurrencyPolicy defines what happens when a trigger arrives while the
// workflow already has an active run.
type ConcurrencyPolicy string

const (
	// ConcurrencySkip rejects the new trigger; the existing run keeps running.
	ConcurrencySkip ConcurrencyPolicy = "skip"
	// ConcurrencyQueue serialises triggers: the queued trigger starts a new run
	// only after the active run reaches a terminal state.
	ConcurrencyQueue ConcurrencyPolicy = "queue"
)

// TaskState is the lifecycle state of a single task execution.
type TaskState string

const (
	TaskPending   TaskState = "pending"
	TaskWaiting   TaskState = "waiting" // sleeping between retry attempts
	TaskRunning   TaskState = "running"
	TaskSucceeded TaskState = "succeeded"
	TaskFailed    TaskState = "failed"
	TaskCanceled  TaskState = "canceled"
	TaskSkipped   TaskState = "skipped" // downstream of a failed task
)

// RunState is the lifecycle state of a workflow run.
type RunState string

const (
	RunQueued    RunState = "queued"
	RunRunning   RunState = "running"
	RunSucceeded RunState = "succeeded"
	RunFailed    RunState = "failed"
	RunCanceled  RunState = "canceled"
	RunTimedOut  RunState = "timedout"
)

// Reason codes distinguish failure / rejection causes.
type Reason string

const (
	ReasonNone                 Reason = ""
	ReasonDAGCycle             Reason = "dag_cycle"
	ReasonDAGMissingDep        Reason = "dag_missing_dependency"
	ReasonTaskFailed           Reason = "task_failed"
	ReasonMaxRetries           Reason = "task_max_retries_exceeded"
	ReasonCanceled             Reason = "canceled"
	ReasonTimeout              Reason = "timeout"
	ReasonAlreadyRunning       Reason = "already_running_skipped"
	ReasonTaskPanic            Reason = "task_panic"
	ReasonRecoveredInterrupted Reason = "recovered_interrupted_execution"
)

// IsTerminal reports whether a run state is terminal (no further transitions).
func IsTerminal(s RunState) bool {
	switch s {
	case RunSucceeded, RunFailed, RunCanceled, RunTimedOut:
		return true
	}
	return false
}

// TaskDef declares one task within a workflow definition.
type TaskDef struct {
	ID           string   `json:"id"`
	Name         string   `json:"name,omitempty"`
	Deps         []string `json:"deps,omitempty"`
	MaxAttempts  int      `json:"max_attempts,omitempty"` // total attempts; <=1 means no retry
	BackoffMilli int      `json:"backoff_milli,omitempty"`
	TimeoutMilli int      `json:"timeout_milli,omitempty"` // per-task execution timeout, 0 = inherit run timeout
}

// Definition is an immutable workflow definition.
type Definition struct {
	ID    string    `json:"id"`
	Name  string    `json:"name,omitempty"`
	Tasks []TaskDef `json:"tasks"`
	// Concurrency policy applied by the engine for this workflow.
	Concurrency ConcurrencyPolicy `json:"concurrency,omitempty"`
	// RunTimeoutMilli bounds the whole run; 0 means no timeout.
	RunTimeoutMilli int `json:"run_timeout_milli,omitempty"`
}

// TaskRun records the state of one task instance inside a run.
type TaskRun struct {
	TaskID     string     `json:"task_id"`
	State      TaskState  `json:"state"`
	Attempts   int        `json:"attempts"` // number of started executions; never decreases
	LastErr    string     `json:"last_err,omitempty"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	UpdatedAt  time.Time  `json:"updated_at"`
}

// Run is one execution of a workflow definition.
type Run struct {
	ID         string        `json:"id"`
	WorkflowID string        `json:"workflow_id"`
	Source     TriggerSource `json:"source"`
	State      RunState      `json:"state"`
	Reason     Reason        `json:"reason,omitempty"`
	Message    string        `json:"message,omitempty"`
	Tasks      []TaskRun     `json:"tasks"`
	CreatedAt  time.Time     `json:"created_at"`
	UpdatedAt  time.Time     `json:"updated_at"`
	StartedAt  *time.Time    `json:"started_at,omitempty"`
	FinishedAt *time.Time    `json:"finished_at,omitempty"`
}

// AuditEvent is one structured trace line describing a state transition.
type AuditEvent struct {
	Time       time.Time     `json:"time"`
	WorkflowID string        `json:"workflow_id"`
	RunID      string        `json:"run_id"`
	Source     TriggerSource `json:"source"`
	TaskID     string        `json:"task_id,omitempty"`
	Event      string        `json:"event"`
	From       string        `json:"from,omitempty"`
	To         string        `json:"to,omitempty"`
	Reason     Reason        `json:"reason,omitempty"`
	Detail     string        `json:"detail,omitempty"`
}
