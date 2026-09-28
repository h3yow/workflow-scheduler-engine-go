package workflow

import "time"

// TaskState is the state of a single task execution within a run.
type TaskState string

const (
	TaskPending   TaskState = "PENDING"   // not yet scheduled
	TaskReady     TaskState = "READY"     // dependencies satisfied, waiting for a worker
	TaskRunning   TaskState = "RUNNING"   // a handler invocation is in progress
	TaskSucceeded TaskState = "SUCCEEDED" // terminal: handler returned success
	TaskFailed    TaskState = "FAILED"    // terminal: retries exhausted
	TaskSkipped   TaskState = "SKIPPED"   // terminal: an upstream dependency failed
	TaskCanceled  TaskState = "CANCELED"  // terminal: run was canceled before completion
)

// IsTerminal reports whether the task state is terminal.
func (s TaskState) IsTerminal() bool {
	switch s {
	case TaskSucceeded, TaskFailed, TaskSkipped, TaskCanceled:
		return true
	}
	return false
}

// RunState is the state of a whole workflow run.
type RunState string

const (
	RunPending     RunState = "PENDING"   // accepted, scheduler not yet started
	RunRunning     RunState = "RUNNING"   // at least one task is active or pending
	RunSucceeded   RunState = "SUCCEEDED" // terminal: all tasks succeeded
	RunFailed      RunState = "FAILED"    // terminal: at least one task failed
	RunCanceled    RunState = "CANCELED"  // terminal: canceled by client or shutdown
	RunInterrupted RunState = "INTERRUPTED"
	// terminal: process restarted under RecoveryTerminate
)

// IsTerminal reports whether the run state is terminal.
func (s RunState) IsTerminal() bool {
	switch s {
	case RunSucceeded, RunFailed, RunCanceled, RunInterrupted:
		return true
	}
	return false
}

// TriggerSource identifies how a run was started.
type TriggerSource string

const (
	TriggerManual   TriggerSource = "manual"
	TriggerSchedule TriggerSource = "schedule"
	TriggerEvent    TriggerSource = "event"
)

// ConcurrencyPolicy controls what happens when a trigger arrives while a
// non-terminal run of the same workflow is active.
type ConcurrencyPolicy string

const (
	// ConcurrencyReject rejects the new trigger; the existing run continues.
	ConcurrencyReject ConcurrencyPolicy = "reject"
	// ConcurrencyQueue waits for the active run to finish, then starts a new one.
	ConcurrencyQueue ConcurrencyPolicy = "queue"
	// ConcurrencyCancel cancels the active run and starts a fresh one.
	ConcurrencyCancel ConcurrencyPolicy = "cancel"
)

// RetryPolicy configures per-task failure retries.
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts (1 means no retry).
	MaxAttempts int
	// InitialBackoff is the delay before attempt #2.
	InitialBackoff time.Duration
	// MaxBackoff caps the (exponentially growing) backoff.
	MaxBackoff time.Duration
}

func (p RetryPolicy) attempts() int {
	if p.MaxAttempts < 1 {
		return 1
	}
	return p.MaxAttempts
}

// TaskDef is a task declared in a workflow definition.
type TaskDef struct {
	ID           string
	Dependencies []string
	Retries      RetryPolicy
	// Timeout bounds a single attempt; zero means no timeout.
	Timeout time.Duration
}

// WorkflowDef is an immutable workflow definition (a DAG of tasks).
type WorkflowDef struct {
	Name  string
	Tasks []TaskDef
	// TriggerConcurrency controls overlapping runs of this workflow.
	TriggerConcurrency ConcurrencyPolicy
	// Timeout bounds the whole run; zero means no run-level timeout.
	Timeout time.Duration
}

// RunID / TaskRunID opaque identifiers.
type (
	RunID     string
	TaskRunID string
)

// TaskRun is the persisted state of one task within one run.
type TaskRun struct {
	ID        TaskRunID
	RunID     RunID
	TaskID    string
	Attempt   int // number of attempts already started (never decreases)
	State     TaskState
	Output    string
	ErrCode   string
	ErrMsg    string
	StartedAt time.Time
	UpdatedAt time.Time
	EndedAt   *time.Time
}

// Run is the persisted state of one workflow execution.
type Run struct {
	ID        RunID
	Workflow  string
	Trigger   TriggerSource
	State     RunState
	Reason    string // machine-readable terminal reason code
	Message   string // human-readable detail
	CreatedAt time.Time
	UpdatedAt time.Time
	EndedAt   *time.Time
	// Dedup key for concurrent triggers (workflow name for policy-based dedup).
	DedupKey string
}

// Event is an append-only, persisted state-transition record.
type Event struct {
	Seq       int64
	Time      time.Time
	RunID     RunID
	TaskID    string // empty for run-level events
	FromState string
	ToState   string
	Trigger   TriggerSource
	Reason    string
	Message   string
}

// EventLog is a reader over persisted events.
type EventLog interface {
	Append(ev Event) error
	List(runID RunID) ([]Event, error)
	All() ([]Event, error)
	LastSeq() (int64, error)
	Close() error
}
