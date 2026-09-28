package workflow

import "errors"

// ErrNotFound is returned by stores when a requested entity does not exist.
var ErrNotFound = errors.New("not found")

// ErrRunActive is returned (wrapped) when a reject-policy trigger collides
// with an existing non-terminal run.
var ErrRunActive = errors.New("run already active")

// ErrUnknownWorkflow is returned when triggering/registering a missing name.
var ErrUnknownWorkflow = errors.New("unknown workflow")

// Reason codes distinguish rejection / failure causes in run records and tests.
const (
	ReasonCycleDetected     = "CYCLE_DETECTED"
	ReasonMissingDependency = "MISSING_DEPENDENCY"
	ReasonDuplicateTask     = "DUPLICATE_TASK"
	ReasonEmptyWorkflow     = "EMPTY_WORKFLOW"
	ReasonUnknownWorkflow   = "UNKNOWN_WORKFLOW"
	ReasonNoHandler         = "NO_HANDLER"
	ReasonRetryExhausted    = "RETRY_EXHAUSTED"
	ReasonUpstreamFailed    = "UPSTREAM_FAILED"
	ReasonCanceled          = "CANCELED"
	ReasonTimeout           = "TIMEOUT"
	ReasonRunInterrupted    = "RUN_INTERRUPTED"
	ReasonAlreadyTriggered  = "ALREADY_TRIGGERED"
	ReasonInvalidDefinition = "INVALID_DEFINITION"
)

// ValidationError marks a workflow definition rejected before scheduling.
type ValidationError struct {
	Reason string
	Msg    string
}

func (e *ValidationError) Error() string { return e.Reason + ": " + e.Msg }

// IsValidation reports whether err is a definition-time validation error.
func IsValidation(err error) bool {
	var ve *ValidationError
	return errors.As(err, &ve)
}

// TaskError is a structured error returned by task handlers so that failure
// reasons are distinguishable in the task record.
type TaskError struct {
	Code string
	Msg  string
}

func (e *TaskError) Error() string { return e.Code + ": " + e.Msg }

// NewTaskError builds a TaskError with a stable machine-readable code.
func NewTaskError(code, msg string) *TaskError {
	return &TaskError{Code: code, Msg: msg}
}
