package workflow

// Store is the local persistence boundary. All implementations must guarantee
// that CompareAndSwapTask / CompareAndSwapRun are atomic: concurrent
// schedulers cannot make the same task run twice or roll a state backwards.
//
// State transitions are always conditional on the expected previous state
// (compare-and-swap), which is the core mechanism enforcing:
//   - no duplicate task execution (PENDING/RUNNING -> RUNNING wins once),
//   - no state rollback on retry/recovery,
//   - single-winner concurrent triggers.
type Store interface {
	// definitions
	PutWorkflow(def WorkflowDef) error
	GetWorkflow(name string) (WorkflowDef, error)
	ListWorkflows() ([]WorkflowDef, error)

	// runs
	CreateRun(r Run) error
	GetRun(id RunID) (Run, error)
	// CompareAndSwapRun updates a run only if its stored state == expect.
	// mutate runs inside the store lock; return false from mutate to abort.
	CompareAndSwapRun(id RunID, expect RunState, mutate func(r *Run)) (Run, bool, error)
	// ActiveRun returns the single non-terminal run for dedupKey, if any.
	ActiveRun(dedupKey string) (Run, bool, error)
	ListRuns(workflow string) ([]Run, error)

	// tasks
	CreateTask(t TaskRun) error
	GetTask(id TaskRunID) (TaskRun, error)
	// CompareAndSwapTask updates a task only if its stored state == expect.
	CompareAndSwapTask(id TaskRunID, expect TaskState, mutate func(t *TaskRun)) (TaskRun, bool, error)
	ListTasks(runID RunID) ([]TaskRun, error)
	// GetTaskByRef fetches the task record for a (run, taskID) pair.
	GetTaskByRef(runID RunID, taskID string) (TaskRun, error)

	// recovery
	ListRecoverableRuns() ([]Run, error)

	// events
	EventLog() EventLog

	Close() error
}
