// Package scheduler validates workflow dependency graphs and computes
// task readiness during a run.
package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/h3yow/workflow-scheduler-engine-go/internal/workflow"
)

// ValidationError describes a rejected workflow definition.
type ValidationError struct {
	Reason  workflow.Reason
	Message string
}

func (e *ValidationError) Error() string { return string(e.Reason) + ": " + e.Message }

// Validate checks a definition for duplicate task ids, missing dependencies
// and dependency cycles. It returns a *ValidationError with a distinguishable
// Reason for each rejection cause.
func Validate(def workflow.Definition) error {
	ids := make(map[string]struct{}, len(def.Tasks))
	for _, t := range def.Tasks {
		if t.ID == "" {
			return &ValidationError{Reason: workflow.ReasonDAGMissingDep, Message: "task with empty id"}
		}
		if _, dup := ids[t.ID]; dup {
			return &ValidationError{Reason: workflow.ReasonDAGCycle, Message: fmt.Sprintf("duplicate task id %q", t.ID)}
		}
		ids[t.ID] = struct{}{}
	}
	for _, t := range def.Tasks {
		seen := make(map[string]struct{}, len(t.Deps))
		for _, dep := range t.Deps {
			if dep == t.ID {
				return &ValidationError{Reason: workflow.ReasonDAGCycle, Message: fmt.Sprintf("task %q depends on itself", t.ID)}
			}
			if _, ok := ids[dep]; !ok {
				return &ValidationError{
					Reason:  workflow.ReasonDAGMissingDep,
					Message: fmt.Sprintf("task %q depends on unknown task %q", t.ID, dep),
				}
			}
			if _, dup := seen[dep]; dup {
				return &ValidationError{Reason: workflow.ReasonDAGCycle, Message: fmt.Sprintf("task %q lists duplicate dependency %q", t.ID, dep)}
			}
			seen[dep] = struct{}{}
		}
	}

	// Cycle detection via Kahn's algorithm.
	indeg := make(map[string]int, len(def.Tasks))
	dependents := make(map[string][]string, len(def.Tasks))
	for _, t := range def.Tasks {
		indeg[t.ID] = len(t.Deps)
		for _, dep := range t.Deps {
			dependents[dep] = append(dependents[dep], t.ID)
		}
	}
	queue := make([]string, 0, len(def.Tasks))
	for id, d := range indeg {
		if d == 0 {
			queue = append(queue, id)
		}
	}
	processed := 0
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		processed++
		for _, dep := range dependents[cur] {
			indeg[dep]--
			if indeg[dep] == 0 {
				queue = append(queue, dep)
			}
		}
	}
	if processed < len(def.Tasks) {
		cyclic := make([]string, 0)
		for id, d := range indeg {
			if d > 0 {
				cyclic = append(cyclic, id)
			}
		}
		return &ValidationError{
			Reason:  workflow.ReasonDAGCycle,
			Message: fmt.Sprintf("dependency cycle detected among tasks %v", cyclic),
		}
	}
	return nil
}

// Scheduler tracks per-run task progress and reports which tasks are ready
// (all dependencies already succeeded).
type Scheduler struct {
	def   workflow.Definition
	order []string
	tasks map[string]*workflow.TaskRun
}

func newPending(id string) *workflow.TaskRun {
	now := time.Now()
	return &workflow.TaskRun{TaskID: id, State: workflow.TaskPending, UpdatedAt: now}
}

// New builds a Scheduler with all tasks initialised to pending.
func New(def workflow.Definition) (*Scheduler, error) {
	if err := Validate(def); err != nil {
		return nil, err
	}
	s := &Scheduler{def: def, tasks: make(map[string]*workflow.TaskRun, len(def.Tasks))}
	for _, t := range def.Tasks {
		s.order = append(s.order, t.ID)
		cp := t
		s.tasks[t.ID] = newPending(cp.ID)
	}
	return s, nil
}

// Restore builds a Scheduler from persisted task states (recovery path).
func Restore(def workflow.Definition, tasks []workflow.TaskRun) (*Scheduler, error) {
	if err := Validate(def); err != nil {
		return nil, err
	}
	s := &Scheduler{def: def, tasks: make(map[string]*workflow.TaskRun, len(def.Tasks))}
	for _, t := range def.Tasks {
		s.order = append(s.order, t.ID)
		s.tasks[t.ID] = newPending(t.ID)
	}
	for i := range tasks {
		if tr, ok := s.tasks[tasks[i].TaskID]; ok {
			cp := tasks[i]
			*tr = cp
		}
	}
	return s, nil
}

// Ready returns task ids whose dependencies are all succeeded and which are
// still pending, in definition order.
func (s *Scheduler) Ready(ctx context.Context) []string {
	ready := make([]string, 0)
	for _, id := range s.order {
		if ctx.Err() != nil {
			return ready
		}
		tr := s.tasks[id]
		if tr.State != workflow.TaskPending {
			continue
		}
		allDepsSucceeded := true
		for _, t := range s.def.Tasks {
			if t.ID != id {
				continue
			}
			for _, dep := range t.Deps {
				if s.tasks[dep].State != workflow.TaskSucceeded {
					allDepsSucceeded = false
					break
				}
			}
			break
		}
		if allDepsSucceeded {
			ready = append(ready, id)
		}
	}
	return ready
}

// Task reads the current state of one task.
func (s *Scheduler) Task(id string) (workflow.TaskRun, bool) {
	tr, ok := s.tasks[id]
	if !ok {
		return workflow.TaskRun{}, false
	}
	return *tr, true
}

// Snapshot returns a copy of all task states, in definition order.
func (s *Scheduler) Snapshot() []workflow.TaskRun {
	out := make([]workflow.TaskRun, 0, len(s.order))
	for _, id := range s.order {
		out = append(out, *s.tasks[id])
	}
	return out
}

func isTerminalTask(st workflow.TaskState) bool {
	switch st {
	case workflow.TaskSucceeded, workflow.TaskFailed, workflow.TaskCanceled, workflow.TaskSkipped:
		return true
	}
	return false
}

// Mark updates one task's state. States of terminal tasks never regress:
// applying another state to a terminal task returns false.
func (s *Scheduler) Mark(id string, st workflow.TaskState, errMsg string) (workflow.TaskRun, bool) {
	tr, ok := s.tasks[id]
	if !ok {
		return workflow.TaskRun{}, false
	}
	if isTerminalTask(tr.State) {
		return *tr, false
	}
	now := time.Now()
	tr.State = st
	if errMsg != "" {
		tr.LastErr = errMsg
	}
	if isTerminalTask(st) {
		tr.FinishedAt = &now
	}
	tr.UpdatedAt = now
	return *tr, true
}

// IncrementAttempt records that an execution has started; the counter never
// decreases (covers retries and crash-interrupted executions).
func (s *Scheduler) IncrementAttempt(id string) (workflow.TaskRun, bool) {
	tr, ok := s.tasks[id]
	if !ok {
		return workflow.TaskRun{}, false
	}
	tr.Attempts++
	now := time.Now()
	if tr.StartedAt == nil {
		tr.StartedAt = &now
	}
	tr.UpdatedAt = now
	return *tr, true
}

// Def returns the workflow definition this scheduler runs.
func (s *Scheduler) Def() workflow.Definition { return s.def }

// Order returns task ids in definition order.
func (s *Scheduler) Order() []string { return append([]string(nil), s.order...) }

// MarkRetry moves a failed/waiting/running task back to pending for another
// attempt. Attempts are untouched (they never decrease). Used for normal
// retries and crash recovery.
func (s *Scheduler) MarkRetry(id string) (workflow.TaskRun, bool) {
	tr, ok := s.tasks[id]
	if !ok {
		return workflow.TaskRun{}, false
	}
	tr.State = workflow.TaskPending
	tr.UpdatedAt = time.Now()
	return *tr, true
}

// MarkCanceled marks a waiting task canceled (only used when cancellation
// lands during retry backoff).
func (s *Scheduler) MarkCanceled(id string, msg string) (workflow.TaskRun, bool) {
	return s.Mark(id, workflow.TaskCanceled, msg)
}

// MarkWaiting moves a running (just-failed, retryable) task to the waiting
// state and records the latest error message.
func (s *Scheduler) MarkWaiting(id string, msg string) (workflow.TaskRun, bool) {
	tr, ok := s.tasks[id]
	if !ok || isTerminalTask(tr.State) {
		return workflow.TaskRun{}, false
	}
	tr.State = workflow.TaskWaiting
	tr.LastErr = msg
	tr.UpdatedAt = time.Now()
	return *tr, true
}

// TransitiveDependents returns all tasks that (transitively) depend on id,
// in definition order. Used to skip downstream tasks after a hard failure.
func (s *Scheduler) TransitiveDependents(id string) []string {
	dependents := make(map[string][]string)
	for _, t := range s.def.Tasks {
		for _, dep := range t.Deps {
			dependents[dep] = append(dependents[dep], t.ID)
		}
	}
	visited := make(map[string]struct{})
	stack := append([]string(nil), dependents[id]...)
	for len(stack) > 0 {
		cur := stack[0]
		stack = stack[1:]
		if _, seen := visited[cur]; seen {
			continue
		}
		visited[cur] = struct{}{}
		stack = append(stack, dependents[cur]...)
	}
	out := make([]string, 0, len(visited))
	for _, tid := range s.order {
		if _, ok := visited[tid]; ok {
			out = append(out, tid)
		}
	}
	return out
}

// RunFinished reports whether no further task can make progress, and the
// terminal run state/reason the run should end with. The engine overrides
// the reason for more specific failure/cancel/timeout codes.
func (s *Scheduler) RunFinished(ctx context.Context) (bool, workflow.RunState, workflow.Reason) {
	hasFailed, hasSkipped, hasCanceled := false, false, false
	allTerminal := true
	for _, id := range s.order {
		switch s.tasks[id].State {
		case workflow.TaskFailed:
			hasFailed = true
		case workflow.TaskSkipped:
			hasSkipped = true
		case workflow.TaskCanceled:
			hasCanceled = true
		case workflow.TaskPending, workflow.TaskWaiting, workflow.TaskRunning:
			allTerminal = false
		}
	}
	switch {
	case hasFailed:
		return true, workflow.RunFailed, workflow.ReasonTaskFailed
	case hasCanceled:
		return true, workflow.RunCanceled, workflow.ReasonCanceled
	case hasSkipped:
		// Skipped tasks without a failed task occur only on cancellation.
		if ctx.Err() != nil {
			return true, workflow.RunCanceled, workflow.ReasonCanceled
		}
		return true, workflow.RunFailed, workflow.ReasonTaskFailed
	case allTerminal:
		return true, workflow.RunSucceeded, workflow.ReasonNone
	default:
		return false, "", ""
	}
}
