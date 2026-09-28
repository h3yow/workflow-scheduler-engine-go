// Package engine orchestrates workflow runs: trigger admission with
// concurrency control, dependency-ordered task execution, failure retries,
// cancellation/timeout and restart recovery.
package engine

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/h3yow/workflow-scheduler-engine-go/internal/scheduler"
	"github.com/h3yow/workflow-scheduler-engine-go/internal/store"
	"github.com/h3yow/workflow-scheduler-engine-go/internal/workflow"
)

// ExecutorFunc executes one task. attempt starts at 1. It must honor ctx
// cancellation promptly; a ctx error results in a canceled run.
type ExecutorFunc func(ctx context.Context, workflowID, taskID string, attempt int) error

// RejectError is returned when a trigger/definition is rejected before a run
// executes; Reason distinguishes the causes.
type RejectError struct {
	Reason  workflow.Reason
	Message string
}

func (e *RejectError) Error() string { return string(e.Reason) + ": " + e.Message }

// RunResult summarises a Trigger call.
type RunResult struct {
	RunID   string
	State   workflow.RunState
	Reason  workflow.Reason
	Message string
	// Skipped is true when a trigger was refused by the skip concurrency policy.
	Skipped bool
}

type waiter struct {
	result *RunResult
	err    error
	done   chan struct{}
}

type wfRuntime struct {
	activeRunID string
	queue       []string // run ids waiting for their turn
	waiters     map[string][]chan *runOutcome
	signaled    map[string]bool
	processing  bool
}

type runOutcome struct {
	result *RunResult
	err    error
}

// Engine runs workflows against a local store.
type Engine struct {
	st store.Store

	mu       sync.Mutex
	runtimes map[string]*wfRuntime
	execs    map[string]ExecutorFunc // key: workflowID/taskID

	cancelsMu sync.Mutex
	cancels   map[string]context.CancelFunc

	logger *log.Logger
	now    func() time.Time
}

// Option configures an Engine.
type Option func(*Engine)

// WithLogger sets the structured event logger (defaults to stderr).
func WithLogger(l *log.Logger) Option {
	return func(e *Engine) { e.logger = l }
}

func defaultLogger() *log.Logger { return log.Default() }

// New constructs an Engine.
func New(st store.Store, opts ...Option) *Engine {
	e := &Engine{
		st:       st,
		runtimes: map[string]*wfRuntime{},
		execs:    map[string]ExecutorFunc{},
		cancels:  map[string]context.CancelFunc{},
		logger:   defaultLogger(),
		now:      time.Now,
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

func (e *Engine) runtimeLocked(wfID string) *wfRuntime {
	rt, ok := e.runtimes[wfID]
	if !ok {
		rt = &wfRuntime{waiters: map[string][]chan *runOutcome{}, signaled: map[string]bool{}}
		e.runtimes[wfID] = rt
	}
	return rt
}

// RegisterDefinition validates and persists a workflow definition.
// Validation rejections carry Reason dag_cycle / dag_missing_dependency.
func (e *Engine) RegisterDefinition(ctx context.Context, def workflow.Definition) error {
	if err := scheduler.Validate(def); err != nil {
		return err
	}
	return e.st.SaveDefinition(ctx, def)
}

// RegisterExecutor binds a task implementation.
func (e *Engine) RegisterExecutor(workflowID, taskID string, fn ExecutorFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.execs[workflowID+"/"+taskID] = fn
}

func (e *Engine) executor(workflowID, taskID string) ExecutorFunc {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.execs[workflowID+"/"+taskID]
}

// Recover re-attaches unfinished runs after a process restart and continues
// or terminates them with well-defined semantics. Must be called once at startup.
func (e *Engine) Recover(ctx context.Context) error {
	runs, err := e.st.ListRecoverable(ctx)
	if err != nil {
		return err
	}
	// Rebuild the in-memory per-workflow queue so that at most the oldest
	// unfinished run of each workflow resumes immediately; the rest stay queued
	// and are started when their predecessor finishes.
	byWorkflow := map[string][]workflow.Run{}
	for _, r := range runs {
		byWorkflow[r.WorkflowID] = append(byWorkflow[r.WorkflowID], r)
	}
	wfIDs := make([]string, 0, len(byWorkflow))
	for id := range byWorkflow {
		wfIDs = append(wfIDs, id)
	}
	sort.Strings(wfIDs)
	for _, wfID := range wfIDs {
		rs := byWorkflow[wfID]
		sort.SliceStable(rs, func(i, j int) bool { return rs[i].CreatedAt.Before(rs[j].CreatedAt) })
		def, ok, err := e.st.GetDefinition(ctx, wfID)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("recover: definition %s missing", wfID)
		}
		e.mu.Lock()
		rt := e.runtimeLocked(wfID)
		rt.activeRunID = rs[0].ID
		rt.processing = true
		for _, qr := range rs[1:] {
			rt.queue = append(rt.queue, qr.ID)
		}
		e.mu.Unlock()
		active := rs[0]
		go e.executeRun(context.Background(), &def, &active, true)
	}
	return nil
}

// Trigger requests a run. All three trigger sources (manual/timer/event) go
// through this same path, so concurrency, timeout and cancellation behaviour
// is identical.
func (e *Engine) Trigger(ctx context.Context, workflowID string, source workflow.TriggerSource) (*RunResult, error) {
	def, ok, err := e.st.GetDefinition(ctx, workflowID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, &RejectError{Reason: workflow.ReasonDAGMissingDep, Message: "workflow not registered: " + workflowID}
	}

	runID := fmt.Sprintf("run-%d-%s", e.now().UnixNano(), source)
	r := workflow.Run{
		ID:         runID,
		WorkflowID: workflowID,
		Source:     source,
		State:      workflow.RunQueued,
		Tasks:      nil,
		CreatedAt:  e.now(),
		UpdatedAt:  e.now(),
	}

	ch := make(chan *runOutcome, 1)

	// Persist first so that every admitted trigger is traceable even if the
	// caller goes away immediately.
	if err := e.st.CreateRun(ctx, r); err != nil {
		return nil, err
	}
	e.audit(ctx, &r, "", "run.trigger", "", string(workflow.RunQueued), workflow.ReasonNone,
		fmt.Sprintf("source=%s policy=%s", source, def.Concurrency))

	e.mu.Lock()
	rt := e.runtimeLocked(workflowID)
	var skip, isActive bool
	if rt.activeRunID != "" || len(rt.queue) > 0 {
		switch def.Concurrency {
		case workflow.ConcurrencyQueue, "":
			rt.queue = append(rt.queue, runID)
			rt.waiters[runID] = append(rt.waiters[runID], ch)
		default: // skip
			skip = true
		}
	} else {
		rt.activeRunID = runID
		rt.waiters[runID] = append(rt.waiters[runID], ch)
		isActive = true
	}
	e.mu.Unlock()

	if skip {
		res := &RunResult{
			Skipped: true,
			Reason:  workflow.ReasonAlreadyRunning,
			Message: "an active run already exists for workflow " + workflowID,
		}
		e.audit(ctx, &r, "", "run.skipped", "", "", workflow.ReasonAlreadyRunning, res.Message)
		// The admitted-but-skipped run is terminated explicitly: no execution
		// is ever started for it, so no intermediate state can dangle.
		e.abortQueuedRun(ctx, workflowID, runID, workflow.ReasonAlreadyRunning, res.Message)
		return res, nil
	}

	if isActive {
		defCopy := def
		runCopy := r
		// Runs own their lifetime: a trigger caller walking away must not
		// cancel a run that other triggers may be waiting on.
		go e.executeRun(context.Background(), &defCopy, &runCopy, false)
	}

	select {
	case oc := <-ch:
		return oc.result, oc.err
	case <-ctx.Done():
		// Caller gave up waiting; detach this waiter, the run keeps running.
		e.detachWaiter(workflowID, runID, ch)
		return nil, ctx.Err()
	}
}

// detachWaiter removes one waiting channel without affecting the run.
func (e *Engine) detachWaiter(wfID, runID string, ch chan *runOutcome) {
	e.mu.Lock()
	defer e.mu.Unlock()
	rt, ok := e.runtimes[wfID]
	if !ok {
		return
	}
	ws := rt.waiters[runID]
	for i, w := range ws {
		if w == ch {
			rt.waiters[runID] = append(ws[:i], ws[i+1:]...)
			return
		}
	}
}

// abortQueuedRun removes a skip-policy-rejected run from the queue and
// records it as a terminal canceled row carrying the distinguishable
// already-running reason.
func (e *Engine) abortQueuedRun(ctx context.Context, wfID, runID string, reason workflow.Reason, msg string) {
	e.mu.Lock()
	if rt, ok := e.runtimes[wfID]; ok {
		for i, id := range rt.queue {
			if id == runID {
				rt.queue = append(rt.queue[:i], rt.queue[i+1:]...)
				break
			}
		}
		delete(rt.waiters, runID)
	}
	e.mu.Unlock()

	r, exists, _ := e.st.GetRun(ctx, runID)
	if !exists {
		return
	}
	now := e.now()
	r.State = workflow.RunCanceled
	r.Reason = reason
	r.Message = msg
	r.FinishedAt = &now
	r.UpdatedAt = now
	_ = e.st.SaveRun(ctx, r)
	e.audit(ctx, &r, "", "run.finish", string(workflow.RunQueued), string(workflow.RunCanceled), reason, msg)
}

// Cancel requests cancellation of a run. No new task starts afterward;
// already running tasks see ctx cancellation and their outcome is recorded.
func (e *Engine) Cancel(runID string) error {
	e.cancelsMu.Lock()
	cf, ok := e.cancels[runID]
	e.cancelsMu.Unlock()
	if !ok {
		return fmt.Errorf("run %s not active (already finished or unknown)", runID)
	}
	cf()
	return nil
}

// Close waits for queued processing to settle (best-effort) and closes store.
func (e *Engine) Close() error {
	// No new triggers expected; give queue processors a moment to finish.
	time.Sleep(20 * time.Millisecond)
	return e.st.Close()
}

func (e *Engine) audit(ctx context.Context, r *workflow.Run, taskID, event, from, to string, reason workflow.Reason, detail string) {
	ev := workflow.AuditEvent{
		Time:       e.now(),
		WorkflowID: r.WorkflowID,
		RunID:      r.ID,
		Source:     r.Source,
		TaskID:     taskID,
		Event:      event,
		From:       from,
		To:         to,
		Reason:     reason,
		Detail:     detail,
	}
	if err := e.st.AppendAudit(ctx, ev); err != nil {
		e.logger.Printf("audit persist error: %v", err)
	}
	e.logger.Printf("[%s] wf=%s run=%s source=%s task=%s %s %s->%s reason=%s %s",
		ev.Time.Format(time.RFC3339Nano), r.WorkflowID, r.ID, r.Source, taskID, event, from, to, reason, detail)
}

func (e *Engine) persist(ctx context.Context, r *workflow.Run, sch *scheduler.Scheduler) {
	r.Tasks = sch.Snapshot()
	r.UpdatedAt = e.now()
	if err := e.st.SaveRun(ctx, *r); err != nil {
		e.logger.Printf("persist run %s error: %v", r.ID, err)
	}
}

// executeRun drives one run to a terminal state. Exactly one goroutine ever
// runs executeRun for a given run id, so each task is launched at most once
// per attempt and attempts never double-count.
func (e *Engine) executeRun(parentCtx context.Context, def *workflow.Definition, r *workflow.Run, recovered bool) {
	ctx, cancel := context.WithCancel(parentCtx)
	if def.RunTimeoutMilli > 0 {
		ctx, cancel = context.WithTimeout(ctx, time.Duration(def.RunTimeoutMilli)*time.Millisecond)
	}
	e.cancelsMu.Lock()
	e.cancels[r.ID] = cancel
	e.cancelsMu.Unlock()
	defer func() {
		cancel()
		e.cancelsMu.Lock()
		delete(e.cancels, r.ID)
		e.cancelsMu.Unlock()
		e.finishRun(parentCtx, def, r)
	}()

	// Load current persisted run state.
	if stored, ok, _ := e.st.GetRun(ctx, r.ID); ok {
		*r = stored
	}

	var sch *scheduler.Scheduler
	var err error
	if recovered || len(r.Tasks) > 0 {
		sch, err = scheduler.Restore(*def, r.Tasks)
	} else {
		sch, err = scheduler.New(*def)
	}
	if err != nil {
		r.State = workflow.RunFailed
		r.Reason = workflow.ReasonDAGCycle
		r.Message = err.Error()
		now := e.now()
		r.FinishedAt = &now
		e.audit(ctx, r, "", "run.failed", "", string(workflow.RunFailed), workflow.ReasonDAGCycle, err.Error())
		e.persist(ctx, r, sch)
		return
	}
	r.Tasks = sch.Snapshot()

	if r.State == workflow.RunQueued {
		r.State = workflow.RunRunning
		now := e.now()
		r.StartedAt = &now
		e.audit(ctx, r, "", "run.start", string(workflow.RunQueued), string(workflow.RunRunning), workflow.ReasonNone, "")
	} else {
		e.audit(ctx, r, "", "run.recover", "", string(r.State), workflow.ReasonRecoveredInterrupted, "resuming after restart")
	}
	// Crash window: a task persisted as running never completed its run
	// function; count that interrupted execution as one attempt and either
	// retry it or fail it — no intermediate state is left dangling.
	if recovered {
		e.settleInterrupted(ctx, def, r, sch)
	}
	e.persist(ctx, r, sch)

	for {
		if ctx.Err() != nil {
			e.cancelRemaining(ctx, def, r, sch)
			return
		}
		ready := sch.Ready(ctx)
		if len(ready) == 0 {
			done, st, reason := sch.RunFinished(ctx)
			if done {
				r.State = st
				r.Reason = reason
				switch st {
				case workflow.RunSucceeded:
					r.Message = "all tasks succeeded"
				case workflow.RunFailed:
					r.Message = e.failureSummary(sch)
				}
				now := e.now()
				r.FinishedAt = &now
				e.audit(ctx, r, "", "run.finish", "", string(st), reason, r.Message)
				e.persist(ctx, r, sch)
				return
			}
			// Should not happen in a valid DAG unless waiting on retry; the
			// retry sleep happens inline below, so this brief sleep is a safety net.
			if !e.sleep(ctx, r, sch, 5*time.Millisecond) {
				e.cancelRemaining(ctx, def, r, sch)
				return
			}
			continue
		}
		for _, taskID := range ready {
			if ctx.Err() != nil {
				e.cancelRemaining(ctx, def, r, sch)
				return
			}
			// CAS-style launch: only a pending task can move to running, and
			// the persisted RUNNING row is written BEFORE invoking the executor.
			tr, ok := sch.Mark(taskID, workflow.TaskRunning, "")
			if !ok {
				continue
			}
			tr2, _ := sch.IncrementAttempt(taskID)
			tr = tr2
			e.audit(ctx, r, taskID, "task.start", string(workflow.TaskPending), string(workflow.TaskRunning),
				workflow.ReasonNone, fmt.Sprintf("attempt=%d", tr.Attempts))
			e.persist(ctx, r, sch)

			taskDef := findTask(def, taskID)
			execCtx := ctx
			var taskCancel context.CancelFunc
			if taskDef != nil && taskDef.TimeoutMilli > 0 {
				execCtx, taskCancel = context.WithTimeout(ctx, time.Duration(taskDef.TimeoutMilli)*time.Millisecond)
			}
			execErr := e.runTask(execCtx, def.ID, taskID, tr.Attempts)
			if taskCancel != nil {
				taskCancel()
			}

			switch {
			case execErr == nil:
				sch.Mark(taskID, workflow.TaskSucceeded, "")
				e.audit(ctx, r, taskID, "task.succeed", string(workflow.TaskRunning), string(workflow.TaskSucceeded),
					workflow.ReasonNone, fmt.Sprintf("attempt=%d", tr.Attempts))
			case ctx.Err() != nil:
				// Run canceled/timed out while the task was executing.
				reason := workflow.ReasonCanceled
				to := workflow.TaskCanceled
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					reason = workflow.ReasonTimeout
					to = workflow.TaskCanceled
				}
				sch.Mark(taskID, to, execErr.Error())
				e.audit(ctx, r, taskID, "task.cancel", string(workflow.TaskRunning), string(to), reason, execErr.Error())
			default:
				maxAttempts := 1
				backoff := 0
				if taskDef != nil {
					if taskDef.MaxAttempts > 0 {
						maxAttempts = taskDef.MaxAttempts
					}
					backoff = taskDef.BackoffMilli
				}
				if tr.Attempts < maxAttempts {
					// Not terminal yet: keep the failure detail in waiting so
					// the run is clearly "mid-retry" on disk.
					sch.MarkWaiting(taskID, execErr.Error())
					e.audit(ctx, r, taskID, "task.fail_retry", string(workflow.TaskRunning), string(workflow.TaskWaiting),
						workflow.ReasonTaskFailed, fmt.Sprintf("attempt=%d/%d err=%q", tr.Attempts, maxAttempts, execErr.Error()))
					e.persist(ctx, r, sch)
					if !e.sleep(ctx, r, sch, time.Duration(backoff)*time.Millisecond) {
						sch.MarkCanceled(taskID, "canceled while waiting for retry")
						e.audit(ctx, r, taskID, "task.cancel", string(workflow.TaskWaiting), string(workflow.TaskCanceled),
							workflow.ReasonCanceled, "canceled during retry backoff")
						e.cancelRemaining(ctx, def, r, sch)
						return
					}
					// Move back to pending so Ready() schedules the next attempt.
					sch.MarkRetry(taskID)
					e.audit(ctx, r, taskID, "task.retry", string(workflow.TaskWaiting), string(workflow.TaskPending),
						workflow.ReasonNone, fmt.Sprintf("next_attempt=%d", tr.Attempts+1))
				} else {
					sch.Mark(taskID, workflow.TaskFailed, execErr.Error())
					e.audit(ctx, r, taskID, "task.fail", string(workflow.TaskRunning), string(workflow.TaskFailed),
						workflow.ReasonMaxRetries, fmt.Sprintf("attempts=%d err=%q", tr.Attempts, execErr.Error()))
					e.skipDownstream(ctx, r, sch, taskID)
				}
			}
			e.persist(ctx, r, sch)
		}
	}
}

func (e *Engine) runTask(ctx context.Context, wfID, taskID string, attempt int) (err error) {
	fn := e.executor(wfID, taskID)
	if fn == nil {
		return fmt.Errorf("no executor registered for task %s/%s", wfID, taskID)
	}
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%w: %v", errTaskPanic, rec)
		}
	}()
	return fn(ctx, wfID, taskID, attempt)
}

var errTaskPanic = errors.New(string(workflow.ReasonTaskPanic))

func (e *Engine) failureSummary(sch *scheduler.Scheduler) string {
	for _, id := range sch.Order() {
		if tr, ok := sch.Task(id); ok && tr.State == workflow.TaskFailed {
			return fmt.Sprintf("task %q failed after %d attempt(s): %s", id, tr.Attempts, tr.LastErr)
		}
	}
	return "one or more tasks did not succeed"
}

func findTask(def *workflow.Definition, id string) *workflow.TaskDef {
	for i := range def.Tasks {
		if def.Tasks[i].ID == id {
			return &def.Tasks[i]
		}
	}
	return nil
}

// settleInterrupted handles tasks persisted as running/waiting after a crash.
// A "running" execution demonstrably never reported a result: it costs one
// attempt and is retried if budget remains, else failed permanently.
func (e *Engine) settleInterrupted(ctx context.Context, def *workflow.Definition, r *workflow.Run, sch *scheduler.Scheduler) {
	for _, tr0 := range sch.Snapshot() {
		switch tr0.State {
		case workflow.TaskRunning:
			taskID := tr0.TaskID
			taskDef := findTask(def, taskID)
			maxAttempts := 1
			if taskDef != nil && taskDef.MaxAttempts > 0 {
				maxAttempts = taskDef.MaxAttempts
			}
			// Attempts was already incremented before the crash, so it counts.
			if tr0.Attempts >= maxAttempts {
				sch.Mark(taskID, workflow.TaskFailed, "interrupted by process restart; retry budget exhausted")
				e.audit(ctx, r, taskID, "task.fail", string(workflow.TaskRunning), string(workflow.TaskFailed),
					workflow.ReasonMaxRetries, fmt.Sprintf("attempts=%d interrupted on restart", tr0.Attempts))
				e.skipDownstream(ctx, r, sch, taskID)
			} else {
				sch.MarkRetry(taskID)
				e.audit(ctx, r, taskID, "task.retry", string(workflow.TaskRunning), string(workflow.TaskPending),
					workflow.ReasonRecoveredInterrupted, fmt.Sprintf("interrupted execution counted as attempt=%d", tr0.Attempts))
			}
		case workflow.TaskWaiting:
			sch.MarkRetry(tr0.TaskID)
			e.audit(ctx, r, tr0.TaskID, "task.retry", string(workflow.TaskWaiting), string(workflow.TaskPending),
				workflow.ReasonRecoveredInterrupted, "backoff interrupted by restart; retrying immediately")
		}
	}
}

func (e *Engine) skipDownstream(ctx context.Context, r *workflow.Run, sch *scheduler.Scheduler, failedID string) {
	def := sch.Def()
	// Mark every not-yet-terminal task whose dependency chain includes the
	// failed task as skipped, so no downstream ever runs and no state dangles.
	skipSet := map[string]bool{}
	for {
		grew := false
		for _, tr := range sch.Snapshot() {
			if skipSet[tr.TaskID] || tr.TaskID == failedID {
				continue
			}
			taskDef := findTask(&def, tr.TaskID)
			if taskDef == nil {
				continue
			}
			for _, dep := range taskDef.Deps {
				if dep == failedID || skipSet[dep] {
					tr2, ok := sch.Task(tr.TaskID)
					if !ok {
						break
					}
					if tr2.State == workflow.TaskPending || tr2.State == workflow.TaskWaiting {
						skipSet[tr.TaskID] = true
						grew = true
					}
					break
				}
			}
		}
		if !grew {
			break
		}
	}
	ids := make([]string, 0, len(skipSet))
	for id := range skipSet {
		ids = append(ids, id)
	}
	for _, id := range sch.Order() {
		if skipSet[id] {
			sch.Mark(id, workflow.TaskSkipped, "upstream task failed: "+failedID)
			e.audit(ctx, r, id, "task.skip", string(workflow.TaskPending), string(workflow.TaskSkipped),
				workflow.ReasonTaskFailed, "blocked by failed task "+failedID)
		}
	}
}

func (e *Engine) cancelRemaining(ctx context.Context, def *workflow.Definition, r *workflow.Run, sch *scheduler.Scheduler) {
	reason := workflow.ReasonCanceled
	runState := workflow.RunCanceled
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		reason = workflow.ReasonTimeout
		runState = workflow.RunTimedOut
	}
	for _, tr := range sch.Snapshot() {
		switch tr.State {
		case workflow.TaskPending, workflow.TaskWaiting:
			sch.Mark(tr.TaskID, workflow.TaskCanceled, "run "+string(reason))
			e.audit(ctx, r, tr.TaskID, "task.cancel", string(tr.State), string(workflow.TaskCanceled),
				reason, "never started")
		case workflow.TaskRunning:
			// If the executor ignored ctx and raced to a terminal result,
			// Mark rejects the regression and the real outcome is kept.
			if _, ok := sch.Mark(tr.TaskID, workflow.TaskCanceled, "run "+string(reason)); ok {
				e.audit(ctx, r, tr.TaskID, "task.cancel", string(workflow.TaskRunning), string(workflow.TaskCanceled),
					reason, "interrupted")
			}
		}
	}
	r.State = runState
	r.Reason = reason
	r.Message = "run " + string(reason)
	now := e.now()
	r.FinishedAt = &now
	e.audit(ctx, r, "", "run.finish", "", string(runState), reason, r.Message)
	e.persist(ctx, r, sch)
}

// sleep waits d or until ctx is canceled. It tolerates d==0. Returns false
// when cancellation was observed.
func (e *Engine) sleep(ctx context.Context, r *workflow.Run, sch *scheduler.Scheduler, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// finishRun marks the runtime slot free and starts the next queued run.
func (e *Engine) finishRun(ctx context.Context, def *workflow.Definition, r *workflow.Run) {
	e.mu.Lock()
	rt := e.runtimeLocked(r.WorkflowID)
	waiters := append([]chan *runOutcome(nil), rt.waiters[r.ID]...)
	delete(rt.waiters, r.ID)
	delete(rt.signaled, r.ID)

	var nextRunID string
	if rt.activeRunID == r.ID {
		rt.activeRunID = ""
		if len(rt.queue) > 0 {
			nextRunID = rt.queue[0]
			rt.queue = rt.queue[1:]
			rt.activeRunID = nextRunID
		}
	}
	e.mu.Unlock()

	res := &RunResult{RunID: r.ID, State: r.State, Reason: r.Reason, Message: r.Message}
	for _, ch := range waiters {
		ch <- &runOutcome{result: res}
	}

	if nextRunID != "" {
		if next, ok, _ := e.st.GetRun(ctx, nextRunID); ok {
			defCopy := *def
			go e.executeRun(ctx, &defCopy, &next, false)
		}
	}
}
