package workflow

import (
	"context"
	"errors"
	"sync"
	"time"
)

// runController is the in-memory handle for one live run.
type runController struct {
	runID   RunID
	trigger TriggerSource
	timeout time.Duration

	ctx      context.Context
	cancelFn context.CancelFunc

	mu        sync.Mutex
	cancelRsn string // empty until cancellation is requested
	once      sync.Once
	doneCh    chan struct{}
	tick      chan struct{} // signaled whenever a task attempt finishes
}

func newRunController(id RunID, src TriggerSource, timeout time.Duration) *runController {
	ctx, cancelFn := context.WithCancel(context.Background())
	rc := &runController{
		runID:    id,
		trigger:  src,
		timeout:  timeout,
		ctx:      ctx,
		cancelFn: cancelFn,
		doneCh:   make(chan struct{}),
		tick:     make(chan struct{}, 1),
	}
	if timeout > 0 {
		// A run deadline is cancellation with a distinct reason (TIMEOUT),
		// keeping timeout and explicit cancellation explainable in the log.
		time.AfterFunc(timeout, func() {
			if rc.ctx.Err() == nil {
				rc.cancel(ReasonTimeout)
			}
		})
	}
	return rc
}

func (r *runController) cancel(reason string) {
	r.mu.Lock()
	if r.cancelRsn == "" {
		r.cancelRsn = reason
	}
	r.mu.Unlock()
	r.cancelFn()
}

func (r *runController) cancelReason() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cancelRsn
}

func (r *runController) canceled() bool { return r.ctx.Err() != nil }

func (r *runController) done() <-chan struct{} { return r.doneCh }

func (r *runController) signal() {
	select {
	case r.tick <- struct{}{}:
	default:
	}
}

func tdList(def WorkflowDef) []TaskDef {
	out := make([]TaskDef, len(def.Tasks))
	copy(out, def.Tasks)
	return out
}

// emit appends a transition event to the log and fans it out to the test sink.
func (e *Engine) emit(ev Event) {
	if ev.Time.IsZero() {
		ev.Time = e.now()
	}
	if err := e.store.EventLog().Append(ev); err != nil {
		// The event log is the audit trail; failing to write it is fatal to
		// correctness of "traceability", so surface it via the sink too.
		ev.Message += " (event persist error: " + err.Error() + ")"
	}
	if e.sink != nil {
		e.sink(ev)
	}
}

// transitRun performs a compare-and-swap run transition and logs it.
func (e *Engine) transitRun(id RunID, expect, to RunState, reason, msg string, src TriggerSource) (Run, bool, error) {
	r, err := e.store.GetRun(id)
	if err != nil {
		return Run{}, false, err
	}
	from := r.State
	now := e.now()
	cp, ok, err := e.store.CompareAndSwapRun(id, expect, func(r *Run) {
		r.State = to
		r.Reason = reason
		r.Message = msg
		if to.IsTerminal() {
			t := now
			r.EndedAt = &t
		}
	})
	if err != nil {
		return cp, false, err
	}
	if ok {
		e.emit(Event{
			RunID: id, FromState: string(from), ToState: string(to),
			Trigger: src, Reason: reason, Message: msg,
		})
	}
	return cp, ok, nil
}

// transitTask performs a compare-and-swap task transition and logs it.
func (e *Engine) transitTask(id TaskRunID, expect, to TaskState, reason, msg, output string, src TriggerSource) (TaskRun, bool, error) {
	t, err := e.store.GetTask(id)
	if err != nil {
		return TaskRun{}, false, err
	}
	from := t.State
	now := e.now()
	cp, ok, err := e.store.CompareAndSwapTask(id, expect, func(t *TaskRun) {
		t.State = to
		if reason != "" {
			t.ErrCode = reason
		}
		if msg != "" {
			t.ErrMsg = msg
		}
		if output != "" {
			t.Output = output
		}
		if to.IsTerminal() {
			tt := now
			t.EndedAt = &tt
		}
	})
	if err != nil {
		return cp, false, err
	}
	if ok {
		e.emit(Event{
			RunID: t.RunID, TaskID: t.TaskID,
			FromState: string(from), ToState: string(to),
			Trigger: src, Reason: reason, Message: msg,
		})
	}
	return cp, ok, nil
}

// terminateInterrupted implements RecoveryTerminate: the run gets an explicit
// terminal INTERRUPTED state and every unfinished task is CANCELED, so no
// intermediate state can dangle.
func (e *Engine) terminateInterrupted(r Run) error {
	tasks, err := e.store.ListTasks(r.ID)
	if err != nil {
		return err
	}
	for _, t := range tasks {
		if t.State.IsTerminal() {
			continue
		}
		if _, _, err := e.transitTask(t.ID, t.State, TaskCanceled, ReasonRunInterrupted,
			"run terminated during recovery", "", r.Trigger); err != nil {
			return err
		}
	}
	if !r.State.IsTerminal() {
		if _, _, err := e.transitRun(r.ID, r.State, RunInterrupted, ReasonRunInterrupted,
			"process restarted with recovery=terminate", r.Trigger); err != nil {
			return err
		}
	}
	return nil
}

// scheduleLoop is the single dispatcher goroutine for one run. Because it is
// the only goroutine that chooses to launch a task, and every launch is gated
// by a compare-and-swap to RUNNING, a task can never start twice.
func (e *Engine) scheduleLoop(rc *runController, def WorkflowDef) {
	defer close(rc.doneCh)
	defer func() {
		e.mu.Lock()
		delete(e.running, rc.runID)
		e.mu.Unlock()
	}()

	// A run may be persisted as PENDING if the process died mid-creation.
	if r, err := e.store.GetRun(rc.runID); err == nil && r.State == RunPending {
		_, _, _ = e.transitRun(rc.runID, RunPending, RunRunning, "", "run activated", rc.trigger)
	}

	defs := map[string]TaskDef{}
	for _, td := range tdList(def) {
		defs[td.ID] = td
	}

	sem := make(chan struct{}, e.maxParallel)
	var wg sync.WaitGroup
	// slotFreed is closed-and-replaced each time a worker returns its slot,
	// letting the dispatcher block until parallelism capacity comes back.
	var slotMu sync.Mutex
	slotFreed := make(chan struct{})
	markSlotFreed := func() {
		slotMu.Lock()
		close(slotFreed)
		slotFreed = make(chan struct{})
		slotMu.Unlock()
	}
	slotWait := func() <-chan struct{} {
		slotMu.Lock()
		defer slotMu.Unlock()
		return slotFreed
	}
	// launched tracks RUNNING tasks this controller has dispatched, so a
	// recovered RUNNING task is re-invoked exactly once.
	launched := map[string]bool{}

	wait := func() bool {
		select {
		case <-rc.ctx.Done():
			return false
		case <-rc.tick:
			return true
		case <-slotWait():
			return true
		case <-time.After(200 * time.Millisecond):
			// Safety ticker: protects against missing a signal during recovery.
			return true
		}
	}

	for {
		tasks, err := e.store.ListTasks(rc.runID)
		if err != nil {
			return
		}
		cur := map[string]TaskRun{}
		allTerminal := true
		for _, t := range tasks {
			cur[t.TaskID] = t
			if !t.State.IsTerminal() {
				allTerminal = false
			}
		}

		// Cancellation: never start anything new; settle pending tasks.
		if rc.canceled() {
			reason := rc.cancelReason()
			if reason == "" {
				reason = ReasonCanceled
			}
			for _, t := range tasks {
				if t.State == TaskPending {
					_, _, _ = e.transitTask(t.ID, TaskPending, TaskCanceled, reason,
						"canceled before start", "", rc.trigger)
				}
			}
			// Let in-flight workers (already CAS'd to RUNNING) write their own
			// terminal state; they never exit without doing so.
			wg.Wait()
			// Anything still non-terminal (e.g. a recovered RUNNING task that
			// was never re-dispatched) must not be left dangling: settle it.
			if rest, err := e.store.ListTasks(rc.runID); err == nil {
				for _, t := range rest {
					if !t.State.IsTerminal() {
						_, _, _ = e.transitTask(t.ID, t.State, TaskCanceled, reason,
							"canceled without an in-flight attempt", "", rc.trigger)
					}
				}
			}
			e.finalizeRun(rc)
			return
		}

		if allTerminal {
			wg.Wait()
			e.finalizeRun(rc)
			return
		}

		progress := false

		// Pass 1: propagate failures -> SKIPPED downstream.
		for _, td := range tdList(def) {
			t := cur[td.ID]
			if t.State != TaskPending {
				continue
			}
			for _, dep := range td.Dependencies {
				dt := cur[dep]
				if dt.State == TaskFailed || dt.State == TaskSkipped || dt.State == TaskCanceled {
					_, _, _ = e.transitTask(t.ID, TaskPending, TaskSkipped, ReasonUpstreamFailed,
						"upstream task "+dep+" is "+string(dt.State), "", rc.trigger)
					progress = true
					break
				}
			}
		}
		if progress {
			continue
		}

		// Pass 2: dispatch ready tasks (deps all SUCCEEDED).
		slotFull := len(sem) >= cap(sem)
		for _, td := range tdList(def) {
			t := cur[td.ID]
			ready := false
			expect := TaskState("")
			switch t.State {
			case TaskPending:
				ready = true
				expect = TaskPending
			case TaskRunning:
				if !launched[td.ID] {
					ready = true // recovered in-doubt task
					expect = TaskRunning
				}
			}
			if !ready {
				continue
			}
			depsOK := true
			for _, dep := range td.Dependencies {
				if cur[dep].State != TaskSucceeded {
					depsOK = false
					break
				}
			}
			if !depsOK {
				continue
			}
			if slotFull {
				// Parallel capacity exhausted; wait for a slot in the idle
				// branch below instead of busy-looping.
				break
			}

			// Claim the attempt atomically: PENDING/RUNNING -> RUNNING with
			// Attempt+1. This CAS is the dedup gate against double execution.
			claimed, ok, err := e.store.CompareAndSwapTask(t.ID, expect, func(x *TaskRun) {
				x.State = TaskRunning
				x.Attempt++
				x.ErrCode = ""
				x.ErrMsg = ""
				x.StartedAt = e.now()
			})
			if err != nil || !ok {
				continue
			}
			e.emit(Event{
				RunID: rc.runID, TaskID: td.ID,
				FromState: string(expect), ToState: string(TaskRunning),
				Trigger: rc.trigger, Message: "attempt " + itoa(claimed.Attempt) + " started",
			})
			launched[td.ID] = true
			progress = true
			sem <- struct{}{}
			slotFull = len(sem) >= cap(sem)
			wg.Add(1)
			go func(td TaskDef, tr TaskRun) {
				defer wg.Done()
				defer func() { <-sem; markSlotFreed() }()
				defer rc.signal()
				e.executeTask(rc, def.Name, td, tr.ID)
			}(td, claimed)
		}

		if !progress {
			// Waiting on running tasks (or a worker slot); block until woken.
			if !wait() {
				continue // ctx canceled -> handled at loop top
			}
		}
	}
}

// executeTask runs one task's handler with per-attempt timeout and retries.
// Retry attempt counting only ever increases, and all transitions are CAS.
func (e *Engine) executeTask(rc *runController, workflow string, td TaskDef, id TaskRunID) {
	fn, ok := e.registry.Lookup(td.ID)
	if !ok {
		_, _, _ = e.transitTask(id, TaskRunning, TaskFailed, ReasonNoHandler,
			"no handler registered", "", rc.trigger)
		return
	}
	maxAttempts := td.Retries.attempts()

	for {
		cur, err := e.store.GetTask(id)
		if err != nil || cur.State != TaskRunning {
			return // superseded (e.g. canceled by recovery)
		}
		tc := TaskContext{
			RunID:    rc.runID,
			Workflow: workflow,
			TaskID:   td.ID,
			Attempt:  cur.Attempt,
			Trigger:  rc.trigger,
		}
		var (
			cctx   = rc.ctx
			cancel context.CancelFunc
		)
		if td.Timeout > 0 {
			cctx, cancel = context.WithTimeout(rc.ctx, td.Timeout)
		}
		output, herr := fn(cctx, tc)
		if cancel != nil {
			cancel()
		}

		// Cancellation/timeout of the whole run takes precedence in the
		// bookkeeping, but a handler that nevertheless completed successfully
		// keeps its SUCCEEDED result so outcomes remain explainable.
		if rc.canceled() {
			if herr == nil {
				_, _, _ = e.transitTask(id, TaskRunning, TaskSucceeded, "",
					"completed just before/at cancellation ("+rc.cancelReason()+")", output, rc.trigger)
				return
			}
			_, _, _ = e.transitTask(id, TaskRunning, TaskCanceled, rc.cancelReason(),
				"interrupted: "+herr.Error(), "", rc.trigger)
			return
		}

		if herr == nil {
			_, _, _ = e.transitTask(id, TaskRunning, TaskSucceeded, "", "", output, rc.trigger)
			return
		}

		code, msg := classifyError(cctx, herr)

		cur, err = e.store.GetTask(id)
		if err != nil || cur.State != TaskRunning {
			return
		}
		if cur.Attempt >= maxAttempts {
			_, _, _ = e.transitTask(id, TaskRunning, TaskFailed, code,
				"retries exhausted after "+itoa(cur.Attempt)+" attempt(s): "+msg, "", rc.trigger)
			return
		}

		// Wait out the backoff without starting a new attempt after cancel.
		d := backoff(td.Retries, cur.Attempt)
		select {
		case <-rc.ctx.Done():
			rsn := rc.cancelReason()
			if rsn == "" {
				rsn = ReasonCanceled
			}
			_, _, _ = e.transitTask(id, TaskRunning, TaskCanceled, rsn,
				"canceled during retry backoff", "", rc.trigger)
			return
		case <-time.After(d):
		}

		// Claim the next attempt before invoking the handler again.
		_, claimed, err := e.store.CompareAndSwapTask(id, TaskRunning, func(x *TaskRun) {
			x.Attempt++
			x.StartedAt = e.now()
		})
		if err != nil || !claimed {
			return
		}
		e.emit(Event{
			RunID: rc.runID, TaskID: td.ID,
			FromState: string(TaskRunning), ToState: string(TaskRunning),
			Trigger: rc.trigger, Reason: code,
			Message: "attempt " + itoa(cur.Attempt+1) + " after failure: " + msg,
		})
	}
}

// classifyError maps a handler error to a stable code/message, distinguishing
// per-attempt timeout from handler failures.
func classifyError(ctx context.Context, err error) (string, string) {
	var te *TaskError
	if errors.As(err, &te) {
		return te.Code, te.Msg
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return ReasonTimeout, err.Error()
	}
	return "TASK_ERROR", err.Error()
}

// backoff computes exponential backoff: initial * 2^(attempt-1), capped.
func backoff(p RetryPolicy, attempt int) time.Duration {
	if p.InitialBackoff <= 0 {
		return 0
	}
	d := p.InitialBackoff
	for i := 1; i < attempt; i++ {
		d *= 2
		if p.MaxBackoff > 0 && d > p.MaxBackoff {
			return p.MaxBackoff
		}
	}
	if p.MaxBackoff > 0 && d > p.MaxBackoff {
		return p.MaxBackoff
	}
	return d
}

// finalizeRun derives the terminal run state from its tasks and the cancel
// reason, then CAS-es the run record to terminal exactly once.
func (e *Engine) finalizeRun(rc *runController) {
	tasks, err := e.store.ListTasks(rc.runID)
	if err != nil {
		return
	}
	var failed, skipped, canceled, succeeded []TaskRun
	for _, t := range tasks {
		switch t.State {
		case TaskFailed:
			failed = append(failed, t)
		case TaskSkipped:
			skipped = append(skipped, t)
		case TaskCanceled:
			canceled = append(canceled, t)
		case TaskSucceeded:
			succeeded = append(succeeded, t)
		}
	}

	var to RunState
	var reason, msg string
	switch {
	case len(failed) > 0:
		to = RunFailed
		reason = ReasonRetryExhausted
		f := failed[0]
		if f.ErrCode != "" {
			reason = f.ErrCode
		}
		msg = "task " + f.TaskID + " failed: " + f.ErrMsg
		if len(skipped) > 0 {
			msg += "; " + itoa(len(skipped)) + " downstream task(s) skipped"
		}
	case rc.canceled():
		if rc.cancelReason() == ReasonTimeout {
			to = RunFailed
			reason = ReasonTimeout
			msg = "run deadline exceeded"
		} else {
			to = RunCanceled
			reason = rc.cancelReason()
			if reason == "" {
				reason = ReasonCanceled
			}
			msg = "run canceled; " + itoa(len(canceled)) + " task(s) canceled, " +
				itoa(len(succeeded)) + " succeeded"
		}
	default:
		to = RunSucceeded
		msg = "all " + itoa(len(succeeded)) + " task(s) succeeded"
	}

	r, err := e.store.GetRun(rc.runID)
	if err == nil && !r.State.IsTerminal() {
		_, _, _ = e.transitRun(rc.runID, r.State, to, reason, msg, rc.trigger)
	}
}

func itoa(n int) string {
	return strconvItoa(n)
}
