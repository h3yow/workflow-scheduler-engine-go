package workflow

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// RecoveryPolicy decides how non-terminal runs found at startup are handled.
type RecoveryPolicy string

const (
	// RecoveryResume re-activates non-terminal runs and continues scheduling.
	// Tasks that were RUNNING when the process died are re-invoked (handlers
	// must be idempotent); Attempt counters are never reset.
	RecoveryResume RecoveryPolicy = "resume"
	// RecoveryTerminate marks non-terminal runs as INTERRUPTED and cancels all
	// their unfinished tasks, giving an explicit terminal reason instead of
	// leaving an intermediate state dangling.
	RecoveryTerminate RecoveryPolicy = "terminate"
)

// Option configures an Engine.
type Option func(*Engine)

// WithRecovery sets the startup recovery policy (default RecoveryResume).
func WithRecovery(p RecoveryPolicy) Option {
	return func(e *Engine) { e.recovery = p }
}

// WithMaxParallel sets max concurrently running tasks per run (default 4).
func WithMaxParallel(n int) Option {
	return func(e *Engine) {
		if n > 0 {
			e.maxParallel = n
		}
	}
}

// WithEventLogger installs an extra sink for transition events (tests use it).
func WithEventLogger(sink func(Event)) Option {
	return func(e *Engine) { e.sink = sink }
}

// WithClock overrides the time source (tests).
func WithClock(now func() time.Time) Option {
	return func(e *Engine) { e.now = now }
}

// Engine is the local workflow scheduler. One Engine owns one Store.
type Engine struct {
	store       Store
	registry    *Registry
	recovery    RecoveryPolicy
	maxParallel int
	sink        func(Event)
	now         func() time.Time

	idCounter uint64

	mu      sync.Mutex
	running map[RunID]*runController
	wg      sync.WaitGroup // tracks active scheduling goroutines
	closed  bool
}

// NewEngine builds an engine and recovers persisted non-terminal runs.
func NewEngine(store Store, registry *Registry, opts ...Option) (*Engine, error) {
	e := &Engine{
		store:       store,
		registry:    registry,
		recovery:    RecoveryResume,
		maxParallel: 4,
		now:         time.Now,
		running:     map[RunID]*runController{},
	}
	for _, o := range opts {
		o(e)
	}
	if err := e.recover(); err != nil {
		return nil, err
	}
	return e, nil
}

// newID produces a unique, time-sortable id.
func (e *Engine) newID(prefix string) string {
	n := atomic.AddUint64(&e.idCounter, 1)
	return fmt.Sprintf("%s-%d-%s", prefix, e.now().UnixNano(), strconv.FormatUint(n, 10))
}

// Register validates and persists a workflow definition. Invalid definitions
// (cycle / missing dependency / duplicate task) are rejected here, before any
// run can be scheduled.
func (e *Engine) Register(def WorkflowDef) error {
	if def.Name == "" {
		return &ValidationError{Reason: ReasonInvalidDefinition, Msg: "workflow name must not be empty"}
	}
	if def.TriggerConcurrency == "" {
		def.TriggerConcurrency = ConcurrencyReject
	}
	if err := ValidateDefinition(def); err != nil {
		return err
	}
	return e.store.PutWorkflow(def)
}

// Trigger starts a new run subject to the workflow's concurrency policy.
// Manual, scheduled and event triggers all flow through this single method,
// so policy/timeout/cancel semantics are identical regardless of source.
//
// The critical section (inspect active run -> decide policy -> create the new
// run) holds e.mu so concurrent triggers can never create duplicate runs.
func (e *Engine) Trigger(ctx context.Context, workflow string, src TriggerSource) (Run, error) {
	def, err := e.store.GetWorkflow(workflow)
	if err != nil {
		return Run{}, fmt.Errorf("%w: %s", ErrUnknownWorkflow, workflow)
	}

	for {
		e.mu.Lock()
		if e.closed {
			e.mu.Unlock()
			return Run{}, fmt.Errorf("engine closed")
		}
		active, exists, err := e.store.ActiveRun(workflow)
		if err != nil {
			e.mu.Unlock()
			return Run{}, err
		}
		if !exists {
			run, err := e.startRunLocked(def, src)
			e.mu.Unlock()
			return run, err
		}

		rc := e.running[active.ID]
		switch def.TriggerConcurrency {
		case ConcurrencyReject:
			e.mu.Unlock()
			return active, fmt.Errorf("%w: %s (run %s, trigger %s)",
				ErrRunActive, workflow, active.ID, src)

		case ConcurrencyQueue:
			if rc == nil {
				// Persisted non-terminal run without a live controller:
				// re-attach it before we can wait for it.
				if err := e.resumeRunLocked(active, def); err != nil {
					e.mu.Unlock()
					return Run{}, err
				}
				rc = e.running[active.ID]
			}
			ch := rc.done()
			e.mu.Unlock()
			select {
			case <-ch:
			case <-ctx.Done():
				return active, ctx.Err()
			}
			// loop: re-check, possibly creating our own run

		case ConcurrencyCancel:
			if rc == nil {
				if err := e.resumeRunLocked(active, def); err != nil {
					e.mu.Unlock()
					return Run{}, err
				}
				rc = e.running[active.ID]
			}
			ch := rc.done()
			rc.cancel(ReasonCanceled)
			e.mu.Unlock()
			select {
			case <-ch:
			case <-ctx.Done():
				return active, ctx.Err()
			}
			// loop: active run is gone, create the replacement run

		default:
			e.mu.Unlock()
			return Run{}, &ValidationError{
				Reason: ReasonInvalidDefinition, Msg: "unknown concurrency policy",
			}
		}
	}
}

// startRunLocked materializes a run with the engine lock held. All records
// (run + one PENDING task per task definition) are persisted before the
// scheduling goroutine starts, so a crash leaves a fully recoverable run.
func (e *Engine) startRunLocked(def WorkflowDef, src TriggerSource) (Run, error) {
	now := e.now()
	run := Run{
		ID:        RunID(e.newID("run")),
		Workflow:  def.Name,
		Trigger:   src,
		State:     RunPending,
		CreatedAt: now,
		UpdatedAt: now,
		DedupKey:  def.Name,
	}
	if err := e.store.CreateRun(run); err != nil {
		return Run{}, err
	}
	for _, td := range tdList(def) {
		tr := TaskRun{
			ID:        TaskRunID(string(run.ID) + ":" + td.ID),
			RunID:     run.ID,
			TaskID:    td.ID,
			Attempt:   0,
			State:     TaskPending,
			StartedAt: now,
			UpdatedAt: now,
		}
		if err := e.store.CreateTask(tr); err != nil {
			return Run{}, err
		}
	}
	if _, _, err := e.transitRun(run.ID, RunPending, RunRunning, "",
		string(src)+" trigger accepted", src); err != nil {
		return Run{}, err
	}
	run.State = RunRunning

	rc := newRunController(run.ID, src, def.Timeout)
	e.running[rc.runID] = rc
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.scheduleLoop(rc, def)
	}()
	return run, nil
}

// resumeRunLocked attaches and launches a scheduler for an existing run.
func (e *Engine) resumeRunLocked(r Run, def WorkflowDef) error {
	rc := newRunController(r.ID, r.Trigger, def.Timeout)
	e.running[rc.runID] = rc
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.scheduleLoop(rc, def)
	}()
	return nil
}

// Cancel requests cancellation of a run. After Cancel returns successfully:
//   - no new task will be started,
//   - running tasks observe ctx cancellation and finish,
//   - pending tasks are marked CANCELED,
//   - the run terminates as CANCELED with the given reason.
func (e *Engine) Cancel(runID RunID, reason string) error {
	e.mu.Lock()
	rc := e.running[runID]
	e.mu.Unlock()
	if rc == nil {
		// Persisted but not attached (e.g. another process): record an
		// explicit terminal state so recovery honors the request.
		r, err := e.store.GetRun(runID)
		if err != nil {
			return err
		}
		if r.State.IsTerminal() {
			return nil
		}
		_, _, err = e.transitRun(runID, r.State, RunCanceled, reason,
			"cancel requested while unattached", r.Trigger)
		return err
	}
	rc.cancel(reason)
	return nil
}

// WaitRun blocks until the run reaches a terminal state or ctx is done.
func (e *Engine) WaitRun(ctx context.Context, runID RunID) (Run, error) {
	for {
		r, err := e.store.GetRun(runID)
		if err != nil {
			return Run{}, err
		}
		if r.State.IsTerminal() {
			return r, nil
		}
		e.mu.Lock()
		rc := e.running[runID]
		var ch <-chan struct{}
		if rc != nil {
			ch = rc.done()
		}
		e.mu.Unlock()
		if ch == nil {
			select {
			case <-ctx.Done():
				return r, ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
			continue
		}
		select {
		case <-ch:
			return e.store.GetRun(runID)
		case <-ctx.Done():
			return r, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// Close stops accepting triggers and waits for in-memory scheduling to wind
// down. Persisted state is unaffected; construct a new engine to resume.
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.mu.Unlock()
	e.wg.Wait()
	return e.store.Close()
}

// recover applies the configured RecoveryPolicy to all non-terminal runs.
func (e *Engine) recover() error {
	runs, err := e.store.ListRecoverableRuns()
	if err != nil {
		return err
	}
	// Attach under the engine lock, same as Trigger.
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, r := range runs {
		def, gerr := e.store.GetWorkflow(r.Workflow)
		if gerr != nil {
			return fmt.Errorf("recover run %s: %w", r.ID, gerr)
		}
		switch e.recovery {
		case RecoveryTerminate:
			// terminateInterrupted does store transitions; safe under the lock.
			if err := e.terminateInterrupted(r); err != nil {
				return err
			}
		case RecoveryResume, "":
			if err := e.resumeRunLocked(r, def); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown recovery policy: %s", e.recovery)
		}
	}
	return nil
}
