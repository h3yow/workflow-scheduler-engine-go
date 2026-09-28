package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/workflow-scheduler-engine-go/internal/store"
	"github.com/h3yow/workflow-scheduler-engine-go/internal/workflow"
)

type testHarness struct {
	t    *testing.T
	st   *store.FileStore
	eng  *Engine
	logs *bytes.Buffer
}

func newHarness(t *testing.T) *testHarness {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := store.NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	buf := &bytes.Buffer{}
	lw := io.MultiWriter(buf, log.Writer())
	eng := New(st, WithLogger(log.New(lw, "", log.LstdFlags)))
	t.Cleanup(func() { _ = eng.Close() })
	return &testHarness{t: t, st: st, eng: eng, logs: buf}
}

func (h *testHarness) register(def workflow.Definition) {
	h.t.Helper()
	if err := h.eng.RegisterDefinition(context.Background(), def); err != nil {
		h.t.Fatalf("register %s: %v", def.ID, err)
	}
}

func (h *testHarness) trigger(wf string, src workflow.TriggerSource) *RunResult {
	h.t.Helper()
	res, err := h.eng.Trigger(context.Background(), wf, src)
	if err != nil {
		h.t.Fatalf("trigger %s/%s: %v", wf, src, err)
	}
	return res
}

func (h *testHarness) mustRun(r *RunResult, state workflow.RunState, reason workflow.Reason) workflow.Run {
	h.t.Helper()
	if r.State != state || r.Reason != reason {
		h.t.Fatalf("run %s state=%s reason=%s, want %s/%s", r.RunID, r.State, r.Reason, state, reason)
	}
	got, ok, _ := h.st.GetRun(context.Background(), r.RunID)
	if !ok {
		h.t.Fatalf("run %s not persisted", r.RunID)
	}
	return got
}

func blockOnCancel(wait time.Duration) ExecutorFunc {
	return func(ctx context.Context, _, _ string, _ int) error {
		select {
		case <-time.After(wait):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func TestDependencyOrderAndRetry(t *testing.T) {
	h := newHarness(t)
	def := workflow.Definition{
		ID: "wf", Concurrency: workflow.ConcurrencySkip,
		Tasks: []workflow.TaskDef{
			{ID: "a", MaxAttempts: 1},
			{ID: "b", Deps: []string{"a"}, MaxAttempts: 3, BackoffMilli: 5},
			{ID: "c", Deps: []string{"b"}, MaxAttempts: 1},
		},
	}
	h.register(def)

	var order []string
	var orderMu sync.Mutex
	h.eng.RegisterExecutor("wf", "a", func(ctx context.Context, _, _ string, _ int) error {
		time.Sleep(10 * time.Millisecond)
		orderMu.Lock()
		order = append(order, "a")
		orderMu.Unlock()
		return nil
	})
	var bAttempts int32
	h.eng.RegisterExecutor("wf", "b", func(ctx context.Context, _, _ string, attempt int) error {
		n := atomic.AddInt32(&bAttempts, 1)
		orderMu.Lock()
		order = append(order, fmt.Sprintf("b%d", attempt))
		orderMu.Unlock()
		if n < 3 {
			return fmt.Errorf("transient-%d", n)
		}
		return nil
	})
	h.eng.RegisterExecutor("wf", "c", func(ctx context.Context, _, _ string, attempt int) error {
		if attempt != 1 {
			t.Errorf("c executed %d times, must run exactly once", attempt)
		}
		orderMu.Lock()
		order = append(order, "c")
		orderMu.Unlock()
		return nil
	})

	res := h.trigger("wf", workflow.TriggerManual)
	run := h.mustRun(res, workflow.RunSucceeded, workflow.ReasonNone)

	orderMu.Lock()
	got := append([]string(nil), order...)
	orderMu.Unlock()
	want := []string{"a", "b1", "b2", "b3", "c"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("execution order = %v, want %v", got, want)
	}
	tasks := map[string]workflow.TaskRun{}
	for _, tr := range run.Tasks {
		tasks[tr.TaskID] = tr
	}
	if tasks["b"].Attempts != 3 || tasks["b"].State != workflow.TaskSucceeded {
		t.Fatalf("b = %+v, want attempts=3 succeeded", tasks["b"])
	}
	if tasks["a"].Attempts != 1 || tasks["c"].Attempts != 1 {
		t.Fatalf("a/c attempts = %d/%d, want 1/1", tasks["a"].Attempts, tasks["c"].Attempts)
	}
	t.Logf("source=%s run=%s transitions:", workflow.TriggerManual, res.RunID)
	for _, ev := range h.auditLines(res.RunID) {
		t.Log("  ", ev)
	}
}

func (h *testHarness) auditLines(runID string) []string {
	events, _ := h.st.ListAudit(context.Background(), runID)
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, fmt.Sprintf("source=%s task=%s %s %s->%s reason=%s %s",
			e.Source, e.TaskID, e.Event, e.From, e.To, e.Reason, e.Detail))
	}
	return out
}

func TestConcurrentTriggersSkipPolicy(t *testing.T) {
	h := newHarness(t)
	def := workflow.Definition{
		ID: "wf", Concurrency: workflow.ConcurrencySkip,
		Tasks: []workflow.TaskDef{{ID: "slow", MaxAttempts: 1}},
	}
	h.register(def)
	var starts int32
	h.eng.RegisterExecutor("wf", "slow", func(ctx context.Context, _, _ string, _ int) error {
		atomic.AddInt32(&starts, 1)
		time.Sleep(120 * time.Millisecond)
		return nil
	})

	sources := []workflow.TriggerSource{workflow.TriggerManual, workflow.TriggerTimer, workflow.TriggerEvent,
		workflow.TriggerManual, workflow.TriggerEvent}
	var wg sync.WaitGroup
	resCh := make(chan *RunResult, len(sources))
	for _, src := range sources {
		wg.Add(1)
		go func(s workflow.TriggerSource) {
			defer wg.Done()
			r, err := h.eng.Trigger(context.Background(), "wf", s)
			if err != nil {
				t.Errorf("trigger %s: %v", s, err)
				return
			}
			resCh <- r
		}(src)
	}
	wg.Wait()
	close(resCh)

	executed, skipped := 0, 0
	for r := range resCh {
		if r.Skipped {
			if r.Reason != workflow.ReasonAlreadyRunning {
				t.Fatalf("skipped run reason = %s", r.Reason)
			}
			skipped++
		} else {
			executed++
		}
	}
	if executed != 1 || skipped != len(sources)-1 {
		t.Fatalf("executed=%d skipped=%d, want 1 and %d", executed, skipped, len(sources)-1)
	}
	if got := atomic.LoadInt32(&starts); got != 1 {
		t.Fatalf("task started %d times across all triggers, want exactly 1", got)
	}
	t.Logf("5 concurrent triggers from 3 sources -> 1 executed, 4 skipped; task starts=1")
}

func TestConcurrentTriggersQueuePolicySerialise(t *testing.T) {
	h := newHarness(t)
	def := workflow.Definition{
		ID: "wf", Concurrency: workflow.ConcurrencyQueue,
		Tasks: []workflow.TaskDef{{ID: "job", MaxAttempts: 1}},
	}
	h.register(def)
	var starts int32
	var mu sync.Mutex
	overlap := false
	runningNow := 0
	h.eng.RegisterExecutor("wf", "job", func(ctx context.Context, _, _ string, _ int) error {
		mu.Lock()
		runningNow++
		if runningNow > 1 {
			overlap = true
		}
		starts++
		mu.Unlock()
		time.Sleep(40 * time.Millisecond)
		mu.Lock()
		runningNow--
		mu.Unlock()
		return nil
	})

	const n = 4
	var wg sync.WaitGroup
	resCh := make(chan *RunResult, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		src := []workflow.TriggerSource{workflow.TriggerManual, workflow.TriggerTimer, workflow.TriggerEvent}[i%3]
		go func(s workflow.TriggerSource) {
			defer wg.Done()
			r, err := h.eng.Trigger(context.Background(), "wf", s)
			if err != nil {
				t.Errorf("trigger: %v", err)
				return
			}
			resCh <- r
		}(src)
	}
	wg.Wait()
	close(resCh)
	ids := map[string]bool{}
	for r := range resCh {
		if r.State != workflow.RunSucceeded {
			t.Fatalf("queued run state=%s reason=%s", r.State, r.Reason)
		}
		if r.Skipped {
			t.Fatal("queue policy must not skip")
		}
		ids[r.RunID] = true
	}
	if len(ids) != n {
		t.Fatalf("distinct successful runs = %d, want %d", len(ids), n)
	}
	if overlap {
		t.Fatal("queued runs overlapped in execution")
	}
	if got := atomic.LoadInt32(&starts); got != n {
		t.Fatalf("task starts = %d, want %d", got, n)
	}
	t.Logf("%d triggers serialised into %d non-overlapping runs, task starts=%d", n, n, starts)
}

func TestValidationRejectionReasons(t *testing.T) {
	h := newHarness(t)
	r1 := validationReason(t, h, workflow.Definition{ID: "x", Tasks: []workflow.TaskDef{{ID: "a", Deps: []string{"nope"}}}})
	if r1 != workflow.ReasonDAGMissingDep {
		t.Fatalf("missing dep reason = %q", r1)
	}
	r2 := validationReason(t, h, workflow.Definition{ID: "y", Tasks: []workflow.TaskDef{
		{ID: "a", Deps: []string{"b"}}, {ID: "b", Deps: []string{"a"}},
	}})
	if r2 != workflow.ReasonDAGCycle {
		t.Fatalf("cycle reason = %q", r2)
	}
	t.Logf("rejection reasons distinguishable: %s vs %s", r1, r2)
}

func TestFailureBlocksDownstream(t *testing.T) {
	h := newHarness(t)
	def := workflow.Definition{
		ID: "wf", Concurrency: workflow.ConcurrencySkip,
		Tasks: []workflow.TaskDef{
			{ID: "a", MaxAttempts: 2, BackoffMilli: 5},
			{ID: "b", Deps: []string{"a"}, MaxAttempts: 1},
			{ID: "c", Deps: []string{"b"}, MaxAttempts: 1},
			{ID: "d", Deps: []string{"a"}, MaxAttempts: 1},
		},
	}
	h.register(def)

	var aAttempts int32
	h.eng.RegisterExecutor("wf", "a", func(ctx context.Context, _, _ string, attempt int) error {
		atomic.AddInt32(&aAttempts, 1)
		return errors.New("boom")
	})
	bStarted := int32(0)
	cStarted := int32(0)
	dStarted := int32(0)
	h.eng.RegisterExecutor("wf", "b", func(ctx context.Context, _, _ string, _ int) error {
		atomic.AddInt32(&bStarted, 1)
		return nil
	})
	h.eng.RegisterExecutor("wf", "c", func(ctx context.Context, _, _ string, _ int) error {
		atomic.StoreInt32(&cStarted, 1)
		return nil
	})
	h.eng.RegisterExecutor("wf", "d", func(ctx context.Context, _, _ string, _ int) error {
		atomic.StoreInt32(&dStarted, 1)
		return nil
	})

	res := h.trigger("wf", workflow.TriggerEvent)
	run := h.mustRun(res, workflow.RunFailed, workflow.ReasonTaskFailed)

	if got := atomic.LoadInt32(&aAttempts); got != 2 {
		t.Fatalf("a attempts = %d, want 2", got)
	}
	if bStarted != 0 || cStarted != 0 || dStarted != 0 {
		t.Fatalf("downstream executed: b=%d c=%d d=%d, want all 0", bStarted, cStarted, dStarted)
	}
	states := map[string]workflow.TaskState{}
	for _, tr := range run.Tasks {
		states[tr.TaskID] = tr.State
	}
	if states["a"] != workflow.TaskFailed {
		t.Fatalf("a state = %s, want failed", states["a"])
	}
	for _, id := range []string{"b", "c", "d"} {
		if states[id] != workflow.TaskSkipped {
			t.Fatalf("%s state = %s, want skipped (blocked downstream)", id, states[id])
		}
	}
	if run.Message == "" {
		t.Fatal("failed run should carry a message")
	}
	t.Logf("run=%s source=event failed; a=failed(attempts=2), b/c/d=skipped, no dangling states", res.RunID)
	for _, line := range h.auditLines(res.RunID) {
		t.Log("  ", line)
	}
}

func TestCancelStopsRunExplainably(t *testing.T) {
	h := newHarness(t)
	def := workflow.Definition{
		ID: "wf", Concurrency: workflow.ConcurrencySkip,
		Tasks: []workflow.TaskDef{
			{ID: "a", MaxAttempts: 1},
			{ID: "b", Deps: []string{"a"}, MaxAttempts: 3, BackoffMilli: 60000},
			{ID: "c", Deps: []string{"b"}, MaxAttempts: 1},
		},
	}
	h.register(def)
	h.eng.RegisterExecutor("wf", "a", func(ctx context.Context, _, _ string, _ int) error { return nil })
	var bAttempts int32
	h.eng.RegisterExecutor("wf", "b", func(ctx context.Context, _, _ string, attempt int) error {
		atomic.AddInt32(&bAttempts, 1)
		<-ctx.Done() // first attempt keeps running until cancel
		return ctx.Err()
	})
	h.eng.RegisterExecutor("wf", "c", func(ctx context.Context, _, _ string, _ int) error {
		t.Fatal("c must never start after cancellation")
		return nil
	})

	// Trigger without waiting, find active run, cancel it.
	go func() {
		_, _ = h.eng.Trigger(context.Background(), "wf", workflow.TriggerManual)
	}()
	var runID string
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if r, ok, _ := h.st.ActiveRun(context.Background(), "wf"); ok {
			runID = r.ID
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if runID == "" {
		t.Fatal("active run never appeared")
	}
	time.Sleep(50 * time.Millisecond) // let b enter running
	if err := h.eng.Cancel(runID); err != nil {
		t.Fatal(err)
	}
	// Wait for terminal.
	var final workflow.Run
	for i := 0; i < 200; i++ {
		final, _, _ = h.st.GetRun(context.Background(), runID)
		if workflow.IsTerminal(final.State) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if final.State != workflow.RunCanceled || final.Reason != workflow.ReasonCanceled {
		t.Fatalf("final state=%s reason=%s, want canceled/canceled", final.State, final.Reason)
	}
	states := map[string]workflow.TaskRun{}
	for _, tr := range final.Tasks {
		states[tr.TaskID] = tr
	}
	if states["a"].State != workflow.TaskSucceeded {
		t.Fatalf("a = %s, want succeeded (completed before cancel)", states["a"].State)
	}
	if states["b"].State != workflow.TaskCanceled {
		t.Fatalf("b = %s, want canceled", states["b"].State)
	}
	if states["b"].Attempts != 1 {
		t.Fatalf("b attempts = %d, must not retry after cancel", states["b"].Attempts)
	}
	if states["c"].State != workflow.TaskCanceled {
		t.Fatalf("c = %s, want canceled (never started)", states["c"].State)
	}
	t.Logf("run=%s canceled: a=succeeded, b=canceled(attempts=1), c=canceled", runID)
	for _, line := range h.auditLines(runID) {
		t.Log("  ", line)
	}
}

func TestRunTimeoutMarkedTimedOut(t *testing.T) {
	h := newHarness(t)
	def := workflow.Definition{
		ID: "wf", Concurrency: workflow.ConcurrencySkip, RunTimeoutMilli: 80,
		Tasks: []workflow.TaskDef{
			{ID: "slow", MaxAttempts: 1},
		},
	}
	h.register(def)
	h.eng.RegisterExecutor("wf", "slow", blockOnCancel(2*time.Second))
	res := h.trigger("wf", workflow.TriggerTimer)
	run := h.mustRun(res, workflow.RunTimedOut, workflow.ReasonTimeout)
	st := run.Tasks[0]
	if st.State != workflow.TaskCanceled {
		t.Fatalf("task state = %s, want canceled after run timeout", st.State)
	}
	t.Logf("run=%s source=timer timed out consistently with cancel semantics; task=%s", res.RunID, st.State)
}
