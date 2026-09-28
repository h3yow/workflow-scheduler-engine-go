package workflow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// logSink prints trigger source / run id / state transitions into the test log,
// as required by the task: every unit test shows the audit trail.
func logSink(t *testing.T) func(Event) {
	t.Helper()
	return func(ev Event) {
		target := string(ev.RunID)
		if ev.TaskID != "" {
			target += "/" + ev.TaskID
		}
		t.Logf("[%s] %-7s %-22s %-9s -> %-9s reason=%-18s %s",
			ev.Time.Format("15:04:05.000"), ev.Trigger, short(target),
			ev.FromState, ev.ToState, ev.Reason, ev.Message)
	}
}

func short(s string) string {
	if len(s) > 22 {
		return s[:19] + "..."
	}
	return s
}

func newTestEngine(t *testing.T, opts ...Option) (*Engine, *MemoryStore) {
	t.Helper()
	st := NewMemoryStore()
	opts = append([]Option{WithEventLogger(logSink(t))}, opts...)
	eng, err := NewEngine(st, NewRegistry(), opts...)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	return eng, st
}

func TestDAGSuccessOrder(t *testing.T) {
	eng, _ := newTestEngine(t)
	var mu sync.Mutex
	var order []string
	for _, id := range []string{"a", "b", "c", "d"} {
		id := id
		eng.registry.Register(id, func(ctx context.Context, tc TaskContext) (string, error) {
			mu.Lock()
			order = append(order, id)
			mu.Unlock()
			return id + "-ok", nil
		})
	}
	def := WorkflowDef{
		Name: "dag",
		Tasks: []TaskDef{
			{ID: "a"},
			{ID: "b", Dependencies: []string{"a"}},
			{ID: "c", Dependencies: []string{"a"}},
			{ID: "d", Dependencies: []string{"b", "c"}},
		},
		TriggerConcurrency: ConcurrencyReject,
	}
	if err := eng.Register(def); err != nil {
		t.Fatalf("register: %v", err)
	}
	run, err := eng.Trigger(context.Background(), "dag", TriggerManual)
	if err != nil {
		t.Fatalf("trigger: %v", err)
	}
	done, err := eng.WaitRun(ctxWith(time.Second*5), run.ID)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if done.State != RunSucceeded {
		t.Fatalf("run state=%s reason=%s msg=%s", done.State, done.Reason, done.Message)
	}
	mu.Lock()
	defer mu.Unlock()
	idx := map[string]int{}
	for i, id := range order {
		idx[id] = i
	}
	if !(idx["a"] < idx["b"] && idx["a"] < idx["c"] && idx["b"] < idx["d"] && idx["c"] < idx["d"]) {
		t.Fatalf("dependency order violated: %v", order)
	}
	t.Logf("execution order: %v", order)
}

func TestRetryThenFailAndSkipDownstream(t *testing.T) {
	eng, _ := newTestEngine(t)
	var flakyAttempts int32
	eng.registry.Register("ok", func(ctx context.Context, tc TaskContext) (string, error) {
		return "ok", nil
	})
	eng.registry.Register("flaky", func(ctx context.Context, tc TaskContext) (string, error) {
		atomic.AddInt32(&flakyAttempts, 1)
		return "", NewTaskError("DENIED", "always fails")
	})
	eng.registry.Register("down", func(ctx context.Context, tc TaskContext) (string, error) {
		return "should-not-run", nil
	})
	def := WorkflowDef{
		Name: "retry",
		Tasks: []TaskDef{
			{ID: "ok"},
			{ID: "flaky", Dependencies: []string{"ok"}, Retries: RetryPolicy{MaxAttempts: 3}},
			{ID: "down", Dependencies: []string{"flaky"}},
		},
		TriggerConcurrency: ConcurrencyReject,
	}
	if err := eng.Register(def); err != nil {
		t.Fatal(err)
	}
	run, err := eng.Trigger(context.Background(), "retry", TriggerManual)
	if err != nil {
		t.Fatal(err)
	}
	final, err := eng.WaitRun(ctxWith(5*time.Second), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != RunFailed {
		t.Fatalf("want FAILED, got %s: %s", final.State, final.Message)
	}
	if final.Reason != "DENIED" {
		t.Fatalf("reason should carry handler code DENIED, got %s", final.Reason)
	}
	if got := atomic.LoadInt32(&flakyAttempts); got != 3 {
		t.Fatalf("flaky must execute exactly MaxAttempts=3 times, got %d", got)
	}
	flaky, _ := eng.store.GetTaskByRef(run.ID, "flaky")
	if flaky.State != TaskFailed || flaky.Attempt != 3 {
		t.Fatalf("flaky record: state=%s attempt=%d", flaky.State, flaky.Attempt)
	}
	down, _ := eng.store.GetTaskByRef(run.ID, "down")
	if down.State != TaskSkipped || down.ErrCode != ReasonUpstreamFailed {
		t.Fatalf("downstream must be SKIPPED/UPSTREAM_FAILED, got %s/%s", down.State, down.ErrCode)
	}
}

func TestRetrySucceedsEventually(t *testing.T) {
	eng, _ := newTestEngine(t)
	var n int32
	eng.registry.Register("t", func(ctx context.Context, tc TaskContext) (string, error) {
		if atomic.AddInt32(&n, 1) < 3 {
			return "", NewTaskError("TRANSIENT", "boom")
		}
		return "recovered", nil
	})
	def := WorkflowDef{
		Name: "w",
		Tasks: []TaskDef{{
			ID:      "t",
			Retries: RetryPolicy{MaxAttempts: 5, InitialBackoff: time.Millisecond},
		}},
		TriggerConcurrency: ConcurrencyReject,
	}
	if err := eng.Register(def); err != nil {
		t.Fatal(err)
	}
	run, _ := eng.Trigger(context.Background(), "w", TriggerEvent)
	final, err := eng.WaitRun(ctxWith(5*time.Second), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != RunSucceeded {
		t.Fatalf("want SUCCEEDED, got %s: %s", final.State, final.Message)
	}
	tr, _ := eng.store.GetTaskByRef(run.ID, "t")
	if tr.Attempt != 3 || tr.Output != "recovered" {
		t.Fatalf("task record: attempt=%d output=%q", tr.Attempt, tr.Output)
	}
}

func TestConcurrentTriggersDoNotDuplicate(t *testing.T) {
	eng, _ := newTestEngine(t)
	var execs int32
	eng.registry.Register("slow", func(ctx context.Context, tc TaskContext) (string, error) {
		atomic.AddInt32(&execs, 1)
		select {
		case <-time.After(80 * time.Millisecond):
			return "done", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
	def := WorkflowDef{
		Name:  "single",
		Tasks: []TaskDef{{ID: "slow"}},
		// default reject
		TriggerConcurrency: ConcurrencyReject,
	}
	if err := eng.Register(def); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	rejected := int32(0)
	var firstRun RunID
	var once sync.Once
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := eng.Trigger(context.Background(), "single", TriggerManual)
			if err != nil {
				if errors.Is(err, ErrRunActive) {
					atomic.AddInt32(&rejected, 1)
				}
				return
			}
			once.Do(func() { firstRun = r.ID })
		}()
	}
	wg.Wait()

	first, err := eng.store.GetRun(firstRun)
	if err != nil {
		t.Fatal(err)
	}
	final, err := eng.WaitRun(ctxWith(5*time.Second), first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != RunSucceeded {
		t.Fatalf("final=%s %s", final.State, final.Message)
	}
	if got := atomic.LoadInt32(&execs); got != 1 {
		t.Fatalf("task must run exactly once across 20 concurrent triggers, got %d", got)
	}
	runs, _ := eng.store.ListRuns("single")
	if len(runs) != 1 {
		t.Fatalf("exactly one run expected, got %d", len(runs))
	}
	if rejected != 19 {
		t.Fatalf("19 triggers should be rejected as ALREADY_TRIGGERED, got %d", rejected)
	}
}

func TestValidationCycleAndMissingDep(t *testing.T) {
	eng, _ := newTestEngine(t)
	cyc := WorkflowDef{
		Name: "cyc",
		Tasks: []TaskDef{
			{ID: "a", Dependencies: []string{"b"}},
			{ID: "b", Dependencies: []string{"a"}},
		},
	}
	err := eng.Register(cyc)
	if !IsValidation(err) || err.(*ValidationError).Reason != ReasonCycleDetected {
		t.Fatalf("want CYCLE_DETECTED, got %v", err)
	}
	t.Logf("cycle rejection: %v", err)

	miss := WorkflowDef{
		Name: "miss",
		Tasks: []TaskDef{
			{ID: "a", Dependencies: []string{"ghost"}},
		},
	}
	err = eng.Register(miss)
	if !IsValidation(err) || err.(*ValidationError).Reason != ReasonMissingDependency {
		t.Fatalf("want MISSING_DEPENDENCY, got %v", err)
	}
	t.Logf("missing-dep rejection: %v", err)

	runs, _ := eng.store.ListRuns("cyc")
	if len(runs) != 0 {
		t.Fatal("invalid workflow must never create runs")
	}
}

func ctxWith(d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	_ = cancel
	return ctx
}

var _ = fmt.Sprintf
var _ = strings.Builder{}
