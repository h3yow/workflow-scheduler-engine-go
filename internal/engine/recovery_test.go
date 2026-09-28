package engine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/workflow-scheduler-engine-go/internal/store"
	"github.com/h3yow/workflow-scheduler-engine-go/internal/workflow"
)

// reopen simulates a process restart: same snapshot path, fresh engine.
func reopen(t *testing.T, h *testHarness) *testHarness {
	t.Helper()
	_ = h.eng.Close()
	path := h.st.Path()
	st, err := store.NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	h2 := &testHarness{t: t, st: st, eng: New(st), logs: h.logs}
	if err := h2.eng.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h2.eng.Close() })
	return h2
}

func TestRestartRetriesInterruptedRunningTask(t *testing.T) {
	h := newHarness(t)
	def := workflow.Definition{
		ID: "wf", Concurrency: workflow.ConcurrencySkip,
		Tasks: []workflow.TaskDef{
			{ID: "a", MaxAttempts: 3, BackoffMilli: 1},
			{ID: "b", Deps: []string{"a"}, MaxAttempts: 1},
		},
	}
	h.register(def)

	// Persist the exact state of a crash: a was RUNNING with 1 attempt used;
	// b still pending; run running.
	crashed := workflow.Run{
		ID: "crash-1", WorkflowID: "wf", Source: workflow.TriggerManual,
		State:     workflow.RunRunning,
		CreatedAt: time.Now().Add(-time.Minute),
		UpdatedAt: time.Now(),
		Tasks: []workflow.TaskRun{
			{TaskID: "a", State: workflow.TaskRunning, Attempts: 1, UpdatedAt: time.Now()},
			{TaskID: "b", State: workflow.TaskPending, Attempts: 0, UpdatedAt: time.Now()},
		},
	}
	if err := h.st.CreateRun(context.Background(), crashed); err != nil {
		t.Fatal(err)
	}

	var attempts int32
	h2 := reopen(t, h)
	h2.eng.RegisterExecutor("wf", "a", func(ctx context.Context, _, _ string, attempt int) error {
		atomic.AddInt32(&attempts, 1)
		// Must resume at attempt 2: interrupted execution counts as attempt 1.
		if attempt != 2 {
			t.Errorf("resume attempt = %d, want 2", attempt)
		}
		return nil
	})
	h2.eng.RegisterExecutor("wf", "b", func(ctx context.Context, _, _ string, attempt int) error { return nil })

	var final workflow.Run
	for i := 0; i < 200; i++ {
		final, _, _ = h2.st.GetRun(context.Background(), "crash-1")
		if workflow.IsTerminal(final.State) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if final.State != workflow.RunSucceeded {
		t.Fatalf("recovered run state=%s reason=%s msg=%s", final.State, final.Reason, final.Message)
	}
	a := final.Tasks[0]
	if a.Attempts != 2 {
		t.Fatalf("a attempts = %d, want 2 total (1 before crash + 1 after)", a.Attempts)
	}
	if got := atomic.LoadInt32(&attempts); got != 1 {
		t.Fatalf("a executed %d times after restart, want exactly 1", got)
	}
	t.Logf("recovered run=%s: interrupted execution counted, retried once, attempts=2, b ran once", final.ID)
	for _, line := range h2.auditLines(final.ID) {
		t.Log("  ", line)
	}
}

func TestRestartFailsInterruptedTaskWithoutBudget(t *testing.T) {
	h := newHarness(t)
	def := workflow.Definition{
		ID: "wf", Concurrency: workflow.ConcurrencySkip,
		Tasks: []workflow.TaskDef{
			{ID: "a", MaxAttempts: 1},
			{ID: "b", Deps: []string{"a"}, MaxAttempts: 1},
		},
	}
	h.register(def)
	crashed := workflow.Run{
		ID: "crash-2", WorkflowID: "wf", Source: workflow.TriggerEvent,
		State: workflow.RunRunning, CreatedAt: time.Now().Add(-time.Minute), UpdatedAt: time.Now(),
		Tasks: []workflow.TaskRun{
			{TaskID: "a", State: workflow.TaskRunning, Attempts: 1, UpdatedAt: time.Now()},
			{TaskID: "b", State: workflow.TaskPending, Attempts: 0, UpdatedAt: time.Now()},
		},
	}
	if err := h.st.CreateRun(context.Background(), crashed); err != nil {
		t.Fatal(err)
	}

	h2 := reopen(t, h)
	started := int32(0)
	h2.eng.RegisterExecutor("wf", "a", func(ctx context.Context, _, _ string, _ int) error {
		atomic.AddInt32(&started, 1)
		return nil
	})
	h2.eng.RegisterExecutor("wf", "b", func(ctx context.Context, _, _ string, _ int) error {
		t.Fatal("b must stay blocked after failed recovery of a")
		return nil
	})

	var final workflow.Run
	for i := 0; i < 200; i++ {
		final, _, _ = h2.st.GetRun(context.Background(), "crash-2")
		if workflow.IsTerminal(final.State) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if final.State != workflow.RunFailed {
		t.Fatalf("state=%s, want failed", final.State)
	}
	if final.Tasks[0].State != workflow.TaskFailed || final.Tasks[1].State != workflow.TaskSkipped {
		t.Fatalf("tasks = %s/%s, want failed/skipped", final.Tasks[0].State, final.Tasks[1].State)
	}
	if started != 0 {
		t.Fatalf("a re-executed %d times despite exhausted budget", started)
	}
	t.Logf("run=%s source=event terminated deterministically on restart: a=failed b=skipped, no dangling state", final.ID)
}

func TestRestartResumesQueuedRunAfterActiveOne(t *testing.T) {
	h := newHarness(t)
	def := workflow.Definition{
		ID: "wf", Concurrency: workflow.ConcurrencyQueue,
		Tasks: []workflow.TaskDef{{ID: "a", MaxAttempts: 1}},
	}
	h.register(def)
	now := time.Now()
	active := workflow.Run{
		ID: "crash-active", WorkflowID: "wf", Source: workflow.TriggerTimer,
		State: workflow.RunRunning, CreatedAt: now.Add(-2 * time.Minute), UpdatedAt: now,
		Tasks: []workflow.TaskRun{{TaskID: "a", State: workflow.TaskPending, Attempts: 0, UpdatedAt: now}},
	}
	queued := workflow.Run{
		ID: "crash-queued", WorkflowID: "wf", Source: workflow.TriggerManual,
		State: workflow.RunQueued, CreatedAt: now.Add(-time.Minute), UpdatedAt: now,
		Tasks: []workflow.TaskRun{{TaskID: "a", State: workflow.TaskPending, Attempts: 0, UpdatedAt: now}},
	}
	if err := h.st.CreateRun(context.Background(), active); err != nil {
		t.Fatal(err)
	}
	if err := h.st.CreateRun(context.Background(), queued); err != nil {
		t.Fatal(err)
	}

	h2 := reopen(t, h)
	var runs int32
	h2.eng.RegisterExecutor("wf", "a", func(ctx context.Context, _, _ string, _ int) error {
		atomic.AddInt32(&runs, 1)
		time.Sleep(20 * time.Millisecond)
		return nil
	})

	terminal := map[string]workflow.RunState{}
	for i := 0; i < 300; i++ {
		terminal = map[string]workflow.RunState{}
		for _, id := range []string{"crash-active", "crash-queued"} {
			r, _, _ := h2.st.GetRun(context.Background(), id)
			terminal[id] = r.State
		}
		if workflow.IsTerminal(terminal["crash-active"]) && workflow.IsTerminal(terminal["crash-queued"]) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if terminal["crash-active"] != workflow.RunSucceeded || terminal["crash-queued"] != workflow.RunSucceeded {
		t.Fatalf("states after recovery = %v, want both succeeded", terminal)
	}
	if got := atomic.LoadInt32(&runs); got != 2 {
		t.Fatalf("task executions = %d, want 2 (one per run)", got)
	}
	t.Log("queue reconstructed on restart: active resumed, queued run ran after it, exactly 2 executions")
}
