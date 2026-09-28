package workflow

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCancelSettlesRun(t *testing.T) {
	eng, _ := newTestEngine(t)

	var started int32
	release := make(chan struct{})
	eng.registry.Register("a", func(ctx context.Context, tc TaskContext) (string, error) {
		atomic.AddInt32(&started, 1)
		<-release // block until canceled
		<-ctx.Done()
		return "", ctx.Err()
	})
	eng.registry.Register("b", func(ctx context.Context, tc TaskContext) (string, error) {
		atomic.AddInt32(&started, 1)
		return "b-ran", nil
	})
	eng.registry.Register("c", func(ctx context.Context, tc TaskContext) (string, error) {
		atomic.AddInt32(&started, 1)
		return "c-ran", nil
	})

	def := WorkflowDef{
		Name: "cancel-wf",
		Tasks: []TaskDef{
			{ID: "a"},
			{ID: "b", Dependencies: []string{"a"}},
			{ID: "c", Dependencies: []string{"a"}},
		},
		TriggerConcurrency: ConcurrencyReject,
	}
	if err := eng.Register(def); err != nil {
		t.Fatal(err)
	}
	run, err := eng.Trigger(context.Background(), "cancel-wf", TriggerManual)
	if err != nil {
		t.Fatal(err)
	}

	// wait until "a" is running
	deadline := time.Now().Add(2 * time.Second)
	for {
		tr, _ := eng.store.GetTaskByRef(run.ID, "a")
		if tr.State == TaskRunning {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("task a never started")
		}
		time.Sleep(time.Millisecond * 5)
	}

	if err := eng.Cancel(run.ID, ReasonCanceled); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	close(release)

	final, err := eng.WaitRun(ctxWith(5*time.Second), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != RunCanceled || final.Reason != ReasonCanceled {
		t.Fatalf("want CANCELED/CANCELED, got %s/%s", final.State, final.Reason)
	}

	a, _ := eng.store.GetTaskByRef(run.ID, "a")
	b, _ := eng.store.GetTaskByRef(run.ID, "b")
	c, _ := eng.store.GetTaskByRef(run.ID, "c")
	if a.State != TaskCanceled {
		t.Fatalf("running task must end CANCELED, got %s (%s)", a.State, a.ErrMsg)
	}
	if b.State != TaskCanceled || c.State != TaskCanceled {
		t.Fatalf("not-started tasks must be CANCELED, got b=%s c=%s", b.State, c.State)
	}
	if got := atomic.LoadInt32(&started); got != 1 {
		t.Fatalf("only 'a' may execute after cancel, got %d started", got)
	}
}

func TestCancelKeepsHandlerSuccess(t *testing.T) {
	eng, _ := newTestEngine(t)
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	eng.registry.Register("race", func(ctx context.Context, tc TaskContext) (string, error) {
		defer wg.Done()
		<-release
		return "completed", nil // succeeds even though cancel was requested
	})
	def := WorkflowDef{
		Name:               "race-wf",
		Tasks:              []TaskDef{{ID: "race"}},
		TriggerConcurrency: ConcurrencyReject,
	}
	if err := eng.Register(def); err != nil {
		t.Fatal(err)
	}
	run, _ := eng.Trigger(context.Background(), "race-wf", TriggerEvent)
	// ensure running, then cancel, then release handler
	waitState(t, eng, run.ID, "race", TaskRunning)
	if err := eng.Cancel(run.ID, ReasonCanceled); err != nil {
		t.Fatal(err)
	}
	close(release)
	final, err := eng.WaitRun(ctxWith(5*time.Second), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != RunCanceled {
		t.Fatalf("run still CANCELED, got %s", final.State)
	}
	tr, _ := eng.store.GetTaskByRef(run.ID, "race")
	if tr.State != TaskSucceeded || tr.Output != "completed" {
		t.Fatalf("handler's own success must be preserved, got %s output=%q msg=%s",
			tr.State, tr.Output, tr.ErrMsg)
	}
}

func TestRunTimeoutFailsRun(t *testing.T) {
	eng, _ := newTestEngine(t)
	eng.registry.Register("slow", func(ctx context.Context, tc TaskContext) (string, error) {
		select {
		case <-time.After(2 * time.Second):
			return "late", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
	def := WorkflowDef{
		Name:               "timeout-wf",
		Tasks:              []TaskDef{{ID: "slow"}},
		Timeout:            120 * time.Millisecond,
		TriggerConcurrency: ConcurrencyReject,
	}
	if err := eng.Register(def); err != nil {
		t.Fatal(err)
	}
	run, _ := eng.Trigger(context.Background(), "timeout-wf", TriggerSchedule)
	final, err := eng.WaitRun(ctxWith(5*time.Second), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != RunFailed || final.Reason != ReasonTimeout {
		t.Fatalf("want FAILED/TIMEOUT, got %s/%s (%s)", final.State, final.Reason, final.Message)
	}
}

func waitState(t *testing.T, eng *Engine, runID RunID, taskID string, want TaskState) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		tr, err := eng.store.GetTaskByRef(runID, taskID)
		if err == nil && tr.State == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("task %s never reached %s (last=%v)", taskID, want, err)
		}
		time.Sleep(3 * time.Millisecond)
	}
}
