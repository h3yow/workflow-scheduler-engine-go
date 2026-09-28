package workflow

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestQueuePolicyRunsSequentialRuns(t *testing.T) {
	eng, _ := newTestEngine(t)
	var running int32
	var maxConcurrent int32
	gate := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	eng.registry.Register("job", func(ctx context.Context, tc TaskContext) (string, error) {
		cur := atomic.AddInt32(&running, 1)
		for {
			m := atomic.LoadInt32(&maxConcurrent)
			if cur <= m || atomic.CompareAndSwapInt32(&maxConcurrent, m, cur) {
				break
			}
		}
		<-gate
		atomic.AddInt32(&running, -1)
		return "ok", nil
	})
	def := WorkflowDef{
		Name:               "queue-wf",
		Tasks:              []TaskDef{{ID: "job"}},
		TriggerConcurrency: ConcurrencyQueue,
	}
	if err := eng.Register(def); err != nil {
		t.Fatal(err)
	}

	r1, err := eng.Trigger(context.Background(), "queue-wf", TriggerSchedule)
	if err != nil {
		t.Fatal(err)
	}

	// Two more triggers from different sources: they queue, not reject.
	var r2, r3 Run
	var e2, e3 error
	var done sync.WaitGroup
	done.Add(2)
	go func() { defer done.Done(); r2, e2 = eng.Trigger(context.Background(), "queue-wf", TriggerEvent) }()
	go func() { defer done.Done(); r3, e3 = eng.Trigger(context.Background(), "queue-wf", TriggerManual) }()

	time.Sleep(80 * time.Millisecond) // let triggers park in the queue
	close(gate)
	// unblock first handler; do not let the deferred wg mark test issue
	wg.Done()
	done.Wait()
	if e2 != nil || e3 != nil {
		t.Fatalf("queued triggers must not error: %v %v", e2, e3)
	}

	for _, id := range []RunID{r1.ID, r2.ID, r3.ID} {
		final, err := eng.WaitRun(ctxWith(5*time.Second), id)
		if err != nil {
			t.Fatal(err)
		}
		if final.State != RunSucceeded {
			t.Fatalf("run %s: %s/%s", id, final.State, final.Message)
		}
	}
	runs, _ := eng.store.ListRuns("queue-wf")
	if len(runs) != 3 {
		t.Fatalf("queue policy must produce exactly 3 sequential runs, got %d", len(runs))
	}
	if max := atomic.LoadInt32(&maxConcurrent); max != 1 {
		t.Fatalf("queued runs must never overlap (max concurrency=%d)", max)
	}
}

func TestCancelPolicyReplacesActiveRun(t *testing.T) {
	eng, _ := newTestEngine(t)
	release1 := make(chan struct{})
	var runs1, runs2 int32
	eng.registry.Register("job", func(ctx context.Context, tc TaskContext) (string, error) {
		switch tc.Trigger {
		case TriggerSchedule:
			atomic.AddInt32(&runs1, 1)
			<-release1
			<-ctx.Done()
			return "", ctx.Err()
		default:
			atomic.AddInt32(&runs2, 1)
			return "replacement-done", nil
		}
	})
	def := WorkflowDef{
		Name:               "replace-wf",
		Tasks:              []TaskDef{{ID: "job"}},
		TriggerConcurrency: ConcurrencyCancel,
	}
	if err := eng.Register(def); err != nil {
		t.Fatal(err)
	}
	old, err := eng.Trigger(context.Background(), "replace-wf", TriggerSchedule)
	if err != nil {
		t.Fatal(err)
	}
	waitState(t, eng, old.ID, "job", TaskRunning)

	// Event trigger cancels the scheduled run and starts its own. It blocks
	// until the old run has fully settled, so release the old handler's gate
	// concurrently first.
	var newRun Run
	var trigErr error
	var trigDone sync.WaitGroup
	trigDone.Add(1)
	go func() {
		defer trigDone.Done()
		newRun, trigErr = EmitEvent(context.Background(), eng, "replace-wf")
	}()
	close(release1)
	trigDone.Wait()
	if trigErr != nil {
		t.Fatalf("event trigger under cancel policy: %v", trigErr)
	}
	if newRun.ID == old.ID {
		t.Fatal("cancel policy must start a replacement run, not reuse the old one")
	}

	finalOld, err := eng.WaitRun(ctxWith(5*time.Second), old.ID)
	if err != nil {
		t.Fatal(err)
	}
	if finalOld.State != RunCanceled {
		t.Fatalf("old run must be CANCELED, got %s", finalOld.State)
	}
	finalNew, err := eng.WaitRun(ctxWith(5*time.Second), newRun.ID)
	if err != nil {
		t.Fatal(err)
	}
	if finalNew.State != RunSucceeded || finalNew.Trigger != TriggerEvent {
		t.Fatalf("replacement must succeed with event trigger, got %s trigger=%s",
			finalNew.State, finalNew.Trigger)
	}
}

func TestScheduledAndEventShareSemantics(t *testing.T) {
	eng, _ := newTestEngine(t)
	gate := make(chan struct{})
	eng.registry.Register("blocker", func(ctx context.Context, tc TaskContext) (string, error) {
		<-gate
		<-ctx.Done()
		return "", ctx.Err()
	})
	def := WorkflowDef{
		Name:               "shared",
		Tasks:              []TaskDef{{ID: "blocker"}},
		TriggerConcurrency: ConcurrencyReject,
	}
	if err := eng.Register(def); err != nil {
		t.Fatal(err)
	}

	// A manual run is active. Scheduled and event triggers must be rejected
	// with the SAME error as another manual trigger (single Trigger path).
	active, err := eng.Trigger(context.Background(), "shared", TriggerManual)
	if err != nil {
		t.Fatal(err)
	}
	waitState(t, eng, active.ID, "blocker", TaskRunning)

	_, errManual := eng.Trigger(context.Background(), "shared", TriggerManual)
	_, errSched := eng.Trigger(context.Background(), "shared", TriggerSchedule)
	_, errEvent := EmitEvent(context.Background(), eng, "shared")
	for name, e := range map[string]error{
		"manual": errManual, "scheduled": errSched, "event": errEvent,
	} {
		if !errors.Is(e, ErrRunActive) {
			t.Fatalf("%s trigger: want ErrRunActive, got %v", name, e)
		}
	}
	t.Logf("manual/schedule/event all rejected identically as ErrRunActive for run %s", active.ID)

	if err := eng.Cancel(active.ID, ReasonCanceled); err != nil {
		t.Fatal(err)
	}
	close(gate)
	if _, err := eng.WaitRun(ctxWith(5*time.Second), active.ID); err != nil {
		t.Fatal(err)
	}
}
