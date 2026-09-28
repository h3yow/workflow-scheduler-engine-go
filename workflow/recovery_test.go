package workflow

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// TestRecoveryResumeContinuesRun kills a real child process (SIGKILL) while a
// task is RUNNING, then opens the store with a new engine under resume policy
// and verifies the run continues without resetting attempt counters.
func TestRecoveryResumeContinuesRun(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	runID := crashInSubprocess(t, dir)

	// Inspect the post-crash on-disk intermediate state with a fresh store.
	st0, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	r, _ := st0.GetRun(runID)
	if r.State.IsTerminal() {
		t.Fatalf("run should be non-terminal on disk, got %s", r.State)
	}
	bt, _ := st0.GetTaskByRef(runID, "b")
	if bt.State != TaskRunning || bt.Attempt != 1 {
		t.Fatalf("b must be persisted RUNNING/attempt=1, got %s/%d", bt.State, bt.Attempt)
	}
	ct, _ := st0.GetTaskByRef(runID, "c")
	if ct.State != TaskPending {
		t.Fatalf("c must remain PENDING, got %s", ct.State)
	}
	if err := st0.Close(); err != nil {
		t.Fatal(err)
	}

	// New "process": resume. Re-register handlers; b/c now finish normally.
	var cRan int32
	reg := NewRegistry()
	reg.Register("a", func(ctx context.Context, tc TaskContext) (string, error) { return "a", nil })
	reg.Register("b", func(ctx context.Context, tc TaskContext) (string, error) { return "b-resumed", nil })
	reg.Register("c", func(ctx context.Context, tc TaskContext) (string, error) {
		atomic.AddInt32(&cRan, 1)
		return "c", nil
	})
	st, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	eng, err := NewEngine(st, reg, WithRecovery(RecoveryResume), WithEventLogger(logSink(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = eng.Close() }()

	final, err := eng.WaitRun(ctxWith(5*time.Second), runID)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != RunSucceeded {
		t.Fatalf("resumed run must SUCCEED, got %s/%s: %s", final.State, final.Reason, final.Message)
	}
	bt2, _ := st.GetTaskByRef(runID, "b")
	if bt2.State != TaskSucceeded {
		t.Fatalf("b should be SUCCEEDED after resume, got %s", bt2.State)
	}
	if bt2.Attempt != 2 {
		t.Fatalf("attempt counter must not roll back: want 2 (crash attempt + resume attempt), got %d", bt2.Attempt)
	}
	ct2, _ := st.GetTaskByRef(runID, "c")
	if ct2.State != TaskSucceeded || atomic.LoadInt32(&cRan) != 1 {
		t.Fatalf("c must execute exactly once after resume, state=%s ran=%d", ct2.State, atomic.LoadInt32(&cRan))
	}
	t.Logf("resume OK: b.Attempt=%d (never reset), c ran exactly once, run SUCCEEDED", bt2.Attempt)
}

// TestRecoveryTerminateMarksInterrupted kills a child mid-run, then opens the
// store with terminate policy: the run must get an explicit INTERRUPTED end,
// unfinished tasks CANCELED with a distinguishable reason, and a subsequent
// trigger must start a fresh successful run.
func TestRecoveryTerminateMarksInterrupted(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	runID := crashInSubprocess(t, dir)

	reg := NewRegistry()
	reg.Register("a", func(ctx context.Context, tc TaskContext) (string, error) { return "a", nil })
	reg.Register("b", func(ctx context.Context, tc TaskContext) (string, error) { return "b", nil })
	reg.Register("c", func(ctx context.Context, tc TaskContext) (string, error) { return "c", nil })
	st, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	eng, err := NewEngine(st, reg, WithRecovery(RecoveryTerminate), WithEventLogger(logSink(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = eng.Close() }()

	final, err := eng.WaitRun(ctxWith(5*time.Second), runID)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != RunInterrupted || final.Reason != ReasonRunInterrupted {
		t.Fatalf("want INTERRUPTED/RUN_INTERRUPTED, got %s/%s", final.State, final.Reason)
	}
	b, _ := st.GetTaskByRef(runID, "b")
	c, _ := st.GetTaskByRef(runID, "c")
	if b.State != TaskCanceled || c.State != TaskCanceled {
		t.Fatalf("unfinished tasks must be CANCELED, got b=%s c=%s", b.State, c.State)
	}
	if b.ErrCode != ReasonRunInterrupted || c.ErrCode != ReasonRunInterrupted {
		t.Fatalf("cancel reason must distinguish interruption, got b=%s c=%s", b.ErrCode, c.ErrCode)
	}

	run2, err := eng.Trigger(context.Background(), "recover", TriggerManual)
	if err != nil {
		t.Fatalf("trigger after interrupt should succeed: %v", err)
	}
	final2, err := eng.WaitRun(ctxWith(5*time.Second), run2.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final2.State != RunSucceeded {
		t.Fatalf("fresh run after interrupt must SUCCEED, got %s", final2.State)
	}
}
