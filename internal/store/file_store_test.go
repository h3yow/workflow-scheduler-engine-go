package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/h3yow/workflow-scheduler-engine-go/internal/workflow"
)

func TestFileStoreRoundTripAndRecoverable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	ctx := context.Background()

	st, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	def := workflow.Definition{ID: "wf", Tasks: []workflow.TaskDef{{ID: "a"}}}
	if err := st.SaveDefinition(ctx, def); err != nil {
		t.Fatal(err)
	}
	base := time.Now()
	r1 := workflow.Run{ID: "r1", WorkflowID: "wf", State: workflow.RunSucceeded, CreatedAt: base}
	r2 := workflow.Run{ID: "r2", WorkflowID: "wf", State: workflow.RunRunning, CreatedAt: base.Add(time.Millisecond)}
	r3 := workflow.Run{ID: "r3", WorkflowID: "wf", State: workflow.RunQueued, CreatedAt: base.Add(2 * time.Millisecond)}
	if err := st.CreateRun(ctx, r1); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateRun(ctx, r2); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateRun(ctx, r3); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendAudit(ctx, workflow.AuditEvent{RunID: "r2", Event: "run.start"}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: simulate process restart.
	st2, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	gotDef, ok, _ := st2.GetDefinition(ctx, "wf")
	if !ok || gotDef.ID != "wf" {
		t.Fatalf("definition lost: %+v ok=%v", gotDef, ok)
	}
	rec, err := st2.ListRecoverable(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rec) != 2 || rec[0].ID != "r2" || rec[1].ID != "r3" {
		t.Fatalf("recoverable runs = %v, want [r2 r3]", rec)
	}
	active, ok, _ := st2.ActiveRun(ctx, "wf")
	if !ok || active.ID != "r2" {
		t.Fatalf("active = %v ok=%v, want r2", active.ID, ok)
	}
	audit, _ := st2.ListAudit(ctx, "r2")
	if len(audit) != 1 || audit[0].Event != "run.start" {
		t.Fatalf("audit round trip failed: %+v", audit)
	}
	t.Log("snapshot survived restart; recoverable=[r2 r3], audit intact")
}

func TestInMemoryStoreWithoutPath(t *testing.T) {
	ctx := context.Background()
	st, _ := NewFileStore("")
	if err := st.CreateRun(ctx, workflow.Run{ID: "x", State: workflow.RunQueued}); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := st.GetRun(ctx, "x"); !ok {
		t.Fatal("run not found in memory store")
	}
}
