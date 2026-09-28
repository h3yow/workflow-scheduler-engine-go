package scheduler

import (
	"context"
	"errors"
	"testing"

	"github.com/h3yow/workflow-scheduler-engine-go/internal/workflow"
)

func TestValidateRejectsBadDAGs(t *testing.T) {
	cases := []struct {
		name   string
		def    workflow.Definition
		reason workflow.Reason
	}{
		{
			name: "missing dependency",
			def: workflow.Definition{ID: "wf", Tasks: []workflow.TaskDef{
				{ID: "a", Deps: []string{"ghost"}},
			}},
			reason: workflow.ReasonDAGMissingDep,
		},
		{
			name: "self cycle",
			def: workflow.Definition{ID: "wf", Tasks: []workflow.TaskDef{
				{ID: "a", Deps: []string{"a"}},
			}},
			reason: workflow.ReasonDAGCycle,
		},
		{
			name: "multi node cycle",
			def: workflow.Definition{ID: "wf", Tasks: []workflow.TaskDef{
				{ID: "a", Deps: []string{"c"}},
				{ID: "b", Deps: []string{"a"}},
				{ID: "c", Deps: []string{"b"}},
			}},
			reason: workflow.ReasonDAGCycle,
		},
		{
			name: "duplicate task id",
			def: workflow.Definition{ID: "wf", Tasks: []workflow.TaskDef{
				{ID: "a"}, {ID: "a"},
			}},
			reason: workflow.ReasonDAGCycle,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(tc.def)
			if err == nil {
				t.Fatalf("expected rejection, got nil")
			}
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("expected *ValidationError, got %T: %v", err, err)
			}
			if ve.Reason != tc.reason {
				t.Fatalf("reason = %q, want %q (msg=%s)", ve.Reason, tc.reason, ve.Message)
			}
			t.Logf("rejected as expected: reason=%s detail=%s", ve.Reason, ve.Message)
		})
	}
}

func TestReadyFollowsDependencies(t *testing.T) {
	def := workflow.Definition{ID: "wf", Tasks: []workflow.TaskDef{
		{ID: "a"},
		{ID: "b", Deps: []string{"a"}},
		{ID: "c", Deps: []string{"a"}},
		{ID: "d", Deps: []string{"b", "c"}},
	}}
	sch, err := New(def)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	ready := sch.Ready(ctx)
	if len(ready) != 1 || ready[0] != "a" {
		t.Fatalf("initial ready = %v, want [a]", ready)
	}
	sch.Mark("a", workflow.TaskSucceeded, "")

	ready = sch.Ready(ctx)
	if len(ready) != 2 || ready[0] != "b" || ready[1] != "c" {
		t.Fatalf("after a: ready = %v, want [b c]", ready)
	}
	sch.Mark("b", workflow.TaskSucceeded, "")
	// c still pending: d must not be ready yet.
	if got := sch.Ready(ctx); len(got) != 1 || got[0] != "c" {
		t.Fatalf("ready = %v, want [c]", got)
	}
	sch.Mark("c", workflow.TaskSucceeded, "")
	if got := sch.Ready(ctx); len(got) != 1 || got[0] != "d" {
		t.Fatalf("ready = %v, want [d]", got)
	}
	sch.Mark("d", workflow.TaskSucceeded, "")
	done, st, _ := sch.RunFinished(ctx)
	if !done || st != workflow.RunSucceeded {
		t.Fatalf("done=%v state=%s, want succeeded", done, st)
	}
}

func TestTerminalStateNeverRegresses(t *testing.T) {
	def := workflow.Definition{ID: "wf", Tasks: []workflow.TaskDef{{ID: "a"}}}
	sch, _ := New(def)
	sch.Mark("a", workflow.TaskSucceeded, "")
	if _, ok := sch.Mark("a", workflow.TaskPending, ""); ok {
		t.Fatal("succeeded task must not regress to pending")
	}
	tr, _ := sch.Task("a")
	if tr.State != workflow.TaskSucceeded {
		t.Fatalf("state mutated to %s", tr.State)
	}
}
