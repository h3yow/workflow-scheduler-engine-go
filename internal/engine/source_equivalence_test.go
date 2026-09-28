package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/workflow-scheduler-engine-go/internal/workflow"
)

// Every trigger source goes through Engine.Trigger; each fresh workflow run
// must produce identical task outcomes regardless of source.
func TestAllTriggerSourcesProduceEquivalentRuns(t *testing.T) {
	sources := []workflow.TriggerSource{workflow.TriggerManual, workflow.TriggerTimer, workflow.TriggerEvent}
	type summary struct {
		state  workflow.RunState
		states []workflow.TaskState
		starts int64
	}
	summaries := make([]summary, len(sources))

	for i, src := range sources {
		h := newHarness(t)
		wfID := "wf-eq-" + src.String()
		def := workflow.Definition{
			ID: wfID, Concurrency: workflow.ConcurrencySkip,
			Tasks: []workflow.TaskDef{
				{ID: "a", MaxAttempts: 2, BackoffMilli: 1},
				{ID: "b", Deps: []string{"a"}, MaxAttempts: 1},
			},
		}
		h.register(def)
		var starts int64
		h.eng.RegisterExecutor(wfID, "a", func(ctx context.Context, _, _ string, attempt int) error {
			atomic.AddInt64(&starts, 1)
			if attempt == 1 {
				return errors.New("first-attempt transient failure")
			}
			return nil
		})
		h.eng.RegisterExecutor(wfID, "b", func(ctx context.Context, _, _ string, _ int) error { return nil })

		res := h.trigger(wfID, src)
		run := h.mustRun(res, workflow.RunSucceeded, workflow.ReasonNone)
		if res.RunID == "" {
			t.Fatalf("%s: missing run id", src)
		}
		taskStates := make([]workflow.TaskState, len(run.Tasks))
		attempts := make([]int, len(run.Tasks))
		for j, tr := range run.Tasks {
			taskStates[j] = tr.State
			attempts[j] = tr.Attempts
		}
		summaries[i] = summary{run.State, taskStates, starts}
		// Audit must record the source for traceability.
		events, _ := h.st.ListAudit(context.Background(), res.RunID)
		for _, ev := range events {
			if ev.Source != src {
				t.Fatalf("%s: audit event %s tagged source=%s", src, ev.Event, ev.Source)
			}
		}
		t.Logf("source=%-6s run=%s state=%s tasks=%v attempts=%v starts=%d",
			src, res.RunID, run.State, taskStates, attempts, starts)
		time.Sleep(5 * time.Millisecond)
	}

	base := summaries[0]
	for i, s := range summaries[1:] {
		if s.state != base.state || len(s.states) != len(base.states) || s.starts != base.starts {
			t.Fatalf("source %s summary=%+v differs from manual %+v", sources[i+1], s, base)
		}
		for j := range s.states {
			if s.states[j] != base.states[j] {
				t.Fatalf("source %s task state mismatch at %d: %s vs %s", sources[i+1], j, s.states[j], base.states[j])
			}
		}
	}
	t.Log("manual/timer/event produced equivalent runs; audit lines carry distinguishable sources")
}
