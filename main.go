package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	wf "github.com/h3yow/workflow-scheduler-engine-go/workflow"
)

func main() {
	dir := flag.String("dir", ".wfdata", "local storage directory")
	flag.Parse()

	// File-backed store: state survives process restarts.
	store, err := wf.NewFileStore(*dir)
	if err != nil {
		fail("open store: %v", err)
	}

	reg := wf.NewRegistry()
	var bAttempts int32
	reg.Register("fetch", func(ctx context.Context, tc wf.TaskContext) (string, error) {
		fmt.Printf("  -> task fetch running (attempt=%d, trigger=%s, run=%s)\n",
			tc.Attempt, tc.Trigger, short(string(tc.RunID)))
		return "records=42", nil
	})
	reg.Register("transform", func(ctx context.Context, tc wf.TaskContext) (string, error) {
		n := atomic.AddInt32(&bAttempts, 1)
		fmt.Printf("  -> task transform running (attempt=%d)\n", tc.Attempt)
		if n < 2 {
			return "", wf.NewTaskError("TRANSIENT", "simulated flaky service")
		}
		return "transformed", nil
	})
	reg.Register("load", func(ctx context.Context, tc wf.TaskContext) (string, error) {
		fmt.Printf("  -> task load running (attempt=%d)\n", tc.Attempt)
		return "loaded", nil
	})

	eng, err := wf.NewEngine(store, reg,
		wf.WithRecovery(wf.RecoveryResume),
		wf.WithEventLogger(func(ev wf.Event) {
			target := string(ev.RunID)
			if ev.TaskID != "" {
				target += "/" + ev.TaskID
			}
			fmt.Printf("     event %-30s %-9s->%-9s %s %s\n",
				short(target), ev.FromState, ev.ToState, ev.Reason, ev.Message)
		}),
	)
	if err != nil {
		fail("new engine (recovery): %v", err)
	}
	defer func() { _ = eng.Close() }()

	def := wf.WorkflowDef{
		Name: "etl",
		Tasks: []wf.TaskDef{
			{ID: "fetch"},
			{ID: "transform", Dependencies: []string{"fetch"},
				Retries: wf.RetryPolicy{MaxAttempts: 3, InitialBackoff: 50 * time.Millisecond}},
			{ID: "load", Dependencies: []string{"transform"}},
		},
		TriggerConcurrency: wf.ConcurrencyReject,
	}
	if err := eng.Register(def); err != nil {
		fail("register: %v", err)
	}

	ctx := context.Background()
	run, err := eng.Trigger(ctx, "etl", wf.TriggerManual)
	if err != nil {
		fail("trigger: %v", err)
	}
	fmt.Printf("started manual run %s\n", short(string(run.ID)))

	final, err := eng.WaitRun(ctx, run.ID)
	if err != nil {
		fail("wait: %v", err)
	}
	fmt.Printf("\nrun finished: state=%s reason=%s message=%s\n", final.State, final.Reason, final.Message)

	// Demonstrate that a concurrent trigger is rejected with a distinguishable
	// cause by starting a second, blocking run of a different workflow.
	setupBlockingDemo(eng)

	if final.State != wf.RunSucceeded {
		os.Exit(1)
	}
	fmt.Println("\nstate is persisted under " + *dir + "; rerun the binary to observe startup recovery.")
}

// setupBlockingDemo shows reject-policy behavior: a second trigger while a run
// is active returns an error wrapping wf.ErrRunActive, instead of duplicating.
func setupBlockingDemo(eng *wf.Engine) {
	reg := wf.NewRegistry()
	reg.Register("only", func(ctx context.Context, tc wf.TaskContext) (string, error) {
		<-time.After(40 * time.Millisecond)
		return "ok", nil
	})
	// Use a throwaway in-memory store/engine to keep the demo self-contained.
	tmpEng, err := wf.NewEngine(wf.NewMemoryStore(), reg)
	if err != nil {
		return
	}
	defer func() { _ = tmpEng.Close() }()
	blocking := wf.WorkflowDef{
		Name: "once", Tasks: []wf.TaskDef{{ID: "only"}},
		TriggerConcurrency: wf.ConcurrencyReject,
	}
	if err := tmpEng.Register(blocking); err != nil {
		return
	}
	r1, _ := tmpEng.Trigger(context.Background(), "once", wf.TriggerSchedule)
	_, err = wf.EmitEvent(context.Background(), tmpEng, "once")
	if errors.Is(err, wf.ErrRunActive) {
		fmt.Printf("\nconcurrent event trigger while run %s active was rejected (no duplicate execution)\n",
			short(string(r1.ID)))
	}
	_, _ = tmpEng.WaitRun(context.Background(), r1.ID)
}

func short(s string) string {
	if len(s) > 18 {
		return s[:18] + "..."
	}
	return s
}

func fail(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", a...)
	os.Exit(1)
}
