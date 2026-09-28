// Command demo runs end-to-end workflow scenarios against a local JSON store.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/h3yow/workflow-scheduler-engine-go/internal/engine"
	"github.com/h3yow/workflow-scheduler-engine-go/internal/store"
	"github.com/h3yow/workflow-scheduler-engine-go/internal/trigger"
	"github.com/h3yow/workflow-scheduler-engine-go/internal/workflow"
)

func main() {
	path := flag.String("store", filepath.Join(os.TempDir(), "wf-demo.json"), "snapshot file")
	flag.Parse()

	st, err := store.NewFileStore(*path)
	must(err)
	eng := engine.New(st)
	defer eng.Close()
	ctx := context.Background()
	must(eng.Recover(ctx))

	// flaky task: fails the first 2 attempts, succeeds on the third.
	var flakyMu sync.Mutex
	flakyAttempt := map[string]int{}
	bind := func(wfID string, task workflow.TaskDef, fn engine.ExecutorFunc) {
		eng.RegisterExecutor(wfID, task.ID, fn)
	}

	// Scenario 1: dependency order + retry.
	wf1 := workflow.Definition{
		ID:              "wf-retry",
		Concurrency:     workflow.ConcurrencySkip,
		RunTimeoutMilli: 0,
		Tasks: []workflow.TaskDef{
			{ID: "extract", MaxAttempts: 1},
			{ID: "transform", Deps: []string{"extract"}, MaxAttempts: 3, BackoffMilli: 50},
			{ID: "load", Deps: []string{"transform"}, MaxAttempts: 1},
		},
	}
	must(eng.RegisterDefinition(ctx, wf1))
	bind(wf1.ID, wf1.Tasks[0], simple("extract done", 30*time.Millisecond, nil))
	bind(wf1.ID, wf1.Tasks[1], func(ctx context.Context, _, _ string, attempt int) error {
		flakyMu.Lock()
		flakyAttempt["transform"] = attempt
		n := attempt
		flakyMu.Unlock()
		fmt.Printf("    transform attempt %d\n", n)
		if n < 3 {
			return fmt.Errorf("transient error #%d", n)
		}
		return nil
	})
	bind(wf1.ID, wf1.Tasks[2], simple("load done", 30*time.Millisecond, nil))

	fmt.Println("== scenario 1: manual trigger with retries ==")
	res, err := eng.Trigger(ctx, wf1.ID, workflow.TriggerManual)
	must(err)
	fmt.Printf("    result: run=%s state=%s reason=%s\n\n", res.RunID, res.State, res.Reason)

	// Scenario 2: concurrent triggers from three sources under skip policy.
	wf2 := workflow.Definition{
		ID:          "wf-skip",
		Concurrency: workflow.ConcurrencySkip,
		Tasks:       []workflow.TaskDef{{ID: "slow", MaxAttempts: 1}},
	}
	must(eng.RegisterDefinition(ctx, wf2))
	bind(wf2.ID, wf2.Tasks[0], simple("working...", 150*time.Millisecond, nil))
	fmt.Println("== scenario 2: 3 concurrent triggers (manual/timer/event), skip policy ==")
	var wg sync.WaitGroup
	results := make([]string, 3)
	fire := func(i int, src workflow.TriggerSource) {
		defer wg.Done()
		r, err := eng.Trigger(ctx, wf2.ID, src)
		if err != nil {
			results[i] = src.String() + ":err=" + err.Error()
			return
		}
		tag := "executed"
		if r.Skipped {
			tag = "skipped"
		}
		results[i] = fmt.Sprintf("%s -> %s state=%s", src, tag, r.State)
	}
	wg.Add(3)
	go fire(0, workflow.TriggerManual)
	go fire(1, workflow.TriggerTimer)
	go fire(2, workflow.TriggerEvent)
	wg.Wait()
	for _, s := range results {
		fmt.Println("   ", s)
	}
	time.Sleep(300 * time.Millisecond)
	fmt.Println()

	// Scenario 3: timer + event sources feeding the queue policy workflow.
	wf3 := workflow.Definition{
		ID:          "wf-queue",
		Concurrency: workflow.ConcurrencyQueue,
		Tasks:       []workflow.TaskDef{{ID: "job", MaxAttempts: 1}},
	}
	must(eng.RegisterDefinition(ctx, wf3))
	bind(wf3.ID, wf3.Tasks[0], simple("job tick", 60*time.Millisecond, nil))
	tmr := trigger.NewTimer(wf3.ID, 80*time.Millisecond)
	evs := trigger.NewEventSource(wf3.ID, 8)
	serveCtx, cancelServe := context.WithCancel(ctx)
	go evs.Serve(serveCtx, eng.TriggerFunc())
	tmr.Start(serveCtx, eng.TriggerFunc())
	fmt.Println("== scenario 3: timer + event triggers for 500ms with queue policy ==")
	for i := 0; i < 3; i++ {
		evs.Emit(fmt.Sprintf("event-%d", i))
	}
	time.Sleep(500 * time.Millisecond)
	tmr.Stop()
	cancelServe()
	evs.Close()
	runs, _ := st.ListRuns(ctx, wf3.ID)
	fmt.Printf("    %d serialised runs for %s\n\n", len(runs), wf3.ID)

	// Scenario 4: cancellation.
	wf4 := workflow.Definition{
		ID:              "wf-cancel",
		Concurrency:     workflow.ConcurrencySkip,
		RunTimeoutMilli: 1000,
		Tasks: []workflow.TaskDef{
			{ID: "a"},
			{ID: "b", Deps: []string{"a"}, TimeoutMilli: 0},
			{ID: "c", Deps: []string{"b"}},
		},
	}
	must(eng.RegisterDefinition(ctx, wf4))
	bind(wf4.ID, wf4.Tasks[0], simple("a done", 20*time.Millisecond, nil))
	bind(wf4.ID, wf4.Tasks[1], func(ctx context.Context, _, _ string, _ int) error {
		select {
		case <-time.After(2 * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	bind(wf4.ID, wf4.Tasks[2], simple("c done", 20*time.Millisecond, nil))
	fmt.Println("== scenario 4: cancel mid-run ==")
	go func() {
		time.Sleep(100 * time.Millisecond)
		if r, _, _ := st.ActiveRun(ctx, wf4.ID); r.ID != "" {
			fmt.Println("    canceling", r.ID)
			_ = eng.Cancel(r.ID)
		}
	}()
	res4, err := eng.Trigger(ctx, wf4.ID, workflow.TriggerManual)
	must(err)
	fmt.Printf("    result: run=%s state=%s reason=%s\n", res4.RunID, res4.State, res4.Reason)
	fmt.Printf("\nsnapshot written to %s\n", *path)
}

func simple(msg string, d time.Duration, retErr error) engine.ExecutorFunc {
	return func(ctx context.Context, _, _ string, _ int) error {
		select {
		case <-time.After(d):
			if msg != "" {
				fmt.Println("    task:", msg)
			}
			return retErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}
