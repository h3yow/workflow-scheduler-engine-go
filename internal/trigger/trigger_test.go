package trigger

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/workflow-scheduler-engine-go/internal/workflow"
)

func TestTimerFiresPeriodicallyAndStops(t *testing.T) {
	var lastSrc atomic.Value
	var count int32
	tr := NewTimer("wf", 20*time.Millisecond)
	tr.Start(context.Background(), func(ctx context.Context, wf string, src workflow.TriggerSource) (*RunResult, error) {
		atomic.AddInt32(&count, 1)
		lastSrc.Store(src)
		return &RunResult{}, nil
	})
	time.Sleep(70 * time.Millisecond)
	tr.Stop()
	got := atomic.LoadInt32(&count)
	if got < 2 {
		t.Fatalf("timer fired %d times, want >=2", got)
	}
	if s := lastSrc.Load().(workflow.TriggerSource); s != workflow.TriggerTimer {
		t.Fatalf("source = %s, want timer", s)
	}
	snapshot := atomic.LoadInt32(&count)
	time.Sleep(50 * time.Millisecond)
	if atomic.LoadInt32(&count) != snapshot {
		t.Fatal("timer kept firing after Stop")
	}
	t.Logf("timer source=timer fired %d times, stopped cleanly", got)
}

func TestEventSourceFiresPerEventAndDropsWhenFull(t *testing.T) {
	var count int32
	var lastSrc atomic.Value
	release := make(chan struct{})
	es := NewEventSource("wf", 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go es.Serve(ctx, func(ctx context.Context, wf string, src workflow.TriggerSource) (*RunResult, error) {
		lastSrc.Store(src)
		atomic.AddInt32(&count, 1)
		<-release // block consumption until the test finishes filling
		return &RunResult{}, nil
	})
	// Wait until serving, then fill the single slot.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && !es.Emit("e1") {
		time.Sleep(2 * time.Millisecond)
	}
	// The first consumer may already hold e1, so the slot may accept one more
	// before blocking; in any case repeated Emit must never block forever.
	dropped := 0
	for i := 0; i < 5; i++ {
		if !es.Emit("overflow") {
			dropped++
		}
	}
	if dropped == 0 {
		t.Fatal("expected at least one Emit to be dropped, none was")
	}
	close(release)
	for i := 0; i < 200 && atomic.LoadInt32(&count) < 1; i++ {
		time.Sleep(5 * time.Millisecond)
	}
	if s := lastSrc.Load().(workflow.TriggerSource); s != workflow.TriggerEvent {
		t.Fatalf("source = %s, want event", s)
	}
	t.Logf("event source=event: overflow Emit returned false (drops=%d), producers never blocked", dropped)
}
