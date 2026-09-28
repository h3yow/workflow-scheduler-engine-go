// Package trigger provides timer- and event-based trigger sources. Both only
// call a trigger function, so semantics are identical to manual triggers
// (same concurrency policy, timeout and cancellation).
package trigger

import (
	"context"
	"sync"
	"time"

	"github.com/h3yow/workflow-scheduler-engine-go/internal/workflow"
)

// RunResult mirrors engine.RunResult to keep this package decoupled.
type RunResult struct {
	RunID   string
	State   workflow.RunState
	Reason  workflow.Reason
	Message string
	Skipped bool
}

// TriggerFunc triggers one run. The engine supplies an adapter to this shape.
type TriggerFunc func(ctx context.Context, workflowID string, source workflow.TriggerSource) (*RunResult, error)

// Timer periodically fires triggers for one workflow until stopped.
type Timer struct {
	workflowID string
	interval   time.Duration

	mu      sync.Mutex
	stop    chan struct{}
	done    chan struct{}
	running bool
}

// NewTimer creates a timer trigger source.
func NewTimer(workflowID string, interval time.Duration) *Timer {
	return &Timer{workflowID: workflowID, interval: interval}
}

// Start begins firing. Repeated calls are ignored while running.
func (t *Timer) Start(ctx context.Context, fire TriggerFunc) {
	t.mu.Lock()
	if t.running {
		t.mu.Unlock()
		return
	}
	t.stop = make(chan struct{})
	t.done = make(chan struct{})
	t.running = true
	t.mu.Unlock()

	go func() {
		defer close(t.done)
		ticker := time.NewTicker(t.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.stop:
				return
			case <-ticker.C:
				// Admission (skip/queue), timeout and the run lifetime are
				// owned by the engine.
				_, _ = fire(context.Background(), t.workflowID, workflow.TriggerTimer)
			}
		}
	}()
}

// Stop halts the timer and waits for the firing loop to exit.
func (t *Timer) Stop() {
	t.mu.Lock()
	if !t.running {
		t.mu.Unlock()
		return
	}
	stop, done := t.stop, t.done
	t.running = false
	t.mu.Unlock()
	close(stop)
	<-done
}

// EventSource accepts named events and triggers a workflow per event.
type EventSource struct {
	workflowID string
	events     chan string
	stop       chan struct{}
	done       chan struct{}
	started    bool
	mu         sync.Mutex
}

// NewEventSource creates an event trigger source.
func NewEventSource(workflowID string, buffer int) *EventSource {
	if buffer < 0 {
		buffer = 0
	}
	return &EventSource{
		workflowID: workflowID,
		events:     make(chan string, buffer),
		stop:       make(chan struct{}),
	}
}

// Emit queues one event. It returns false (and drops the event) when the
// buffer is full or the source is not serving, so producers never block.
func (es *EventSource) Emit(name string) bool {
	es.mu.Lock()
	started := es.started
	es.mu.Unlock()
	if !started {
		return false
	}
	select {
	case es.events <- name:
		return true
	default:
		return false
	}
}

// Serve processes events until ctx is canceled or Close is called.
func (es *EventSource) Serve(ctx context.Context, fire TriggerFunc) {
	es.mu.Lock()
	if es.started {
		es.mu.Unlock()
		return
	}
	es.started = true
	es.done = make(chan struct{})
	done := es.done
	es.mu.Unlock()
	defer func() {
		es.mu.Lock()
		es.started = false
		es.mu.Unlock()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-es.stop:
			return
		case <-es.events:
			_, _ = fire(context.Background(), es.workflowID, workflow.TriggerEvent)
		case <-done:
			return
		}
	}
}

// Close stops the event source.
func (es *EventSource) Close() {
	es.mu.Lock()
	defer es.mu.Unlock()
	select {
	case <-es.stop:
		// already closed
	default:
		close(es.stop)
	}
}
