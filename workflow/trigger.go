package workflow

import (
	"context"
	"sync"
	"time"
)

// Scheduler triggers a workflow on a fixed time interval. It uses the same
// Engine.Trigger entrypoint as manual/event sources, so concurrency policy,
// timeout and cancellation behave identically no matter where a trigger came
// from.
type Scheduler struct {
	engine   *Engine
	workflow string
	interval time.Duration

	mu      sync.Mutex
	started bool
	stop    chan struct{}
	done    chan struct{}
	// OnTrigger, if non-nil, is invoked after each tick with the outcome.
	OnTrigger func(run Run, err error)
}

// NewScheduler creates (but does not start) a periodic trigger.
func NewScheduler(eng *Engine, workflow string, interval time.Duration) *Scheduler {
	return &Scheduler{
		engine:   eng,
		workflow: workflow,
		interval: interval,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Start begins firing periodic triggers until Stop is called. Each tick goes
// through Engine.Trigger, honoring the workflow's concurrency policy.
func (s *Scheduler) Start(ctx context.Context) {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return
	}
	s.started = true
	s.mu.Unlock()

	go func() {
		defer close(s.done)
		t := time.NewTicker(s.interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.stop:
				return
			case <-t.C:
				run, err := s.engine.Trigger(ctx, s.workflow, TriggerSchedule)
				if s.OnTrigger != nil {
					s.OnTrigger(run, err)
				}
			}
		}
	}()
}

// Stop halts the periodic trigger. In-flight runs are unaffected unless
// canceled separately.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started {
		return
	}
	close(s.stop)
	<-s.done
	s.started = false
}

// EmitEvent feeds an external event into a workflow through exactly the same
// Trigger path as manual and scheduled sources.
func EmitEvent(ctx context.Context, eng *Engine, workflow string) (Run, error) {
	return eng.Trigger(ctx, workflow, TriggerEvent)
}
