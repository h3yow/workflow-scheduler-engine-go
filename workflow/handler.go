package workflow

import (
	"context"
	"sync"
)

// TaskContext carries immutable execution context to a handler.
type TaskContext struct {
	RunID    RunID
	Workflow string
	TaskID   string
	Attempt  int
	Trigger  TriggerSource
}

// TaskFunc is user-supplied task logic. Handlers should be idempotent because
// a process crash between attempts may cause the same task to be re-invoked.
type TaskFunc func(ctx context.Context, tc TaskContext) (string, error)

// Registry maps task ids to handlers.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]TaskFunc
}

// NewRegistry creates an empty handler registry.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]TaskFunc)}
}

// Register binds a handler to a task id. Duplicate registration is rejected
// so test/demo setup mistakes surface immediately.
func (r *Registry) Register(taskID string, fn TaskFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.handlers[taskID]; exists {
		panic("workflow: handler already registered for task " + taskID)
	}
	r.handlers[taskID] = fn
}

// Lookup returns the handler registered for taskID.
func (r *Registry) Lookup(taskID string) (TaskFunc, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.handlers[taskID]
	return fn, ok
}
