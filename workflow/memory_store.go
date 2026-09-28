package workflow

import (
	"fmt"
	"sync"
	"time"
)

// MemoryStore is a fully in-memory Store used by unit tests. It is safe for
// concurrent use and mirrors the semantics FileStore must provide on disk.
type MemoryStore struct {
	mu        sync.Mutex
	workflows map[string]WorkflowDef
	runs      map[RunID]*Run
	tasks     map[TaskRunID]*TaskRun
	events    []Event
}

// NewMemoryStore creates an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		workflows: map[string]WorkflowDef{},
		runs:      map[RunID]*Run{},
		tasks:     map[TaskRunID]*TaskRun{},
	}
}

func (s *MemoryStore) PutWorkflow(def WorkflowDef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workflows[def.Name] = def
	return nil
}

func (s *MemoryStore) GetWorkflow(name string) (WorkflowDef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	def, ok := s.workflows[name]
	if !ok {
		return WorkflowDef{}, fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	return def, nil
}

func (s *MemoryStore) ListWorkflows() ([]WorkflowDef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]WorkflowDef, 0, len(s.workflows))
	for _, def := range s.workflows {
		out = append(out, def)
	}
	return out, nil
}

func (s *MemoryStore) CreateRun(r Run) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.runs[r.ID]; exists {
		return fmt.Errorf("run already exists: %s", r.ID)
	}
	cp := r
	s.runs[r.ID] = &cp
	return nil
}

func (s *MemoryStore) GetRun(id RunID) (Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[id]
	if !ok {
		return Run{}, fmt.Errorf("%w: run %s", ErrNotFound, id)
	}
	return *r, nil
}

func (s *MemoryStore) CompareAndSwapRun(id RunID, expect RunState, mutate func(r *Run)) (Run, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[id]
	if !ok {
		return Run{}, false, fmt.Errorf("%w: run %s", ErrNotFound, id)
	}
	if r.State != expect {
		return *r, false, nil
	}
	cp := *r
	mutate(&cp)
	cp.UpdatedAt = time.Now()
	s.runs[id] = &cp
	return cp, true, nil
}

func (s *MemoryStore) ActiveRun(dedupKey string) (Run, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.runs {
		if r.DedupKey == dedupKey && !r.State.IsTerminal() {
			return *r, true, nil
		}
	}
	return Run{}, false, nil
}

func (s *MemoryStore) ListRuns(workflow string) ([]Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Run
	for _, r := range s.runs {
		if r.Workflow == workflow {
			out = append(out, *r)
		}
	}
	return out, nil
}

func (s *MemoryStore) CreateTask(t TaskRun) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.tasks[t.ID]; exists {
		return fmt.Errorf("task run already exists: %s", t.ID)
	}
	cp := t
	s.tasks[t.ID] = &cp
	return nil
}

func (s *MemoryStore) GetTask(id TaskRunID) (TaskRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return TaskRun{}, fmt.Errorf("%w: task %s", ErrNotFound, id)
	}
	return *t, nil
}

func (s *MemoryStore) CompareAndSwapTask(id TaskRunID, expect TaskState, mutate func(t *TaskRun)) (TaskRun, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return TaskRun{}, false, fmt.Errorf("%w: task %s", ErrNotFound, id)
	}
	if t.State != expect {
		return *t, false, nil
	}
	cp := *t
	mutate(&cp)
	cp.UpdatedAt = time.Now()
	s.tasks[id] = &cp
	return cp, true, nil
}

func (s *MemoryStore) ListTasks(runID RunID) ([]TaskRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []TaskRun
	for _, t := range s.tasks {
		if t.RunID == runID {
			out = append(out, *t)
		}
	}
	return out, nil
}

func (s *MemoryStore) GetTaskByRef(runID RunID, taskID string) (TaskRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.tasks {
		if t.RunID == runID && t.TaskID == taskID {
			return *t, nil
		}
	}
	return TaskRun{}, fmt.Errorf("%w: task %s in run %s", ErrNotFound, taskID, runID)
}

func (s *MemoryStore) ListRecoverableRuns() ([]Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Run
	for _, r := range s.runs {
		if !r.State.IsTerminal() {
			out = append(out, *r)
		}
	}
	return out, nil
}

func (s *MemoryStore) EventLog() EventLog { return &memoryEventLog{s: s} }

func (s *MemoryStore) Close() error { return nil }

// memoryEventLog shares the store lock and keeps events ordered by Seq.
type memoryEventLog struct {
	s *MemoryStore
}

func (l *memoryEventLog) Append(ev Event) error {
	l.s.mu.Lock()
	defer l.s.mu.Unlock()
	if ev.Time.IsZero() {
		ev.Time = time.Now()
	}
	ev.Seq = int64(len(l.s.events)) + 1
	l.s.events = append(l.s.events, ev)
	return nil
}

func (l *memoryEventLog) List(runID RunID) ([]Event, error) {
	l.s.mu.Lock()
	defer l.s.mu.Unlock()
	var out []Event
	for _, ev := range l.s.events {
		if ev.RunID == runID {
			out = append(out, ev)
		}
	}
	return out, nil
}

func (l *memoryEventLog) All() ([]Event, error) {
	l.s.mu.Lock()
	defer l.s.mu.Unlock()
	out := make([]Event, len(l.s.events))
	copy(out, l.s.events)
	return out, nil
}

func (l *memoryEventLog) LastSeq() (int64, error) {
	l.s.mu.Lock()
	defer l.s.mu.Unlock()
	return int64(len(l.s.events)), nil
}

func (l *memoryEventLog) Close() error { return nil }
