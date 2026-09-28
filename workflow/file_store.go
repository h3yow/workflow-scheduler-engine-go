package workflow

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"
)

// FileStore persists all state under a directory:
//
//	workflows.json      workflow definitions
//	runs.json           run records
//	tasks.json          task-run records
//	events.jsonl        append-only state transition events
//	lock                advisory flock (one engine per directory)
//
// Each mutating operation rewrites the affected JSON file atomically
// (write temp file + fsync + rename) under the in-process mutex, so a crash
// leaves either the old or the new state, never a torn file.
type FileStore struct {
	dir  string
	mu   sync.Mutex
	lock *os.File

	workflows map[string]WorkflowDef
	runs      map[RunID]*Run
	tasks     map[TaskRunID]*TaskRun
	events    []Event
}

// NewFileStore opens (or creates) a local store rooted at dir and replays
// events.jsonl. Only one process may hold the directory at a time.
func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create store dir: %w", err)
	}
	lock, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("store dir %s is locked by another process: %w", dir, err)
	}

	s := &FileStore{
		dir:       dir,
		lock:      lock,
		workflows: map[string]WorkflowDef{},
		runs:      map[RunID]*Run{},
		tasks:     map[TaskRunID]*TaskRun{},
	}
	if err := s.load(); err != nil {
		lock.Close()
		return nil, err
	}
	return s, nil
}

func (s *FileStore) load() error {
	if err := readJSON(filepath.Join(s.dir, "workflows.json"), &s.workflows); err != nil {
		return err
	}
	if s.workflows == nil {
		s.workflows = map[string]WorkflowDef{}
	}
	if err := readJSON(filepath.Join(s.dir, "runs.json"), &s.runs); err != nil {
		return err
	}
	if s.runs == nil {
		s.runs = map[RunID]*Run{}
	}
	if err := readJSON(filepath.Join(s.dir, "tasks.json"), &s.tasks); err != nil {
		return err
	}
	if s.tasks == nil {
		s.tasks = map[TaskRunID]*TaskRun{}
	}
	f, err := os.OpenFile(filepath.Join(s.dir, "events.jsonl"), os.O_RDONLY|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 8<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var ev Event
		if err := json.Unmarshal(line, &ev); err != nil {
			return fmt.Errorf("corrupt event log: %w", err)
		}
		s.events = append(s.events, ev)
	}
	return sc.Err()
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) || len(b) == 0 {
		return nil
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func writeJSONAtomic(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (s *FileStore) persistWorkflows() error {
	return writeJSONAtomic(filepath.Join(s.dir, "workflows.json"), s.workflows)
}
func (s *FileStore) persistRuns() error {
	return writeJSONAtomic(filepath.Join(s.dir, "runs.json"), s.runs)
}
func (s *FileStore) persistTasks() error {
	return writeJSONAtomic(filepath.Join(s.dir, "tasks.json"), s.tasks)
}

func (s *FileStore) PutWorkflow(def WorkflowDef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workflows[def.Name] = def
	return s.persistWorkflows()
}

func (s *FileStore) GetWorkflow(name string) (WorkflowDef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	def, ok := s.workflows[name]
	if !ok {
		return WorkflowDef{}, fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	return def, nil
}

func (s *FileStore) ListWorkflows() ([]WorkflowDef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]WorkflowDef, 0, len(s.workflows))
	for _, def := range s.workflows {
		out = append(out, def)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (s *FileStore) CreateRun(r Run) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.runs[r.ID]; exists {
		return fmt.Errorf("run already exists: %s", r.ID)
	}
	cp := r
	s.runs[r.ID] = &cp
	return s.persistRuns()
}

func (s *FileStore) GetRun(id RunID) (Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[id]
	if !ok {
		return Run{}, fmt.Errorf("%w: run %s", ErrNotFound, id)
	}
	return *r, nil
}

func (s *FileStore) CompareAndSwapRun(id RunID, expect RunState, mutate func(r *Run)) (Run, bool, error) {
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
	if err := s.persistRuns(); err != nil {
		return cp, false, err
	}
	return cp, true, nil
}

func (s *FileStore) ActiveRun(dedupKey string) (Run, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.runs {
		if r.DedupKey == dedupKey && !r.State.IsTerminal() {
			return *r, true, nil
		}
	}
	return Run{}, false, nil
}

func (s *FileStore) ListRuns(workflow string) ([]Run, error) {
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

func (s *FileStore) CreateTask(t TaskRun) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.tasks[t.ID]; exists {
		return fmt.Errorf("task run already exists: %s", t.ID)
	}
	cp := t
	s.tasks[t.ID] = &cp
	return s.persistTasks()
}

func (s *FileStore) GetTask(id TaskRunID) (TaskRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return TaskRun{}, fmt.Errorf("%w: task %s", ErrNotFound, id)
	}
	return *t, nil
}

func (s *FileStore) CompareAndSwapTask(id TaskRunID, expect TaskState, mutate func(t *TaskRun)) (TaskRun, bool, error) {
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
	if err := s.persistTasks(); err != nil {
		return cp, false, err
	}
	return cp, true, nil
}

func (s *FileStore) ListTasks(runID RunID) ([]TaskRun, error) {
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

func (s *FileStore) GetTaskByRef(runID RunID, taskID string) (TaskRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.tasks {
		if t.RunID == runID && t.TaskID == taskID {
			return *t, nil
		}
	}
	return TaskRun{}, fmt.Errorf("%w: task %s in run %s", ErrNotFound, taskID, runID)
}

func (s *FileStore) ListRecoverableRuns() ([]Run, error) {
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

func (s *FileStore) EventLog() EventLog { return &fileEventLog{s: s} }

func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN); err != nil {
		return err
	}
	return s.lock.Close()
}

// fileEventLog appends JSON lines and fsyncs so events survive a kill -9.
type fileEventLog struct {
	s *FileStore
}

func (l *fileEventLog) Append(ev Event) error {
	l.s.mu.Lock()
	defer l.s.mu.Unlock()
	if ev.Time.IsZero() {
		ev.Time = time.Now()
	}
	ev.Seq = int64(len(l.s.events)) + 1
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	path := filepath.Join(l.s.dir, "events.jsonl")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	l.s.events = append(l.s.events, ev)
	return nil
}

func (l *fileEventLog) List(runID RunID) ([]Event, error) {
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

func (l *fileEventLog) All() ([]Event, error) {
	l.s.mu.Lock()
	defer l.s.mu.Unlock()
	out := make([]Event, len(l.s.events))
	copy(out, l.s.events)
	return out, nil
}

func (l *fileEventLog) LastSeq() (int64, error) {
	l.s.mu.Lock()
	defer l.s.mu.Unlock()
	return int64(len(l.s.events)), nil
}

func (l *fileEventLog) Close() error { return nil }
