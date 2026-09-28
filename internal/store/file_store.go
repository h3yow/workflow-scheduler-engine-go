package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/h3yow/workflow-scheduler-engine-go/internal/workflow"
)

// FileStore is a mutex-guarded JSON snapshot store. Every mutation rewrites
// the snapshot atomically (temp file + rename), so a crash always leaves
// either the old or the new complete state.
type FileStore struct {
	path string
	mu   sync.Mutex
	data snapshotData
}

type snapshotData struct {
	Definitions map[string]workflow.Definition `json:"definitions"`
	Runs        map[string]workflow.Run        `json:"runs"`
	Audit       []workflow.AuditEvent          `json:"audit"`
}

// NewFileStore creates (or opens) a snapshot store at path.
func NewFileStore(path string) (*FileStore, error) {
	s := &FileStore{path: path, data: snapshotData{
		Definitions: map[string]workflow.Definition{},
		Runs:        map[string]workflow.Run{},
	}}
	if path == "" {
		return s, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return s, nil
		}
		return nil, fmt.Errorf("open store %q: %w", path, err)
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &s.data); err != nil {
			return nil, fmt.Errorf("parse store %q: %w", path, err)
		}
		if s.data.Definitions == nil {
			s.data.Definitions = map[string]workflow.Definition{}
		}
		if s.data.Runs == nil {
			s.data.Runs = map[string]workflow.Run{}
		}
	}
	return s, nil
}

func (s *FileStore) saveLocked() error {
	if s.path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(&s.data, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (s *FileStore) SaveDefinition(ctx context.Context, def workflow.Definition) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Definitions[def.ID] = def
	return s.saveLocked()
}

func (s *FileStore) GetDefinition(ctx context.Context, id string) (workflow.Definition, bool, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	def, ok := s.data.Definitions[id]
	return def, ok, nil
}

func (s *FileStore) CreateRun(ctx context.Context, r workflow.Run) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.data.Runs[r.ID]; exists {
		return fmt.Errorf("run %s already exists", r.ID)
	}
	s.data.Runs[r.ID] = r
	return s.saveLocked()
}

func (s *FileStore) GetRun(ctx context.Context, id string) (workflow.Run, bool, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.data.Runs[id]
	return r, ok, nil
}

func (s *FileStore) SaveRun(ctx context.Context, r workflow.Run) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.data.Runs[r.ID]; !exists {
		return fmt.Errorf("run %s not found", r.ID)
	}
	s.data.Runs[r.ID] = r
	return s.saveLocked()
}

func (s *FileStore) ActiveRun(ctx context.Context, workflowID string) (workflow.Run, bool, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	var found *workflow.Run
	for i := range s.data.Runs {
		r := s.data.Runs[i]
		if r.WorkflowID != workflowID || workflow.IsTerminal(r.State) {
			continue
		}
		if found == nil || r.CreatedAt.Before(found.CreatedAt) {
			cp := r
			found = &cp
		}
	}
	if found == nil {
		return workflow.Run{}, false, nil
	}
	return *found, true, nil
}

func sortedRuns(m map[string]workflow.Run) []workflow.Run {
	out := make([]workflow.Run, 0, len(m))
	for _, r := range m {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out
}

func (s *FileStore) ListRuns(ctx context.Context, workflowID string) ([]workflow.Run, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]workflow.Run, 0)
	for _, r := range sortedRuns(s.data.Runs) {
		if r.WorkflowID == workflowID {
			out = append(out, r)
		}
	}
	return out, nil
}

func (s *FileStore) ListRecoverable(ctx context.Context) ([]workflow.Run, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]workflow.Run, 0)
	for _, r := range sortedRuns(s.data.Runs) {
		if !workflow.IsTerminal(r.State) {
			out = append(out, r)
		}
	}
	return out, nil
}

func (s *FileStore) AppendAudit(ctx context.Context, e workflow.AuditEvent) error {
	_ = ctx
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Audit = append(s.data.Audit, e)
	return s.saveLocked()
}

func (s *FileStore) ListAudit(ctx context.Context, runID string) ([]workflow.AuditEvent, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]workflow.AuditEvent, 0)
	for _, e := range s.data.Audit {
		if e.RunID == runID {
			out = append(out, e)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Time.Equal(out[j].Time) {
			return out[i].Event < out[j].Event
		}
		return out[i].Time.Before(out[j].Time)
	})
	return out, nil
}

func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveLocked()
}

// Path returns the snapshot file path ("" for a memory-only store).
func (s *FileStore) Path() string { return s.path }
