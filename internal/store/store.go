// Package store provides local persistence for workflow definitions, runs
// and audit events. A JSON snapshot file is used so that runs can survive
// process restarts.
package store

import (
	"context"

	"github.com/h3yow/workflow-scheduler-engine-go/internal/workflow"
)

// Store persists definitions and runs. All mutating operations are
// serialised; conditional operations use CAS to support concurrency control.
type Store interface {
	SaveDefinition(ctx context.Context, def workflow.Definition) error
	GetDefinition(ctx context.Context, id string) (workflow.Definition, bool, error)

	// CreateRun inserts a new run.
	CreateRun(ctx context.Context, r workflow.Run) error
	// GetRun reads one run.
	GetRun(ctx context.Context, id string) (workflow.Run, bool, error)
	// SaveRun overwrites a run unconditionally (caller must hold transition locks).
	SaveRun(ctx context.Context, r workflow.Run) error
	// ActiveRun returns the single non-terminal run of a workflow, if any.
	ActiveRun(ctx context.Context, workflowID string) (workflow.Run, bool, error)
	// ListRuns returns all runs of a workflow, oldest first.
	ListRuns(ctx context.Context, workflowID string) ([]workflow.Run, error)
	// ListRecoverable returns runs that were not terminal at last shutdown
	// (queued/running/waiting), used by crash recovery.
	ListRecoverable(ctx context.Context) ([]workflow.Run, error)

	AppendAudit(ctx context.Context, e workflow.AuditEvent) error
	ListAudit(ctx context.Context, runID string) ([]workflow.AuditEvent, error)

	Close() error
}
