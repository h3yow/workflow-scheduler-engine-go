package engine

import (
	"context"

	"github.com/h3yow/workflow-scheduler-engine-go/internal/trigger"
	"github.com/h3yow/workflow-scheduler-engine-go/internal/workflow"
)

// TriggerFunc adapts Engine.Trigger to trigger.TriggerFunc so that timer and
// event sources share the exact same trigger path as manual calls.
func (e *Engine) TriggerFunc() trigger.TriggerFunc {
	return func(ctx context.Context, workflowID string, source workflow.TriggerSource) (*trigger.RunResult, error) {
		res, err := e.Trigger(ctx, workflowID, source)
		if err != nil {
			return nil, err
		}
		return &trigger.RunResult{
			RunID:   res.RunID,
			State:   res.State,
			Reason:  res.Reason,
			Message: res.Message,
			Skipped: res.Skipped,
		}, nil
	}
}
