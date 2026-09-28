package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/h3yow/workflow-scheduler-engine-go/internal/scheduler"
	"github.com/h3yow/workflow-scheduler-engine-go/internal/workflow"
)

func validationReason(t *testing.T, h *testHarness, def workflow.Definition) workflow.Reason {
	t.Helper()
	err := h.eng.RegisterDefinition(context.Background(), def)
	var ve *scheduler.ValidationError
	if errors.As(err, &ve) {
		return ve.Reason
	}
	t.Fatalf("expected ValidationError for %s, got %v", def.ID, err)
	return ""
}
