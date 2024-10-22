package run

import (
	"bytes"
	"context"

	"github.com/ThijsKoot/openlineage-go"
	"github.com/ThijsKoot/openlineage-go/pkg/facets"
	"github.com/google/uuid"
)

var _ Run = (*noopRun)(nil)

type noopRun struct{}

// RecordFacets implements Run.
func (n *noopRun) RecordRunFacets(...facets.RunFacet) {}

func (n *noopRun) RecordJobFacets(...facets.JobFacet) {}

// RecordInputs implements Run.
func (n *noopRun) RecordInputs(...openlineage.InputElement) {}

// RecordOutputs implements Run.
func (n *noopRun) RecordOutputs(...openlineage.OutputElement) {}

// NewChild implements RunContext.
func (n *noopRun) NewChild(ctx context.Context, jobName string) (context.Context, Run) {
	return ctx, &noopRun{}
}

// StartChild implements RunContext.
func (n *noopRun) StartChild(ctx context.Context, jobName string) (context.Context, Run) {
	return ctx, &noopRun{}
}

// HasFailed implements RunContext.
func (n *noopRun) HasFailed() bool {
	return false
}

// Child implements RunContext.
func (n *noopRun) Child(ctx context.Context, jobName string) (context.Context, Run) {
	return ctx, &noopRun{}
}

// Finish implements RunContext.
func (n *noopRun) Finish() {}

// Emit implements RunContext.
func (n *noopRun) Emit(context.Context, openlineage.Emittable) {}

// RecordError implements RunContext.
func (n *noopRun) RecordError(error) {}

// NewEvent implements RunContext.
func (n *noopRun) NewEvent(openlineage.EventType) *openlineage.RunEvent {
	return &openlineage.RunEvent{}
}

// JobName implements RunContext.
func (n *noopRun) JobName() string {
	return ""
}

// JobNamespace implements RunContext.
func (n *noopRun) JobNamespace() string {
	return ""
}

// Parent implements RunContext.
func (n *noopRun) Parent() Run {
	return &noopRun{}
}

// RunID implements RunContext.
func (n *noopRun) RunID() uuid.UUID {
	empty := bytes.Repeat([]byte{0}, 16)
	id, _ := uuid.FromBytes(empty)
	return id
}
