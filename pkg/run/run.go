package run

import (
	"context"
	"runtime"

	"github.com/ThijsKoot/openlineage-go"
	"github.com/ThijsKoot/openlineage-go/pkg/facets"
	"github.com/go-stack/stack"
	"github.com/google/uuid"
)

// Run is an instrumentation utility that allows for more ergonomic usage of the SDK.
// It is loosely modeled after the OpenTelemetry Span/Trace APIs.
type Run interface {
	// Parent returns the parent of this run, if any.
	Parent() Run

	// RunID returns the ID for this Run.
	RunID() uuid.UUID

	// JobName returns the name for this Run's job.
	JobName() string

	// JobNamespace returns the namespace for this Run's job.
	JobNamespace() string

	// NewChild creates a new Run with the current Run set as its parent
	NewChild(ctx context.Context, jobName string) (context.Context, Run)

	// StartChild calls NewChild and emits a START event
	StartChild(ctx context.Context, jobName string) (context.Context, Run)

	// NewEvent creates a new Event of the provided EventType
	NewEvent(openlineage.EventType) *openlineage.RunEvent

	// Finish will emit a COMPLETE event if no error has occurred.
	// Otherwise, it will emit a FAIL event.
	Finish()

	// Returns true if RecordError was called for this Run.
	HasFailed() bool

	// RecordError emits an OTHER event with an ErrorMessage facet.
	// Once this is called, the run is considered to have failed.
	RecordError(error)

	// RecordRunFacets emits an OTHER event with the supplied RunFacets
	RecordRunFacets(...facets.RunFacet)

	// RecordJobFacets emits an OTHER event with the supplied JobFacets
	RecordJobFacets(...facets.JobFacet)

	// RecordInputs emits an OTHER event with the supplied InputElements
	RecordInputs(...openlineage.InputElement)

	// RecordOutputs emits an OTHER event with the supplied OutputElements
	RecordOutputs(...openlineage.OutputElement)
}

type run struct {
	parent       Run
	runID        uuid.UUID
	jobName      string
	jobNamespace string

	hasFailed bool
	client    *Client
}

// RecordFacets implements Run.
func (r *run) RecordRunFacets(facets ...facets.RunFacet) {
	event := r.NewEvent(openlineage.EventTypeOther).
		WithRunFacets(facets...)

	r.Emit(context.Background(), event)
}

// RecordFacets implements Run.
func (r *run) RecordJobFacets(facets ...facets.JobFacet) {
	event := r.NewEvent(openlineage.EventTypeOther).
		WithJobFacets(facets...)

	r.Emit(context.Background(), event)
}

// RecordInputs implements Run.
func (r *run) RecordInputs(inputs ...openlineage.InputElement) {
	event := r.NewEvent(openlineage.EventTypeOther).
		WithInputs(inputs...)

	r.Emit(context.Background(), event)
}

// RecordOutputs implements Run.
func (r *run) RecordOutputs(outputs ...openlineage.OutputElement) {
	event := r.NewEvent(openlineage.EventTypeOther).
		WithOutputs(outputs...)

	r.Emit(context.Background(), event)
}

// JobName implements RunContext.
func (r *run) JobName() string {
	return r.jobName
}

// JobNamespace implements RunContext.
func (r *run) JobNamespace() string {
	return r.jobNamespace
}

// RunID implements RunContext.
func (r *run) RunID() uuid.UUID {
	return r.runID
}

func (r *run) Parent() Run {
	return r.parent
}

func (r *run) NewEvent(eventType openlineage.EventType) *openlineage.RunEvent {
	run := openlineage.NewNamespacedRunEvent(
		eventType,
		r.runID,
		r.jobName,
		r.jobNamespace,
	)

	if r.Parent() != nil {
		parent := facets.NewParent(
			facets.Job{
				Name:      r.parent.JobName(),
				Namespace: r.parent.JobNamespace(),
			},
			facets.Run{
				RunID: r.parent.RunID().String(),
			},
		)

		run = run.WithRunFacets(parent)
	}

	return run
}

// NewChild calls [Client.NewRun]. [ctx] is expected to have a Run associated with it already.
func (r *run) NewChild(ctx context.Context, jobName string) (context.Context, Run) {
	return r.client.NewRun(ctx, jobName)
}

// StartChild calls [Client.StartRun]. [ctx] is expected to have a Run associated with it already.
func (r *run) StartChild(ctx context.Context, jobName string) (context.Context, Run) {
	return r.client.StartRun(ctx, jobName)
}

// Emit uses its openlineage.Client to emit an event
func (r *run) Emit(ctx context.Context, event openlineage.Emittable) {
	go func() {
		_ = r.client.Emit(ctx, event)
	}()
}

func (r *run) RecordError(err error) {
	r.hasFailed = true

	errorMessage := err.Error()

	stacktrace := stack.Caller(1).String()
	language := runtime.Version()

	errorFacet := facets.
		NewErrorMessage(errorMessage, language).
		WithStackTrace(stacktrace)

	errorEvent := r.NewEvent(openlineage.EventTypeOther).
		WithRunFacets(errorFacet)

	r.Emit(context.Background(), errorEvent)
}

func (r *run) Finish() {
	eventType := openlineage.EventTypeComplete
	if r.hasFailed {
		eventType = openlineage.EventTypeFail
	}

	r.Emit(context.Background(), r.NewEvent(eventType))
}

func (r *run) HasFailed() bool {
	return r.hasFailed
}
