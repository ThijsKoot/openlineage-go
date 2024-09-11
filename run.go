package openlineage

import (
	"context"
	"time"

	"github.com/ThijsKoot/openlineage-go/pkg/facets"
	"github.com/google/uuid"
)

type RunEvent struct {
	Run Run
	Job Job

	EventType EventType

	// The set of **input** datasets.
	Inputs []InputElement

	// The set of **output** datasets.
	Outputs []OutputElement

	BaseEvent
}

// AsEmittable transforms this RunEvent into its emittable representation.
func (e *RunEvent) AsEmittable() Event {
	eventType := e.EventType
	if eventType == "" {
		eventType = EventTypeOther
	}

	return Event{
		EventTime: e.EventTime,
		EventType: &eventType,
		Run:       &e.Run,
		Job:       &e.Job,
		Inputs:    e.Inputs,
		Outputs:   e.Outputs,
		Producer:  e.Producer,
		SchemaURL: e.SchemaURL,
	}
}

// Emit calls [Client.Emit] on [DefaultClient].
func (e *RunEvent) Emit() {
	_ = DefaultClient.Emit(context.Background(), e)
}

// NewNamespacedRunEvent creates a new [RunEvent] with EventTime set to [time.Now].
func NewNamespacedRunEvent(
	eventType EventType,
	runID uuid.UUID,
	jobName string,
	jobNamespace string,
) *RunEvent {
	return &RunEvent{
		BaseEvent: BaseEvent{
			Producer:  producer,
			SchemaURL: schemaURL,
			EventTime: time.Now().Format(time.RFC3339),
		},
		Run: Run{
			RunID: runID.String(),
		},
		EventType: eventType,
		Job: Job{
			Name:      jobName,
			Namespace: jobNamespace,
		},
	}
}

// NewRunEvent calls [NewNamespacedRunEvent] with [DefaultNamespace].
func NewRunEvent(eventType EventType, runID uuid.UUID, jobName string) *RunEvent {
	return NewNamespacedRunEvent(eventType, runID, jobName, DefaultNamespace)
}

// WithRunFacets sets the supplied [facets.RunFacet] for this RunEvent.
func (r *RunEvent) WithRunFacets(runFacets ...facets.RunFacet) *RunEvent {
	for _, rf := range runFacets {
		rf.Apply(&r.Run.Facets)
	}

	return r
}

// WithJobFacets sets the supplied instances of [facets.JobFacet] for this RunEvent.
func (r *RunEvent) WithJobFacets(jobFacets ...facets.JobFacet) *RunEvent {
	for _, rf := range jobFacets {
		rf.Apply(&r.Job.Facets)
	}

	return r
}

// WithInputs appends the supplied instances of [InputElement] to this event's inputs.
func (r *RunEvent) WithInputs(inputs ...InputElement) *RunEvent {
	r.Inputs = append(r.Inputs, inputs...)

	return r
}

// WithOutputs appends the supplied instances of [OutputElement] to this event's outputs.
func (r *RunEvent) WithOutputs(outputs ...OutputElement) *RunEvent {
	r.Outputs = append(r.Outputs, outputs...)

	return r
}

// WithParent configures [facets.Parent] for this RunEvent.
func (r *RunEvent) WithParent(parentID uuid.UUID, jobName, namespace string) *RunEvent {
	parent := facets.NewParent(
		facets.Job{
			Name:      jobName,
			Namespace: namespace,
		},
		facets.Run{
			RunID: parentID.String(),
		},
	)

	return r.WithRunFacets(parent)
}
