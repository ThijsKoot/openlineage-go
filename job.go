package openlineage

import (
	"context"
	"time"

	"github.com/ThijsKoot/openlineage-go/pkg/facets"
)

// JobEvent represents an OpenLineage JobEvent.
type JobEvent struct {
	Job Job

	// The set of **input** datasets.
	Inputs []InputElement
	// The set of **output** datasets.
	Outputs []OutputElement

	BaseEvent
}

func (e *JobEvent) AsEmittable() Event {
	return Event{
		EventTime: e.EventTime,
		Job:       &e.Job,
		Inputs:    e.Inputs,
		Outputs:   e.Outputs,
		Producer:  e.Producer,
		SchemaURL: e.SchemaURL,
	}
}

// Emit calls [Client.Emit] on [DefaultClient].
func (e *JobEvent) Emit() {
	_ = DefaultClient.Emit(context.Background(), e)
}

func NewNamespacedJobEvent(name, namespace string) *JobEvent {
	return &JobEvent{
		BaseEvent: BaseEvent{
			Producer:  producer,
			SchemaURL: schemaURL,
			EventTime: time.Now().Format(time.RFC3339),
		},
		Job: NewNamespacedJob(name, namespace),
	}
}

// NewJobEvent calls [NewNamespacedJobEvent] with [DefaultNamespace].
func NewJobEvent(name string) *JobEvent {
	return NewNamespacedJobEvent(name, DefaultNamespace)
}

// WithFacets sets the supplied instances of [facets.JobFacet] for this event.
func (j *JobEvent) WithFacets(facets ...facets.JobFacet) *JobEvent {
	for _, f := range facets {
		f.Apply(&j.Job.Facets)
	}

	return j
}

// WithInputs appends the supplied instances of [InputElement] to this event's inputs.
func (j *JobEvent) WithInputs(inputs ...InputElement) *JobEvent {
	j.Inputs = append(j.Inputs, inputs...)

	return j
}

// WithOutputs appends the supplied instances of [OutputElement] to this event's outputs.
func (j *JobEvent) WithOutputs(inputs ...OutputElement) *JobEvent {
	j.Outputs = append(j.Outputs, inputs...)

	return j
}

// NewNamespacedJob creates a new [Job].
func NewNamespacedJob(name string, namespace string, jobFacets ...facets.JobFacet) Job {

	var job *facets.JobFacets
	for _, f := range jobFacets {
		f.Apply(&job)
	}

	return Job{
		Name:      name,
		Namespace: DefaultNamespace,
		Facets:    job,
	}
}

// NewJob calls [NewNamespacedJob] with [DefaultNamespace].
func NewJob(name string, jobFacets ...facets.JobFacet) Job {
	return NewNamespacedJob(name, DefaultNamespace, jobFacets...)
}
