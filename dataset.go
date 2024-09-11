package openlineage

import (
	"context"
	"time"

	"github.com/ThijsKoot/openlineage-go/pkg/facets"
)

// DatasetEvent represents an OpenLineage DatasetEvent.
type DatasetEvent struct {
	Dataset Dataset

	BaseEvent
}

func (e *DatasetEvent) AsEmittable() Event {
	return Event{
		EventTime: e.EventTime,
		Dataset:   &e.Dataset,
		Producer:  e.Producer,
		SchemaURL: e.SchemaURL,
	}
}

// Emit calls [Client.Emit] on [DefaultClient].
func (e *DatasetEvent) Emit() {
	_ = DefaultClient.Emit(context.Background(), e)
}

func NewDatasetEvent(
	name string,
	namespace string,
	facets ...facets.DatasetFacet,
) DatasetEvent {
	return DatasetEvent{
		BaseEvent: BaseEvent{
			Producer:  producer,
			SchemaURL: schemaURL,
			EventTime: time.Now().Format(time.RFC3339),
		},
		Dataset: NewDataset(name, namespace, facets...),
	}
}

func NewDataset(name string, namespace string, datasetFacets ...facets.DatasetFacet) Dataset {
	var dataset *facets.DatasetFacets
	for _, f := range datasetFacets {
		f.Apply(&dataset)
	}

	return Dataset{
		Name:      name,
		Namespace: namespace,
		Facets:    dataset,
	}
}
