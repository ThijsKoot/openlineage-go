package openlineage

import (
	"github.com/ThijsKoot/openlineage-go/pkg/facets"
)

const (
	producer  = "openlineage-go"
	schemaURL = "foo"
)

var DefaultNamespace = "default"

type BaseEvent struct {
	// the time the event occurred at
	EventTime string
	// URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
	Producer string
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this RunEvent
	SchemaURL string
}

func NewInputElement(name string, namespace string) InputElement {
	return InputElement{
		Name:      name,
		Namespace: namespace,
	}
}

func (ie InputElement) WithInputFacets(facets ...facets.InputDatasetFacet) InputElement {
	for _, f := range facets {
		f.Apply(&ie.InputFacets)
	}

	return ie
}

func (ie InputElement) WithFacets(facets ...facets.DatasetFacet) InputElement {
	for _, f := range facets {
		f.Apply(&ie.Facets)
	}

	return ie
}

func NewOutputElement(name string, namespace string) OutputElement {
	return OutputElement{
		Name:      name,
		Namespace: namespace,
	}
}

func (oe OutputElement) WithOutputFacets(facets ...facets.OutputDatasetFacet) OutputElement {
	for _, f := range facets {
		f.Apply(&oe.OutputFacets)
	}

	return oe
}

func (oe OutputElement) WithFacets(facets ...facets.DatasetFacet) OutputElement {
	for _, f := range facets {
		f.Apply(&oe.Facets)
	}

	return oe
}
