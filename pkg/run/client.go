package run

import (
	"context"

	"github.com/ThijsKoot/openlineage-go"
	"github.com/google/uuid"
)

func NewClient(client *openlineage.Client) *Client {
	return &Client{
		olc: client,
	}
}

type Client struct {
	olc *openlineage.Client
}

// NewRun creates a Run.
// If ctx already contains a RunContext, it set as the parent.
// The resulting Run is stored in ctx using [ContextWithRun].
func (c *Client) NewRun(ctx context.Context, job string) (context.Context, Run) {
	r := run{
		client:       c,
		runID:        uuid.Must(uuid.NewV7()),
		jobName:      job,
		jobNamespace: c.olc.Namespace,
	}

	parent := FromContext(ctx)
	if _, isNoop := parent.(*noopRun); !isNoop {
		r.parent = parent
	}

	return ContextWithRun(ctx, &r), &r
}

// StartRun calls NewRun and emits a START event.
// For details, see NewRun.
func (c *Client) StartRun(ctx context.Context, job string) (context.Context, Run) {
	ctx, r := c.NewRun(ctx, job)

	startEvent := r.NewEvent(openlineage.EventTypeStart)
	_ = c.Emit(ctx, startEvent)

	return ctx, r
}

// ExistingRun recreates a Run for a given job and ID.
// The resulting Run is stored in ctx using [ContextWithRun].
func (c *Client) ExistingRun(ctx context.Context, job string, runID uuid.UUID) (context.Context, Run) {
	r := run{
		client:       c,
		runID:        runID,
		jobName:      job,
		jobNamespace: c.olc.Namespace,
	}

	return ContextWithRun(ctx, &r), &r
}

func (c *Client) Emit(ctx context.Context, event openlineage.Emittable) error {
	return c.olc.Emit(ctx, event)
}

// New calls [Client.New] using [openlineage.DefaultClient].
func New(ctx context.Context, job string) (context.Context, Run) {
	return NewClient(openlineage.DefaultClient).NewRun(ctx, job)
}

// Start calls [Client.Start] using [openlineage.DefaultClient].
func Start(ctx context.Context, job string) (context.Context, Run) {
	return NewClient(openlineage.DefaultClient).StartRun(ctx, job)
}

// Existing calls [Client.ExistingRun] using [openlineage.DefaultClient].
func Existing(ctx context.Context, job string, runID uuid.UUID) (context.Context, Run) {
	return NewClient(openlineage.DefaultClient).ExistingRun(ctx, job, runID)
}
