package openlineage

import (
	"context"
	"fmt"

	"github.com/ThijsKoot/openlineage-go/pkg/transport"
)

var DefaultClient, _ = NewClient(ClientConfig{
	Transport: transport.Config{
		Type: transport.TransportTypeConsole,
		Console: &transport.ConsoleConfig{
			PrettyPrint: true,
		},
	},
})

func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.Disabled {
		return &Client{
			disabled: true,
		}, nil
	}

	transport, err := transport.New(cfg.Transport)
	if err != nil {
		return nil, fmt.Errorf("create transport: %w", err)
	}

	namespace := cfg.Namespace
	if cfg.Namespace == "" {
		namespace = "default"
	}

	return &Client{
		transport: transport,
		Namespace: namespace,
	}, nil
}

type Client struct {
	disabled  bool
	transport transport.Transport
	Namespace string
}

type Emittable interface {
	AsEmittable() Event
}

func (olc *Client) Emit(ctx context.Context, event Emittable) error {
	if olc.disabled {
		return nil
	}

	return olc.transport.Emit(ctx, event.AsEmittable())
}
