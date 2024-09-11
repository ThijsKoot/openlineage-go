package openlineage_test

import (
	"context"
	"log/slog"

	"github.com/ThijsKoot/openlineage-go"
	"github.com/ThijsKoot/openlineage-go/pkg/transport"
	"github.com/google/uuid"
)

func ExampleClient() {
	cfg := openlineage.ClientConfig{
		Transport: transport.Config{
			Type: transport.TransportTypeConsole,
			Console: &transport.ConsoleConfig{
				PrettyPrint: true,
			},
		},
	}

	client, err := openlineage.NewClient(cfg)
	if err != nil {
		slog.Error("ol.NewClient failed", "error", err)
	}

	ctx := context.Background()
	runID := uuid.Must(uuid.NewV7())
	event := openlineage.NewRunEvent(openlineage.EventTypeStart, runID, "foo-job")

	if err := client.Emit(ctx, event); err != nil {
		slog.Error("emitting event failed", "error", err)
	}
}
