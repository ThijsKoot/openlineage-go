package run_test

import (
	"context"
	"errors"
	"log/slog"

	ol "github.com/ThijsKoot/openlineage-go"
	"github.com/ThijsKoot/openlineage-go/pkg/run"
	"github.com/ThijsKoot/openlineage-go/pkg/transport"
)

func ExampleRun() {
	ctx := context.Background()

	cfg := ol.ClientConfig{
		Transport: transport.Config{
			Type: transport.TransportTypeConsole,
			Console: &transport.ConsoleConfig{
				PrettyPrint: true,
			},
		},
	}
	olClient, err := ol.NewClient(cfg)
	if err != nil {
		slog.Error("ol.NewClient failed", "error", err)
	}

	runClient := run.NewClient(olClient)

	ctx, run := runClient.StartRun(ctx, "ingest")
	defer run.Finish()

	if err := ChildFunction(ctx); err != nil {
		run.RecordError(err)

		slog.Warn("child function failed", "error", err)
	}

}

func ChildFunction(ctx context.Context) error {
	parent := run.FromContext(ctx)
	_, childRun := parent.StartChild(ctx, "child")
	defer childRun.Finish()

	if err := DoWork(); err != nil {
		// Record the error in this run.
		// Finish() will emit a FAIL event.
		childRun.RecordError(err)

		return err
	}

	return nil
}

func DoWork() error {
	return errors.New("did not do work")
}
