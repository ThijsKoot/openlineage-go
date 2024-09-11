package transport

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/hashicorp/go-retryablehttp"
)

const (
	TransportTypeHTTP    TransportType = "http"
	TransportTypeConsole TransportType = "console"
)

type Transport interface {
	Emit(ctx context.Context, event any) error
}

type TransportType string

type Config struct {
	Type    TransportType  `yaml:"type" env:"OPENLINEAGE_TRANSPORT,overwrite"`
	Console *ConsoleConfig `yaml:"console,omitempty" env:",noinit"`
	HTTP    *HTTPConfig    `yaml:"http,omitempty" env:",noinit"`
}

func New(config Config) (Transport, error) {
	switch config.Type {
	case TransportTypeConsole:
		return &consoleTransport{
			prettyPrint: config.Console.PrettyPrint,
		}, nil
	case TransportTypeHTTP:
		httpClient := retryablehttp.NewClient().StandardClient()

		u, err := url.Parse(config.HTTP.URL)
		if err != nil {
			return nil, fmt.Errorf("parsing URL \"%s\" failed: %w", config.HTTP.URL, err)
		}

		ep := config.HTTP.Endpoint
		if ep == "" {
			ep = "api/v1/lineage"
		}

		u = u.JoinPath(ep)

		return &httpTransport{
			httpClient: httpClient,
			uri:        u.String(),
			apiKey:     config.HTTP.APIKey,
		}, nil
	default:
		return nil, errors.New("no valid transport specified")
	}
}
