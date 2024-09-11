package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

const (
	DefaultEndpoint = "api/v1/lineage"
)

var _ Transport = (*httpTransport)(nil)

type HTTPConfig struct {
	// The URL to send lineage events to (also see OPENLINEAGE_ENDPOINT)
	URL string `yaml:"url" env:"OPENLINEAGE_URL,overwrite"`

	// Endpoint to which events are sent (default: api/v1/lineage)
	Endpoint string `yaml:"endpoint" env:"OPENLINEAGE_ENDPOINT,overwrite,default=api/v1/lineage"`

	// Token included in the Authentication HTTP header as the Bearer
	APIKey string `yaml:"apiKey" env:"OPENLINEAGE_API_KEY,overwrite"`
}

type httpTransport struct {
	httpClient *http.Client
	uri        string
	apiKey     string
}

// Emit implements transport.
func (h *httpTransport) Emit(ctx context.Context, event any) error {
	body, err := json.Marshal(&event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		h.uri,
		bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	if h.apiKey != "" {
		bearer := fmt.Sprintf("Bearer %s", h.apiKey)
		req.Header.Add("Authorization", bearer)
	}

	req.Header.Add("Content-Type", "application/json")

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute POST request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server responded with status %v: %s", resp.StatusCode, body)
	}

	return nil
}
