package openlineage_test

import (
	"testing"

	"github.com/ThijsKoot/openlineage-go"
	"github.com/ThijsKoot/openlineage-go/pkg/transport"

	"github.com/go-test/deep"
)

func Test_ConfigFromEnv(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want openlineage.ClientConfig
	}{
		{
			name: "console-pretty-print",
			env: map[string]string{
				"OPENLINEAGE_TRANSPORT":    "console",
				"OPENLINEAGE_PRETTY_PRINT": "true",
			},
			want: openlineage.ClientConfig{
				Transport: transport.Config{
					Type: transport.TransportTypeConsole,
					Console: &transport.ConsoleConfig{
						PrettyPrint: true,
					},
					HTTP: &transport.HTTPConfig{
						Endpoint: transport.DefaultEndpoint,
					},
				},
				Namespace: "default",
			},
		},
		{
			name: "http",
			env: map[string]string{
				"OPENLINEAGE_TRANSPORT": "http",
				"OPENLINEAGE_URL":       "https://foo",
				"OPENLINEAGE_ENDPOINT":  "bar/api/v1/lineage",
				"OPENLINEAGE_NAMESPACE": "httpns",
			},
			want: openlineage.ClientConfig{
				Transport: transport.Config{
					Type: transport.TransportTypeHTTP,
					HTTP: &transport.HTTPConfig{
						URL:      "https://foo",
						Endpoint: "bar/api/v1/lineage",
					},
				},
				Namespace: "httpns",
			},
		},
		{
			name: "file-env-combi",
			env: map[string]string{
				"OPENLINEAGE_CONFIG":    "testdata/config-http.yaml",
				"OPENLINEAGE_URL":       "https://foo",
				"OPENLINEAGE_NAMESPACE": "httpns",
			},
			want: openlineage.ClientConfig{
				Transport: transport.Config{
					Type: transport.TransportTypeHTTP,
					HTTP: &transport.HTTPConfig{
						URL:      "https://foo",
						Endpoint: "api/v1/lineage",
						APIKey:   "bar",
					},
				},
				Namespace: "httpns",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			got, err := openlineage.ConfigFromEnv()
			if err != nil {
				t.Errorf("ConfigFromEnv failed: %s", err)
			}

			if diff := deep.Equal(tt.want, got); diff != nil {
				t.Errorf("differences found:\n%s", diff)
			}
		})
	}
}

func Test_ConfigFromFile(t *testing.T) {
	cases := []struct {
		name string
		file string
		want openlineage.ClientConfig
	}{
		{
			name: "console",
			file: "testdata/config-console.yaml",
			want: openlineage.ClientConfig{
				Transport: transport.Config{
					Type: transport.TransportTypeConsole,
					Console: &transport.ConsoleConfig{
						PrettyPrint: true,
					},
				},
				Namespace: "console-ns",
				Disabled:  false,
			},
		},
		{
			name: "http",
			file: "testdata/config-http.yaml",
			want: openlineage.ClientConfig{
				Transport: transport.Config{
					Type: transport.TransportTypeHTTP,
					HTTP: &transport.HTTPConfig{
						URL:      "foo",
						APIKey:   "bar",
						Endpoint: "api/v1/lineage",
					},
				},
				Namespace: "http-ns",
				Disabled:  false,
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, err := openlineage.ConfigFromFile(tt.file)
			if err != nil {
				t.Fatalf("ConfigFromFile failed: %s", err.Error())
			}

			if diff := deep.Equal(tt.want, got); diff != nil {
				t.Errorf("differences found:\n%s", diff)
			}
		})
	}
}
