package openlineage

import (
	"context"
	"fmt"
	"os"

	"github.com/ThijsKoot/openlineage-go/pkg/transport"
	"github.com/sethvargo/go-envconfig"
	"gopkg.in/yaml.v3"
)

type ClientConfig struct {
	Transport transport.Config `yaml:"transport"`

	// Namespace for events. Defaults to "default"
	Namespace string `yaml:"namespace" env:"OPENLINEAGE_NAMESPACE, overwrite, default=default"`

	// When true, OpenLineage will not emit events (default: false)
	Disabled bool `yaml:"disabled" env:"OPENLINEAGE_DISABLED, overwrite"`
}

// ConfigFromEnv attempts to parse [ClientConfig] from the environment.
// If OPENLINEAGE_CONFIG_FILE is specified, it will be read first.
// Environment variables take precedence over values from the configuration file.
func ConfigFromEnv() (ClientConfig, error) {
	var config ClientConfig

	configFile := os.Getenv("OPENLINEAGE_CONFIG")
	if configFile != "" {
		c, err := ConfigFromFile(configFile)
		if err != nil {
			return ClientConfig{}, fmt.Errorf("parsing OPENLINEAGE_CONFIG_FILE: %w", err)
		}

		config = c
	}

	if err := envconfig.Process(context.Background(), &config); err != nil {
		return ClientConfig{}, fmt.Errorf("unable to parse config from environment: %w", err)
	}

	return config, nil
}

// ConfigFromFile reads a configuration file in YAML-format from the specified location.
func ConfigFromFile(location string) (ClientConfig, error) {
	f, err := os.ReadFile(location)
	if err != nil {
		return ClientConfig{}, fmt.Errorf("read config file: %w", err)
	}

	var cfg ClientConfig
	if err := yaml.Unmarshal(f, &cfg); err != nil {
		return ClientConfig{}, fmt.Errorf("unmarshal config file: %w", err)
	}

	return cfg, nil
}
