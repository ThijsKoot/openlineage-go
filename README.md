# OpenLineage SDK for Go [![GoDoc](https://img.shields.io/badge/pkg.go.dev-doc-blue)](http://pkg.go.dev/github.com/ThijsKoot/openlineage-go)

This library contains an SDK for the [OpenLineage project](https://github.com/openlineage/OpenLineage).
Large parts are generated from the OpenLineage specification.

The root of the library contains the core OpenLineage types, facets are located in `pkg/facets`.

## Installing

```sh
go get github.com/ThijsKoot/openlineage-go
```

## Usage

```go
runID := uuid.Must(uuid.NewV7())
facet := facets.NewProcessingEngine("v1.2.3").
    WithName("my-go-engine")

openlineage.NewRunEvent(openlineage.EventTypeStart, runID, "my-job").
    WithRunFacets(facet).
    Emit()
```

### Configuring

All configuration is captured in `openlineage.ClientConfig`, which can be read from the following sources:

- YAML configuration file
- Environment variables
- Variables set in code

```go
// config which prints pretty-printed events to console
cfg := openlineage.ClientConfig{
	Transport: transport.Config{
		Type: transport.TransportTypeConsole,
		Console: &transport.ConsoleConfig{
			PrettyPrint: true,
		},
	},
}

// create a new client using this configuration
client, err := openlineage.NewClient(cfg)
```

#### File

See below for how to read a configuration file and its format.

```go
config, err := openlineage.ConfigFromFile("my/openlineage/config.yaml")
```

```yaml
# example file
namespace: default # default
disabled: false # default

transport:
  type: console
  console:
    prettyPrint: true

  http:
    url: https://foo
    endpoint: api/v1/lineage # default
    apiKey: ""
```

#### Environment

Use `openlineage.ConfigFromEnv` to read configuration values from the environment.
If `OPENLINEAGE_CONFIG` is specified, it is processed first. 
Values from the environment are applied afterwards.

```go
cfg, err := openlineage.ConfigFromEnv()
```

The table below contains an overview of all environment variables.

| Variable                 | Default        | Description                                         |
| ------------------------ | -------------- | --------------------------------------------------- |
| OPENLINEAGE_CONFIG       |                | Path to YAML-file containing configuration          |
| OPENLINEAGE_TRANSPORT    |                | Transport to use. Can be: http, console             |
| OPENLINEAGE_PRETTY_PRINT |                | Pretty-print JSON events if using console transport |
| OPENLINEAGE_NAMESPACE    | default        | Namespace used for emitting events                  |
| OPENLINEAGE_ENDPOINT     | api/v1/lineage | Endpoint on OPENLINEAGE_URL accepting events        |
| OPENLINEAGE_API_KEY      |                | API key for HTTP transport, if required             |
| OPENLINEAGE_URL          |                | URL for HTTP transport                              |
| OPENLINEAGE_DISABLED     | false          | Disable OpenLineage                                 |

### Transport

The SDK supports pluggable transports via the `transport.Transport` interface.

```go
type Transport interface {
	Emit(ctx context.Context, event any) error
}
```

The built-in transports are HTTP and Console.
HTTP uses POST-requests to an endpoint, optionally secured with bearer authentication.
Console prints JSON-formatted events to stdout.

### Run API

The `run` package contains a tracing-like API modeled loosely after OpenTelemetry's.
It is separate from the core functionality because of its opinionated design.
The purpose of this package is to provide an ergonomic way of emitting events within code with less verbosity.
It also allows for implicit passing of Runs with `context.Context` to avoid having to manually propagate a run context.
