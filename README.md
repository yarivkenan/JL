# Judgment Labs — OTLP Metrics Pipeline

A backend system that ingests OpenTelemetry metrics, processes them via a message queue, stores them in a database, and evaluates user-defined rules.

## Services

| Service | Description | Port |
|---|---|---|
| `ingest` | HTTP server accepting OTLP metrics | `4317` |
| `storage` | Queue consumer, DB writer, rules engine | TBD |
| `redpanda` | Kafka-compatible broker | `19092` |
| `redpanda-console` | Broker web UI | `8080` |

## Requirements

- Go 1.26+
- Docker + Docker Compose

## Getting started

```bash
# Start all services
make up

# Stop all services
make down

# Follow logs
make logs
```

## Testing

```bash
# Run ingest e2e tests (spins up compose, tests, tears down)
make e2e

# Send a test metric manually
curl -X POST http://localhost:4317/v1/metrics \
  -H "Content-Type: application/json" \
  -d '{
    "resourceMetrics": [{
      "resource": {
        "attributes": [{"key": "service.name", "value": {"stringValue": "my-service"}}]
      },
      "scopeMetrics": [{
        "metrics": [{
          "name": "http.request.duration",
          "gauge": {
            "dataPoints": [{"asDouble": 123.4, "timeUnixNano": "1700000000000000000"}]
          }
        }]
      }]
    }]
  }'
```

## Configuration

All services are configured via environment variables. Defaults work out of the box with `make up`.

| Variable | Default | Description |
|---|---|---|
| `SERVER_ADDR` | `:4317` | Ingest server listen address |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker list |
| `KAFKA_TOPIC` | `otel.metrics` | Topic for ingested metric batches |

## Repository layout

```
JL/
├── go.mod              # shared Go module (github.com/yarivkenan/JL)
├── docker-compose.yml  # all services
├── Makefile            # up / down / logs / e2e
├── ingest/             # OTLP ingestion service
│   ├── cmd/server/
│   ├── internal/
│   │   ├── config/     # env-based config
│   │   ├── handler/    # POST /v1/metrics
│   │   ├── otel/       # OTLP parsing + validation
│   │   ├── queue/      # Publisher interface + Kafka impl
│   │   └── server/     # chi router + middleware
│   ├── e2e/            # end-to-end tests
│   └── Dockerfile
└── storage/            # (coming soon)
```
