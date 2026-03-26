# Judgment Labs — OTLP Metrics Pipeline

A backend system that ingests OpenTelemetry metrics, processes them via a message queue, stores them in a time-series database, and evaluates user-defined rules.

## Services

| Service | Description | Port |
|---|---|---|
| `ingest` | HTTP server accepting OTLP metrics | `4317` |
| `storage` | Queue consumer, DB writer, query API | `8081` |
| `redpanda` | Kafka-compatible broker | `19092` |

## Architecture

```
POST /v1/metrics
      │
      ▼
 ingest server          (ingest/)
      │  binary proto + ingested_at header
      ▼
   Kafka (Redpanda)     otel.metrics topic
      │
      ▼
 storage consumer       (storage/)
      │
      ├── UpsertMetric
      ├── InsertDataPoints
      │         │
      │         ▼
      │    TimescaleDB       data_points hypertable
      │
      └── query API (:8081)
            GET /v1/data_points
            GET /v1/dead_letters
            GET /healthz
```

Failed messages are retried up to `MAX_RETRIES` times, then written to the `dead_letter_queue` table.

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

# Storage layer (DB + Kafka consumer)
cd storage && make e2e

# Ingest server
cd ingest && make e2e

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
| `CONSUMER_GROUP` | `storage-consumers` | Kafka consumer group |
| `MAX_RETRIES` | `3` | Retry attempts before dead-letter |
| `DATABASE_URL` | `postgres://otel:otel@localhost:5432/otel_metrics` | TimescaleDB DSN |
| `STORAGE_ADDR` | `:8081` | Storage consumer query API listen address |

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
└── storage/            # queue consumer + DB storage
    ├── cmd/consumer/   # entry point
    ├── internal/
    │   ├── api/        # query HTTP API
    │   ├── consumer/   # Kafka consumer loop
    │   ├── models/     # OTLP proto helpers
    │   └── storage/    # pgx repository + migrations
    ├── e2e/            # end-to-end tests
    └── Dockerfile
```

## Data model

```
metrics            — one row per unique metric name (name, type, unit, description)
data_points        — individual measurements; TimescaleDB hypertable on timestamp
                     columns: value, timestamp, ingestion_timestamp,
                              service_name, attributes (JSONB), resource_attributes (JSONB)
dead_letter_queue  — messages that exceeded the retry limit
```

### Example queries

```sql
-- Recent data points for a service
SELECT m.name, dp.value, dp.timestamp, dp.attributes
FROM data_points dp
JOIN metrics m ON m.id = dp.metric_id
WHERE dp.service_name = 'checkout-service'
  AND dp.timestamp > NOW() - INTERVAL '5 minutes'
ORDER BY dp.timestamp DESC;

-- Average latency per method over last 5 minutes
SELECT dp.attributes->>'method' AS method,
       AVG(dp.value)            AS avg_ms
FROM data_points dp
JOIN metrics m ON m.id = dp.metric_id
WHERE m.name = 'http.request.duration'
  AND dp.timestamp > NOW() - INTERVAL '5 minutes'
GROUP BY method;

-- Attribute containment filter (GIN index)
SELECT * FROM data_points
WHERE attributes @> '{"method": "GET"}';
```
