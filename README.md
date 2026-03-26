# Judgment Labs — OTLP Metrics Pipeline

A backend system that ingests OpenTelemetry metrics, processes them via a message queue, stores them in a time-series database, and evaluates user-defined rules.

## Services

| Service | Description | Port |
|---|---|---|
| `ingest` | HTTP server accepting OTLP metrics | `4317` |
| `consumer` | Kafka consumer, DB writer (no public API) | — |
| `query` | Read-only HTTP API over stored metrics | `8081` |
| `rules` | Cron scheduler + alert evaluator | — |
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
 consumer               (storage/cmd/consumer)
      │                  no public API
      ├── UpsertMetric
      ├── InsertDataPoints
      │         │
      │         ▼
      │    TimescaleDB       data_points hypertable
      │
      │
 query API (:8081)      (storage/cmd/query)
      │                  read-only, DB only
      ├── GET /v1/metrics
      ├── GET /v1/data_points
      ├── GET /v1/dead_letters
      └── GET /healthz

 rules engine           (cmd/rules)
      │  every 30s, one job per rule
      ▼
   Kafka (Redpanda)     otel.rule-checks topic
      │
      ▼
 evaluator              (cmd/rules — same process)
      │  queries TimescaleDB with rule's window + filter
      ▼
   alerts table         firing / resolved records
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
| `QUERY_ADDR` | `:8081` | Query API server listen address |
| `RULES_FILE` | `/etc/rules/rules.json` | Path to rules JSON config |
| `RULES_CHECKS_TOPIC` | `otel.rule-checks` | Kafka topic for rule check jobs |
| `RULES_CONSUMER_GROUP` | `rules-engine` | Kafka consumer group for evaluator |
| `RULES_INTERVAL` | `30s` | How often rules are dispatched for evaluation |

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
├── cmd/rules/          # rules engine entry point
├── internal/
│   ├── rules/          # rule types, scheduler, evaluator
│   └── store/          # alert repository + DB pool
├── configs/rules.json  # example rule definitions
├── rules/Dockerfile
└── storage/            # queue consumer + DB storage + query API
    ├── cmd/
    │   ├── consumer/   # Kafka consumer (no public API)
    │   └── query/      # read-only HTTP API server
    ├── internal/
    │   ├── api/        # query HTTP handlers
    │   ├── config/     # LoadConsumer() / LoadQuery()
    │   ├── consumer/   # Kafka consumer loop
    │   ├── models/     # OTLP proto helpers
    │   └── storage/    # pgx repository + migrations
    ├── e2e/            # end-to-end tests
    └── Dockerfile      # CMD arg selects consumer or query
```

## Data model

```
metrics            — one row per unique metric name (name, type, unit, description)
data_points        — individual measurements; TimescaleDB hypertable on timestamp
                     columns: value, timestamp, ingestion_timestamp,
                              service_name, attributes (JSONB), resource_attributes (JSONB)
dead_letter_queue  — messages that exceeded the retry limit
alerts             — rule evaluation results (rule_name, metric_name,
                     evaluated_value, threshold, fired_at, status)
```

## Rules engine

Rules are defined in a JSON config file (see `configs/rules.json`). Each rule specifies a metric, optional filters, an aggregation condition, a time window, and an action:

```json
{
  "name": "high-latency-checkout",
  "metric_name": "http.request.duration",
  "filter": { "service.name": "checkout-service", "method": "GET" },
  "condition": { "aggregation": "avg", "operator": ">", "threshold": 500 },
  "window": { "type": "time", "duration": "5m" },
  "action": { "type": "alert", "severity": "warning", "message": "Checkout GET latency exceeds 500ms average" }
}
```

Supported aggregations: `avg`, `p95`, `count`, `value` (any single data point). The `service.name` filter key maps to the `service_name` column; all other keys use JSONB containment on `attributes`.

Every `RULES_INTERVAL` (default 30s), the scheduler publishes one `RuleCheckJob` per rule to the `otel.rule-checks` Kafka topic. The evaluator consumer reads each job, queries TimescaleDB, and writes a `firing` or `resolved` record to the `alerts` table.

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
