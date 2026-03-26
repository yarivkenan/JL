---

# Overview

**Duration**: 4 hours
**Format**: Hands-on coding. Localhost only (though, feel free to use cloud services if that is easier or you feel this is doable in the time permitted)

**Goal**: Build a backend system that ingests OpenTelemetry metrics, processes them via a message queue, stores them in a database, evaluates user-defined rules against those metrics and exposes a CLI for interacting with the system.

This is an end-to-end design and implementation exercise. You are expected to produce clean, production-quality code. We are evaluating architecture decisions, code organization, and your ability to reason about extensibility and reliability, not just correctness.

---

# Constraints and Setup

- Language: Candidate's choice.  Pick what you're strongest in.
- AI tools are allowed. Cursor, Claude Code, GitHub Copilot, ChatGPT, or any other AI assistant are all fair game. We evaluate the output and your reasoning, not whether you typed every character. You are expected to understand and be able to explain every line of code in your submission.
- As a general guideline, you will likely want some kind of API server, some appropriate database, and some appropriate queue / worker system.
- Importantly, imagine we are trying to build a baseline codebase that will be iterated on by multiple developers overtime. This means we do care about a codebase that has **established practices** for development. This needs to be **maintainable** and **scalable**. Imagine that schemas and API routes may change overtime, like in any large scale software application. Therefore we want to see how we build this end to end setup that allows us to iterate safely.

---

# OpenTelemetry References

The following specification defines the format and semantics you should follow.

- https://opentelemetry.io/docs/specs/otlp/
    - OTLP/HTTP section: https://opentelemetry.io/docs/specs/otlp/#otlphttp
    - What is a Gauge: https://opentelemetry.io/docs/specs/otel/metrics/data-model/#gauge
    - What is a Sum: https://opentelemetry.io/docs/specs/otel/metrics/data-model/#gauge
- Opentelemetry Protobuf
    - https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto

---

## What We're Looking For:

- Clean separation of concerns (ingestion, processing, storage, rules, CLI)
- Readable code
- Thoughtful data modeling
- Extensible design, rather than clever one-offs
- Error handling that reflects real-world usage
- Sensible use of abstractions (not over-engineered, but also not under-engineered)

---

Time Breakdown

| Phase | Task  | Suggested Time |
| --- | --- | --- |
| 1 | Understand the problem prompt, opentelementry specs, high level approach that will be taken, etc | 30 minutes |
| 2 | Metrics ingestion server + queue | 45 min  |
| 3 | Queue consumers + database storage | 45 min |
| 4 | Rules engine + automation | 45 min |
| 5 | CLI | 45 min |
| 6 | Debrief with a member of the Judgment Labs team | 30 min |

These are general guidelines. If you move fast on early phases you should invest in quality on later ones. Partial completion is fine as long as what exists is solid.

---

## Phase 1: Scoping (30 min)

### Objective

Understand the problem prompt, look through the Opentelemetry links to understand the specification, and think about a high level approach for solving the problem.

## Phase 2: Metrics Ingestion Server (45 min)

### Objective

Build an HTTP server that accepts OpenTelemetry v1 metrics payloads, validates them, and enqueues a job for each ingested metric batch.

### Requirements

Endpoint: `POST /v1/metrics`

Input format: The body of the POST request is a payload either in binary-encoded Protobuf format or in JSON-encoded Protobuf format following the OTEL v1 [spec](https://opentelemetry.io/docs/specs/otlp/#otlphttp-request).

You should handle at minimum gauge and sum metric types.

---

## Phase 3: Queue Consumers + Database Storage (45 min)

### Objective

Write one or more consumers that read from the queue, parse the job payloads, and persist the metrics to a database.

### Requirements

Consumer behavior:

- Consume messages from the queue produced in Phase 1.
- Parse each message and store the metric in the database.
- Handle failures gracefully. A failed message should not be silently dropped.
- The consumer should be runnable as a separate process from the ingestion server.

Data model:

Design a schema that captures:

- Metric name, unit, description
- Metric type (gauge, sum, etc.)
- Individual data points, including their numeric value and timestamp
- Attributes/labels on both the resource and the data point
- The service name (extracted from resource attributes)
- Ingestion timestamp (when your system received it, not the OTel timestamp)

The schema should be queryable. Interviewers will ask the candidate to write queries during Phase 4.

---

## Phase 4: Rules Engine + Automation (45 min)

### Objective

Allow users to define rules that evaluate stored metrics and trigger automation (e.g., log an alert, write to an alerts table, call a webhook) when thresholds are breached.

### Requirements

Rule definition:

Users define rules through the CLI or via a config file. A rule specifies:

- name: Human-readable identifier
- metric_name: The metric to evaluate
- filter: Optional attribute filters (e.g., service.name = "checkout-service")
- condition: A threshold condition, e.g.:
    - avg > 500 (over last N data points or a time window)
    - p95 > 1000
    - count > 100
    - value > 200 (for any single data point)
- window: Time window or data point window for aggregation (e.g., last 5 minutes, last 100 points)
- action: What to trigger when the condition is met. At minimum, support writing an alert record to the database. A webhook POST is a
stretch goal.

Example rule (any format acceptable: JSON, YAML, DSL):

```json
{
  "name": "high-latency-checkout",
  "metric_name": "http.request.duration",
  "filter": {
    "service.name": "checkout-service",
    "method": "GET"
  },
  "condition": {
    "aggregation": "avg",
    "operator": ">",
    "threshold": 500
  },
  "window": {
    "type": "time",
    "duration": "5m"
  },
  "action": {
    "type": "alert",
    "severity": "warning",
    "message": "Checkout GET latency exceeds 500ms average"
  }
}
```

Rule evaluation:

- Rules should be evaluated either on a periodic schedule (e.g., every 30 seconds) or triggered on each consumer write.
- Evaluation reads from the database using the rule's window and filter.
- If the condition is met, an alert record is written to the database with:
    - Rule name
    - Metric name
    - Evaluated value
    - Threshold
    - Timestamp
    - Status: firing or resolved
- Exact rule schema/structure is up to the candidate (feel free to leverage part or all of the example rule above)

---

## Phase 5: CLI (45 min)

### Objective

Build a CLI that communicates with the backend system and allows a user to inspect metrics, rules, and alerts. In terms of structure, the CLI should be a clean and organized abstraction over the API server we are building. Keep in mind, many developers may be working on the API and constantly making updates over time. We would like this CLI to have a clear way of how to maintain it when backend changes are made.

### Requirements

The CLI should support the following commands. Exact flag syntax is up to the candidate.

### Metrics

1. 

`metrics list
--service <name>       Filter by service name
--metric <name>        Filter by metric name
--since <duration>     e.g. "1h", "30m"
--limit <n>            Max results (default 50)`

Displays: a list of metrics that meet the filter criteria

1. 

`metrics inspect <metric-name>
--service <name>
--since <duration>`

Displays: name, type, unit, recent data points with timestamps and attributes

### Rules

1. 

`rules list`
Shows all defined rules with their condition and action

1. 

`rules create`
Interactive prompt or flags to define a new rule

1. 

`rules delete <name>` 

deletes a rule

### Alerts

1. 

`alerts list
--rule <name> Filter by rule
--since <duration>
--status firing|resolved`

Shows: rule name, metric, evaluated value, threshold, timestamp, status

1. 

`alerts inspect <alert-id>`

Full detailed view of a single alert

### General expectations

- Output should be human-readable. Tables or structured text, not raw JSON by default.
- -json flag on any command that outputs data is a nice-to-have.
- The CLI should communicate with the server over HTTP.

---

## Phase 6: Debrief with a member of the Judgment Labs team

1. How did you feel about the overall exercise?
2. What would you change about your architecture if you had more time?
3. Where did you feel most constrained by the time limit?
4. Walk me through your data model. Help me understand the attribute storage, schema design, and query patterns.
5. How extensible is your rules engine? Walk me through how you’d add a new condition type, or extend the rules engine in another way.
6. What's the weakest part of your implementation?
7. How did you use AI tools, and where did they help or slow you down?
8. What would you need to productionize this?
9. Was anything in the spec unclear or underspecified?
10. What questions do you have for us?