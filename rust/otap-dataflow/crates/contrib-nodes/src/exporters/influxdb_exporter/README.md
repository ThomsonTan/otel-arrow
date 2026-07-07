# InfluxDB Exporter

## Metadata

- Type: `exporter:influxdb`
- Full URN: `urn:otel:exporter:influxdb`
- Feature gate: `influxdb-exporter`
- Stability: Alpha; supports metrics, logs, and traces

## Overview

The InfluxDB Exporter writes OpenTelemetry metrics, logs, and traces to InfluxDB
using the v2 write API (`POST {endpoint}/api/v2/write`). The OTLP-to-line-protocol
mapping follows the OpenTelemetry Collector Contrib
[`influxdbexporter`][collector-influxdb], which is built on influxdata's
[`otel2influx`][otel2influx] library. The v2 write API also covers InfluxDB 3.x
endpoints that expose the v2-compatible write path.

Conversion is written once against backend-agnostic view traits, so both the
OTLP-bytes and OTAP-Arrow pipeline formats are supported without an intermediate
re-encode.

## Getting Started

```yaml
type: exporter:influxdb
config:
  endpoint: "http://localhost:8086"
  org: "my-org"
  bucket: "my-bucket"
  token: "${env:INFLUXDB_TOKEN}"
```

## Build df_engine with the InfluxDB Exporter

From the `otap-dataflow` directory:

```bash
cargo build --release --features influxdb-exporter
```

The exporter is also included in the `contrib-exporters` aggregate feature.

## Configuration

| Field                   | Type        | Default                          | Description                                                                 |
| ----------------------- | ----------- | -------------------------------- | --------------------------------------------------------------------------- |
| `endpoint`              | string      | (required)                       | Base URL of the InfluxDB server (scheme + host + port). `/api/v2/write` is appended automatically. |
| `org`                   | string      | (required)                       | Destination organization.                                                   |
| `bucket`                | string      | (required)                       | Destination bucket.                                                         |
| `token`                 | string      | (required)                       | Auth token, sent as `Authorization: Token <token>`. Redacted in logs.       |
| `metrics_schema`        | enum        | `telegraf-prometheus-v1`         | `telegraf-prometheus-v1` or `telegraf-prometheus-v2`.                        |
| `span_dimensions`       | list        | `["service.name", "span.name"]` | Span/resource attributes promoted to tags on the `spans` measurement.       |
| `log_record_dimensions` | list        | `["service.name"]`              | Log/resource attributes promoted to tags on the `logs` measurement.         |
| `payload_max_lines`     | integer     | `10000`                          | Maximum line-protocol lines per write request.                              |
| `payload_max_bytes`     | integer     | `10000000`                       | Maximum request body size in bytes.                                         |
| `precision`             | enum        | `ns`                             | Timestamp precision: `ns`, `us`, `ms`, or `s`.                              |
| `http`                  | object      | see below                        | HTTP client settings (TLS, timeouts, headers, compression).                 |

The `http` object accepts the shared HTTP client settings (connect timeout,
TLS/mTLS, `compression` = `gzip`/`zstd`/`deflate`, extra `headers`, custom
`user_agent`, etc.). Header values are treated as secrets and redacted.

```yaml
type: exporter:influxdb
config:
  endpoint: "https://influx.example.com:8086"
  org: "acme"
  bucket: "telemetry"
  token: "${env:INFLUXDB_TOKEN}"
  metrics_schema: telegraf-prometheus-v2
  precision: ns
  span_dimensions: ["service.name", "span.name"]
  log_record_dimensions: ["service.name", "deployment.environment"]
  http:
    timeout: 30s
    compression: gzip
```

## Line-Protocol Semantics

### Metrics

Resource attributes, instrumentation scope (`otel.library.name` /
`otel.library.version`), and data-point attributes are promoted to tags.

`telegraf-prometheus-v1` (measurement per metric):

- Gauge -> field `gauge`.
- Monotonic cumulative sum -> field `counter`; non-monotonic sum -> field `gauge`.
- Histogram -> fields `count`, `sum`, and one cumulative field per bucket bound
  (field key = bound, plus `+Inf`).
- Summary -> fields `count`, `sum`, and one field per quantile (field key = quantile).

`telegraf-prometheus-v2` (single `prometheus` measurement):

- Gauge / sum -> field named after the metric.
- Histogram -> `{metric}_count` / `{metric}_sum` points, plus one `{metric}_bucket`
  point per bucket carrying an `le` tag (cumulative counts; `+Inf` overflow bucket).
- Summary -> `{metric}_count` / `{metric}_sum` points, plus one `{metric}` point per
  quantile carrying a `quantile` tag.

Exponential histograms are unsupported by both schemas and are dropped (counted in
`influxdb.exporter.exp_histograms_dropped`).

### Logs

Measurement `logs`. Tags: `trace_id`, `span_id`, `severity_number`,
`otel.library.*`, and any attribute in `log_record_dimensions` (resolved against
the log record first, then the resource). Fields: `body`, `severity_text`, the
remaining (non-dimension) log-record attributes, and `dropped_attributes_count`
when non-zero. Timestamp = `time_unix_nano` (falling back to
`observed_time_unix_nano`).

### Traces

Measurement `spans`. Tags: `trace_id`, `span_id`, `parent_span_id`, `trace_state`,
`name`, `kind`, `otel.status_code`, `otel.library.*`, and any attribute in
`span_dimensions`. Fields: `end_time_unix_nano`, `duration_nano`,
`otel.status_description`, `otel.span.attributes` (a JSON object of the remaining
non-dimension attributes), and dropped counts when non-zero. Timestamp = span
start time.

Span events map to the `logs` measurement (tagged with the parent `trace_id` /
`span_id`); span links map to the `span-links` measurement (with
`linked_trace_id` / `linked_span_id` tags).

Complex attribute values (arrays and key-value lists) are JSON-encoded; byte
values are hex-encoded; non-finite floating-point field values are dropped
(counted in `influxdb.exporter.invalid_points_dropped`).

## Delivery Semantics

Each pdata message is converted and its line-protocol payloads are written
sequentially. On full success the message is acknowledged; on the first failure
it is negatively acknowledged and the write stops. Nacks are classified:

- Retryable: HTTP 429, any 5xx, and connect/timeout transport errors
  (`permanent = false`).
- Permanent: other 4xx responses and conversion errors (`permanent = true`).

Retries are delegated upstream via the nack rather than retried in-exporter. A
message whose payload spans multiple write requests may leave earlier chunks
written if a later chunk fails; because InfluxDB points are idempotent upserts on
(measurement, tag set, timestamp), upstream retries are safe.

## Telemetry

Metric set `influxdb.exporter`:

| Metric                            | Description                                             |
| --------------------------------- | ------------------------------------------------------ |
| `write_requests`                  | Write requests issued to the v2 write API.             |
| `write_failures_retryable`        | Write requests that failed with a retryable error.     |
| `write_failures_permanent`        | Write requests that failed with a permanent error.     |
| `lines_written`                   | Line-protocol lines written.                           |
| `exp_histograms_dropped`          | Exponential-histogram data points dropped.             |
| `invalid_points_dropped`          | Points dropped for having no representable field value.|

The generic `exporter.pdata` metric set (consumed / exported / failed per signal)
is also reported.

## Limits

- InfluxDB v2 write API only; no legacy v1 `/write` compatibility mode.
- Writes are sequential (no in-flight concurrency window yet).
- Exponential histograms are not exported.

## Related Docs

- [`docs/urns.md`](../../../../docs/urns.md) - node URN format.
- [Contrib nodes catalog](../../../README.md).

[collector-influxdb]: https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/influxdbexporter
[otel2influx]: https://github.com/influxdata/influxdb-observability/tree/main/otel2influx
