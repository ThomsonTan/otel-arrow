// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Conversion of OTLP logs to InfluxDB line protocol (`logs` measurement).

use std::collections::HashSet;

use otap_df_pdata_views::views::common::{AttributeView, InstrumentationScopeView};
use otap_df_pdata_views::views::logs::{
    LogRecordView, LogsDataView, ResourceLogsView, ScopeLogsView,
};
use otap_df_pdata_views::views::resource::ResourceView;

use super::super::config::Config;
use super::super::line_protocol::{LineBuilder, LinesBatcher};
use super::{
    ConvertStats, FIELD_BODY, FIELD_DROPPED_ATTRS, FIELD_SEVERITY_TEXT, MEASUREMENT_LOGS,
    TAG_SEVERITY_NUMBER, TAG_SPAN_ID, TAG_TRACE_ID, add_scope_tags, append_any_value_field,
    bytes_to_string, find_attr, hex_encode,
};

/// Appends all log records in `logs` to `batcher` as `logs`-measurement lines.
pub fn append_logs<L: LogsDataView>(
    logs: &L,
    cfg: &Config,
    batcher: &mut LinesBatcher,
    stats: &mut ConvertStats,
) {
    let dims: HashSet<&str> = cfg
        .log_record_dimensions
        .iter()
        .map(String::as_str)
        .collect();

    for resource_logs in logs.resources() {
        let resource = resource_logs.resource();
        for scope_logs in resource_logs.scopes() {
            let scope = scope_logs.scope();
            for record in scope_logs.log_records() {
                append_log_record(
                    &record,
                    resource.as_ref(),
                    scope.as_ref(),
                    cfg,
                    &dims,
                    batcher,
                    stats,
                );
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn append_log_record<Rec, R, S>(
    record: &Rec,
    resource: Option<&R>,
    scope: Option<&S>,
    cfg: &Config,
    dims: &HashSet<&str>,
    batcher: &mut LinesBatcher,
    stats: &mut ConvertStats,
) where
    Rec: LogRecordView,
    R: ResourceView,
    S: InstrumentationScopeView,
{
    let mut line = LineBuilder::new(MEASUREMENT_LOGS);

    // ---- Tags ----
    if let Some(trace_id) = record.trace_id() {
        line.tag(TAG_TRACE_ID, hex_encode(trace_id));
    }
    if let Some(span_id) = record.span_id() {
        line.tag(TAG_SPAN_ID, hex_encode(span_id));
    }
    if let Some(severity) = record.severity_number() {
        line.tag(TAG_SEVERITY_NUMBER, severity.to_string());
    }
    if let Some(scope) = scope {
        add_scope_tags(&mut line, scope);
    }
    for dim in &cfg.log_record_dimensions {
        let value = find_attr(record.attributes(), dim)
            .or_else(|| resource.and_then(|r| find_attr(r.attributes(), dim)));
        if let Some(value) = value {
            line.tag(dim.as_str(), value);
        }
    }

    // ---- Timestamp ----
    if let Some(ts) = record
        .time_unix_nano()
        .or_else(|| record.observed_time_unix_nano())
    {
        line.timestamp(ts);
    }

    // ---- Fields ----
    if let Some(body) = record.body() {
        let _ = append_any_value_field(&mut line, FIELD_BODY, &body, stats);
    }
    if let Some(severity_text) = record.severity_text() {
        line.field_str(FIELD_SEVERITY_TEXT, bytes_to_string(severity_text));
    }
    for attr in record.attributes() {
        let key = bytes_to_string(attr.key());
        if dims.contains(key.as_str()) {
            continue;
        }
        if let Some(value) = attr.value() {
            let _ = append_any_value_field(&mut line, &key, &value, stats);
        }
    }
    let dropped = record.dropped_attributes_count();
    if dropped > 0 {
        line.field_u64(FIELD_DROPPED_ATTRS, u64::from(dropped));
    }

    batcher.push(line);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exporters::influxdb_exporter::convert::test_util::{
        cfg, int_val, kv, render_lines, str_val,
    };
    use otap_df_pdata::proto::opentelemetry::common::v1::InstrumentationScope;
    use otap_df_pdata::proto::opentelemetry::logs::v1::{
        LogRecord, LogsData, ResourceLogs, ScopeLogs,
    };
    use otap_df_pdata::proto::opentelemetry::resource::v1::Resource;
    use otap_df_pdata::views::otlp::bytes::logs::RawLogsData;

    fn render(logs: LogsData) -> Vec<String> {
        let config = cfg(serde_json::json!({}));
        render_lines(&config, |batcher, stats| {
            let mut buf = Vec::new();
            prost::Message::encode(&logs, &mut buf).expect("encode");
            let view = RawLogsData::new(&buf);
            append_logs(&view, &config, batcher, stats);
        })
    }

    #[test]
    fn log_record_maps_tags_and_fields() {
        let logs = LogsData {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![
                        kv("service.name", str_val("svc")),
                        kv("host.id", str_val("h1")),
                    ],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "lib".to_string(),
                        version: "1.0".to_string(),
                        ..Default::default()
                    }),
                    log_records: vec![LogRecord {
                        time_unix_nano: 5,
                        severity_number: 9,
                        severity_text: "INFO".to_string(),
                        body: Some(str_val("hello")),
                        trace_id: vec![1u8; 16],
                        span_id: vec![2u8; 8],
                        attributes: vec![kv("http.method", str_val("GET"))],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let lines = render(logs);
        assert_eq!(lines.len(), 1);
        assert_eq!(
            lines[0],
            "logs,otel.library.name=lib,otel.library.version=1.0,service.name=svc,\
severity_number=9,span_id=0202020202020202,\
trace_id=01010101010101010101010101010101 \
body=\"hello\",severity_text=\"INFO\",http.method=\"GET\" 5"
        );
    }

    #[test]
    fn log_record_dimension_falls_back_to_record_attr() {
        // service.name present on the record overrides absence on the resource.
        let logs = LogsData {
            resource_logs: vec![ResourceLogs {
                resource: None,
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: 1,
                        body: Some(int_val(42)),
                        attributes: vec![kv("service.name", str_val("from-record"))],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let lines = render(logs);
        assert_eq!(lines.len(), 1);
        // service.name promoted to a tag (not a field); body is the only field.
        assert_eq!(lines[0], "logs,service.name=from-record body=42i 1");
    }

    #[test]
    fn otlp_bytes_and_otap_backends_agree() {
        use otap_df_pdata::proto::OtlpProtoMessage;
        use otap_df_pdata::testing::round_trip::otlp_to_otap;
        use otap_df_pdata::views::otap::OtapLogsView;

        // A single-field record keeps line ordering deterministic across backends.
        // Scope name/version are intentionally omitted: the OTAP logs view does not
        // surface `otel.library.*`, so including a scope would make the backends
        // disagree on a detail owned by the pdata view layer, not this exporter.
        let logs = LogsData {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", str_val("svc"))],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: 5,
                        severity_number: 9,
                        body: Some(str_val("hi")),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let bytes_lines = render(logs.clone());

        let config = cfg(serde_json::json!({}));
        let records = otlp_to_otap(&OtlpProtoMessage::Logs(logs));
        let otap_lines = render_lines(&config, |batcher, stats| {
            let view = OtapLogsView::try_from(&records).expect("otap logs view");
            append_logs(&view, &config, batcher, stats);
        });

        assert_eq!(bytes_lines, otap_lines);
        assert_eq!(
            bytes_lines,
            vec!["logs,service.name=svc,severity_number=9 body=\"hi\" 5".to_string()]
        );
    }
}
