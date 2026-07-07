// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Conversion of OTLP traces to InfluxDB line protocol.
//!
//! Spans map to the `spans` measurement, span events to the `logs` measurement,
//! and span links to the `span-links` measurement.

use std::collections::HashSet;

use otap_df_pdata_views::views::common::{AttributeView, InstrumentationScopeView};
use otap_df_pdata_views::views::resource::ResourceView;
use otap_df_pdata_views::views::trace::{
    EventView, LinkView, ResourceSpansView, ScopeSpansView, SpanView, StatusView, TracesView,
};

use super::super::config::Config;
use super::super::line_protocol::{LineBuilder, LinesBatcher};
use super::{
    ConvertStats, FIELD_DROPPED_ATTRS, FIELD_DROPPED_EVENTS, FIELD_DROPPED_LINKS, FIELD_DURATION,
    FIELD_END_TIME, FIELD_NAME, FIELD_SPAN_ATTRIBUTES, FIELD_STATUS_DESCRIPTION, FIELD_TRACE_STATE,
    MEASUREMENT_LOGS, MEASUREMENT_SPAN_LINKS, MEASUREMENT_SPANS, TAG_LINKED_SPAN_ID,
    TAG_LINKED_TRACE_ID, TAG_NAME, TAG_PARENT_SPAN_ID, TAG_SPAN_ID, TAG_SPAN_KIND, TAG_STATUS_CODE,
    TAG_TRACE_ID, TAG_TRACE_STATE, add_scope_tags, any_value_to_json, append_any_value_field,
    bytes_to_string, find_attr, hex_encode,
};

/// Appends all spans (and their events and links) in `traces` to `batcher`.
pub fn append_traces<T: TracesView>(
    traces: &T,
    cfg: &Config,
    batcher: &mut LinesBatcher,
    stats: &mut ConvertStats,
) {
    let dims: HashSet<&str> = cfg.span_dimensions.iter().map(String::as_str).collect();

    for resource_spans in traces.resources() {
        let resource = resource_spans.resource();
        for scope_spans in resource_spans.scopes() {
            let scope = scope_spans.scope();
            for span in scope_spans.spans() {
                append_span(
                    &span,
                    resource.as_ref(),
                    scope.as_ref(),
                    cfg,
                    &dims,
                    batcher,
                );
                append_span_events(&span, batcher, stats);
                append_span_links(&span, batcher, stats);
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn append_span<Sp, R, S>(
    span: &Sp,
    resource: Option<&R>,
    scope: Option<&S>,
    cfg: &Config,
    dims: &HashSet<&str>,
    batcher: &mut LinesBatcher,
) where
    Sp: SpanView,
    R: ResourceView,
    S: InstrumentationScopeView,
{
    let mut line = LineBuilder::new(MEASUREMENT_SPANS);

    // ---- Tags ----
    if let Some(trace_id) = span.trace_id() {
        line.tag(TAG_TRACE_ID, hex_encode(trace_id));
    }
    if let Some(span_id) = span.span_id() {
        line.tag(TAG_SPAN_ID, hex_encode(span_id));
    }
    if let Some(parent) = span.parent_span_id() {
        line.tag(TAG_PARENT_SPAN_ID, hex_encode(parent));
    }
    if let Some(trace_state) = span.trace_state() {
        line.tag(TAG_TRACE_STATE, bytes_to_string(trace_state));
    }
    if let Some(name) = span.name() {
        line.tag(TAG_NAME, bytes_to_string(name));
    }
    line.tag(TAG_SPAN_KIND, span_kind_str(span.kind()));
    if let Some(status) = span.status() {
        line.tag(TAG_STATUS_CODE, status_code_str(status.status_code()));
    }
    if let Some(scope) = scope {
        add_scope_tags(&mut line, scope);
    }
    for dim in &cfg.span_dimensions {
        let value = find_attr(span.attributes(), dim)
            .or_else(|| resource.and_then(|r| find_attr(r.attributes(), dim)));
        if let Some(value) = value {
            line.tag(dim.as_str(), value);
        }
    }

    // ---- Timestamp (span start) ----
    let start = span.start_time_unix_nano();
    if let Some(start) = start {
        line.timestamp(start);
    }

    // ---- Fields ----
    let end = span.end_time_unix_nano();
    if let Some(end) = end {
        line.field_i64(FIELD_END_TIME, i64::try_from(end).unwrap_or(i64::MAX));
    }
    if let (Some(start), Some(end)) = (start, end) {
        let duration = end.saturating_sub(start);
        line.field_i64(FIELD_DURATION, i64::try_from(duration).unwrap_or(i64::MAX));
    }
    if let Some(status) = span.status() {
        if let Some(message) = status.message() {
            line.field_str(FIELD_STATUS_DESCRIPTION, bytes_to_string(message));
        }
    }

    // Non-dimension span attributes collapse into a single JSON string field.
    let mut attr_map = serde_json::Map::new();
    for attr in span.attributes() {
        let key = bytes_to_string(attr.key());
        if dims.contains(key.as_str()) {
            continue;
        }
        let value = attr
            .value()
            .map(|v| any_value_to_json(&v))
            .unwrap_or(serde_json::Value::Null);
        let _ = attr_map.insert(key, value);
    }
    if !attr_map.is_empty() {
        let json = serde_json::to_string(&serde_json::Value::Object(attr_map)).unwrap_or_default();
        line.field_str(FIELD_SPAN_ATTRIBUTES, json);
    }

    add_dropped_field(
        &mut line,
        FIELD_DROPPED_ATTRS,
        span.dropped_attributes_count(),
    );
    add_dropped_field(&mut line, FIELD_DROPPED_EVENTS, span.dropped_events_count());
    add_dropped_field(&mut line, FIELD_DROPPED_LINKS, span.dropped_links_count());

    batcher.push(line);
}

fn append_span_events<Sp: SpanView>(
    span: &Sp,
    batcher: &mut LinesBatcher,
    stats: &mut ConvertStats,
) {
    let trace_id = span.trace_id().map(|id| hex_encode(id));
    let span_id = span.span_id().map(|id| hex_encode(id));

    for event in span.events() {
        let mut line = LineBuilder::new(MEASUREMENT_LOGS);
        if let Some(trace_id) = &trace_id {
            line.tag(TAG_TRACE_ID, trace_id.clone());
        }
        if let Some(span_id) = &span_id {
            line.tag(TAG_SPAN_ID, span_id.clone());
        }
        if let Some(ts) = event.time_unix_nano() {
            line.timestamp(ts);
        }
        if let Some(name) = event.name() {
            line.field_str(FIELD_NAME, bytes_to_string(name));
        }
        for attr in event.attributes() {
            let key = bytes_to_string(attr.key());
            if let Some(value) = attr.value() {
                let _ = append_any_value_field(&mut line, &key, &value, stats);
            }
        }
        add_dropped_field(
            &mut line,
            FIELD_DROPPED_ATTRS,
            event.dropped_attributes_count(),
        );
        batcher.push(line);
    }
}

fn append_span_links<Sp: SpanView>(
    span: &Sp,
    batcher: &mut LinesBatcher,
    stats: &mut ConvertStats,
) {
    let trace_id = span.trace_id().map(|id| hex_encode(id));
    let span_id = span.span_id().map(|id| hex_encode(id));
    let start = span.start_time_unix_nano();

    for link in span.links() {
        let mut line = LineBuilder::new(MEASUREMENT_SPAN_LINKS);
        if let Some(trace_id) = &trace_id {
            line.tag(TAG_TRACE_ID, trace_id.clone());
        }
        if let Some(span_id) = &span_id {
            line.tag(TAG_SPAN_ID, span_id.clone());
        }
        if let Some(linked_trace_id) = link.trace_id() {
            line.tag(TAG_LINKED_TRACE_ID, hex_encode(linked_trace_id));
        }
        if let Some(linked_span_id) = link.span_id() {
            line.tag(TAG_LINKED_SPAN_ID, hex_encode(linked_span_id));
        }
        if let Some(start) = start {
            line.timestamp(start);
        }
        if let Some(trace_state) = link.trace_state() {
            line.field_str(FIELD_TRACE_STATE, bytes_to_string(trace_state));
        }
        for attr in link.attributes() {
            let key = bytes_to_string(attr.key());
            if let Some(value) = attr.value() {
                let _ = append_any_value_field(&mut line, &key, &value, stats);
            }
        }
        add_dropped_field(
            &mut line,
            FIELD_DROPPED_ATTRS,
            link.dropped_attributes_count(),
        );
        batcher.push(line);
    }
}

fn add_dropped_field(line: &mut LineBuilder, key: &str, count: u32) {
    if count > 0 {
        line.field_u64(key, u64::from(count));
    }
}

fn span_kind_str(kind: i32) -> &'static str {
    match kind {
        1 => "SPAN_KIND_INTERNAL",
        2 => "SPAN_KIND_SERVER",
        3 => "SPAN_KIND_CLIENT",
        4 => "SPAN_KIND_PRODUCER",
        5 => "SPAN_KIND_CONSUMER",
        _ => "SPAN_KIND_UNSPECIFIED",
    }
}

fn status_code_str(code: i32) -> &'static str {
    match code {
        1 => "STATUS_CODE_OK",
        2 => "STATUS_CODE_ERROR",
        _ => "STATUS_CODE_UNSET",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exporters::influxdb_exporter::convert::test_util::{cfg, kv, render_lines, str_val};
    use otap_df_pdata::proto::opentelemetry::trace::v1::{
        ResourceSpans, ScopeSpans, Span, Status, TracesData, span,
    };
    use otap_df_pdata::views::otlp::bytes::traces::RawTraceData;

    fn render(traces: TracesData) -> Vec<String> {
        let config = cfg(serde_json::json!({}));
        render_lines(&config, |batcher, stats| {
            let mut buf = Vec::new();
            prost::Message::encode(&traces, &mut buf).expect("encode");
            let view = RawTraceData::new(&buf);
            append_traces(&view, &config, batcher, stats);
        })
    }

    #[test]
    fn span_with_event_and_link() {
        let span = Span {
            trace_id: vec![0xaa; 16],
            span_id: vec![0xbb; 8],
            parent_span_id: vec![0xcc; 8],
            name: "myspan".to_string(),
            kind: 2, // SERVER
            start_time_unix_nano: 100,
            end_time_unix_nano: 250,
            status: Some(Status {
                message: "boom".to_string(),
                code: 2, // ERROR
            }),
            attributes: vec![
                kv("service.name", str_val("svc")),
                kv("http.method", str_val("GET")),
            ],
            events: vec![span::Event {
                time_unix_nano: 120,
                name: "evt".to_string(),
                attributes: vec![kv("k", str_val("v"))],
                ..Default::default()
            }],
            links: vec![span::Link {
                trace_id: vec![0x11; 16],
                span_id: vec![0x22; 8],
                trace_state: "ts".to_string(),
                attributes: vec![kv("lk", str_val("lv"))],
                ..Default::default()
            }],
            ..Default::default()
        };
        let traces = TracesData {
            resource_spans: vec![ResourceSpans {
                resource: None,
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![span],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let lines = render(traces);
        assert_eq!(lines.len(), 3);

        let trace = "aa".repeat(16);
        let span_id = "bb".repeat(8);
        let parent = "cc".repeat(8);
        let linked_trace = "11".repeat(16);
        let linked_span = "22".repeat(8);

        assert_eq!(
            lines[0],
            format!(
                "spans,kind=SPAN_KIND_SERVER,name=myspan,otel.status_code=STATUS_CODE_ERROR,\
parent_span_id={parent},service.name=svc,span_id={span_id},trace_id={trace} \
end_time_unix_nano=250i,duration_nano=150i,otel.status_description=\"boom\",\
otel.span.attributes=\"{{\\\"http.method\\\":\\\"GET\\\"}}\" 100"
            )
        );
        assert_eq!(
            lines[1],
            format!("logs,span_id={span_id},trace_id={trace} name=\"evt\",k=\"v\" 120")
        );
        assert_eq!(
            lines[2],
            format!(
                "span-links,linked_span_id={linked_span},linked_trace_id={linked_trace},\
span_id={span_id},trace_id={trace} trace_state=\"ts\",lk=\"lv\" 100"
            )
        );
    }
}
