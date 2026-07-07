// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Conversion from OTLP view traits to InfluxDB line protocol.
//!
//! The submodules are generic over the backend-agnostic view traits, so the
//! same code serves both the OTLP-bytes and OTAP-Arrow backends. This module
//! holds the shared helpers: attribute stringification, JSON encoding of
//! complex values, hex id encoding, and attribute lookup.

pub mod logs;
pub mod metrics;
pub mod traces;

use otap_df_pdata_views::views::common::{
    AnyValueView, AttributeView, InstrumentationScopeView, ValueType,
};

use super::line_protocol::LineBuilder;

// ---- Measurement names ----
pub(crate) const MEASUREMENT_LOGS: &str = "logs";
pub(crate) const MEASUREMENT_SPANS: &str = "spans";
pub(crate) const MEASUREMENT_SPAN_LINKS: &str = "span-links";
pub(crate) const MEASUREMENT_PROMETHEUS: &str = "prometheus";

// ---- Common tag names ----
pub(crate) const TAG_TRACE_ID: &str = "trace_id";
pub(crate) const TAG_SPAN_ID: &str = "span_id";
pub(crate) const TAG_PARENT_SPAN_ID: &str = "parent_span_id";
pub(crate) const TAG_TRACE_STATE: &str = "trace_state";
pub(crate) const TAG_NAME: &str = "name";
pub(crate) const TAG_SPAN_KIND: &str = "kind";
pub(crate) const TAG_STATUS_CODE: &str = "otel.status_code";
pub(crate) const TAG_SEVERITY_NUMBER: &str = "severity_number";
pub(crate) const TAG_OTEL_LIB_NAME: &str = "otel.library.name";
pub(crate) const TAG_OTEL_LIB_VERSION: &str = "otel.library.version";
pub(crate) const TAG_LINKED_TRACE_ID: &str = "linked_trace_id";
pub(crate) const TAG_LINKED_SPAN_ID: &str = "linked_span_id";

// ---- Common field names ----
pub(crate) const FIELD_BODY: &str = "body";
pub(crate) const FIELD_NAME: &str = "name";
pub(crate) const FIELD_SEVERITY_TEXT: &str = "severity_text";
pub(crate) const FIELD_END_TIME: &str = "end_time_unix_nano";
pub(crate) const FIELD_DURATION: &str = "duration_nano";
pub(crate) const FIELD_TRACE_STATE: &str = "trace_state";
pub(crate) const FIELD_STATUS_DESCRIPTION: &str = "otel.status_description";
pub(crate) const FIELD_SPAN_ATTRIBUTES: &str = "otel.span.attributes";
pub(crate) const FIELD_DROPPED_ATTRS: &str = "dropped_attributes_count";
pub(crate) const FIELD_DROPPED_EVENTS: &str = "dropped_events_count";
pub(crate) const FIELD_DROPPED_LINKS: &str = "dropped_links_count";

/// Statistics accumulated during a single conversion pass, surfaced to the
/// exporter's telemetry.
#[derive(Debug, Default, Clone, Copy)]
pub struct ConvertStats {
    /// Exponential-histogram data points dropped (schema does not support them).
    pub exp_histograms_dropped: u64,
    /// Points dropped for having no representable field value (e.g. NaN/Inf).
    pub invalid_points_dropped: u64,
}

/// Lowercase-hex encodes a byte slice (used for trace/span ids).
pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Lossily converts a byte string to an owned `String`.
pub(crate) fn bytes_to_string(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn format_f64(v: f64) -> String {
    if v.is_finite() {
        let mut buf = ryu::Buffer::new();
        buf.format_finite(v).to_string()
    } else {
        v.to_string()
    }
}

/// Converts an `AnyValue` view into a `serde_json::Value`, recursing into
/// arrays and key-value lists. Non-representable values become `Null`.
pub(crate) fn any_value_to_json<'a, V: AnyValueView<'a>>(v: &V) -> serde_json::Value {
    use serde_json::Value;
    match v.value_type() {
        ValueType::Empty => Value::Null,
        ValueType::String => v
            .as_string()
            .map(|b| Value::String(bytes_to_string(b)))
            .unwrap_or(Value::Null),
        ValueType::Bool => Value::Bool(v.as_bool().unwrap_or(false)),
        ValueType::Int64 => v.as_int64().map(Value::from).unwrap_or(Value::Null),
        ValueType::Double => v
            .as_double()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        ValueType::Bytes => v
            .as_bytes()
            .map(|b| Value::String(hex_encode(b)))
            .unwrap_or(Value::Null),
        ValueType::Array => {
            let mut arr = Vec::new();
            if let Some(iter) = v.as_array() {
                for item in iter {
                    arr.push(any_value_to_json(&item));
                }
            }
            Value::Array(arr)
        }
        ValueType::KeyValueList => {
            let mut map = serde_json::Map::new();
            if let Some(iter) = v.as_kvlist() {
                for kv in iter {
                    let key = bytes_to_string(kv.key());
                    let val = kv
                        .value()
                        .map(|vv| any_value_to_json(&vv))
                        .unwrap_or(Value::Null);
                    let _ = map.insert(key, val);
                }
            }
            Value::Object(map)
        }
    }
}

/// Renders an `AnyValue` view as a tag string, or `None` if the value is empty.
/// Complex values are JSON-encoded.
pub(crate) fn any_value_to_tag_string<'a, V: AnyValueView<'a>>(v: &V) -> Option<String> {
    match v.value_type() {
        ValueType::Empty => None,
        ValueType::String => v.as_string().map(bytes_to_string),
        ValueType::Bool => Some(v.as_bool().unwrap_or(false).to_string()),
        ValueType::Int64 => Some(v.as_int64().unwrap_or(0).to_string()),
        ValueType::Double => Some(format_f64(v.as_double().unwrap_or(0.0))),
        ValueType::Bytes => v.as_bytes().map(hex_encode),
        ValueType::Array | ValueType::KeyValueList => {
            Some(serde_json::to_string(&any_value_to_json(v)).unwrap_or_default())
        }
    }
}

/// Appends an `AnyValue` view as a field on `line` under `key`.
///
/// Returns `true` if a field was written. Non-finite doubles are dropped and
/// counted in `stats.invalid_points_dropped`; empty values are skipped.
pub(crate) fn append_any_value_field<'a, V: AnyValueView<'a>>(
    line: &mut LineBuilder,
    key: &str,
    v: &V,
    stats: &mut ConvertStats,
) -> bool {
    match v.value_type() {
        ValueType::Empty => false,
        ValueType::String => match v.as_string() {
            Some(b) => {
                line.field_str(key, bytes_to_string(b));
                true
            }
            None => false,
        },
        ValueType::Bool => {
            line.field_bool(key, v.as_bool().unwrap_or(false));
            true
        }
        ValueType::Int64 => {
            line.field_i64(key, v.as_int64().unwrap_or(0));
            true
        }
        ValueType::Double => {
            let d = v.as_double().unwrap_or(0.0);
            if d.is_finite() {
                line.field_f64(key, d);
                true
            } else {
                stats.invalid_points_dropped += 1;
                false
            }
        }
        ValueType::Bytes => match v.as_bytes() {
            Some(b) => {
                line.field_str(key, hex_encode(b));
                true
            }
            None => false,
        },
        ValueType::Array | ValueType::KeyValueList => {
            let json = serde_json::to_string(&any_value_to_json(v)).unwrap_or_default();
            line.field_str(key, json);
            true
        }
    }
}

/// Finds an attribute by key in an attribute iterator and returns its
/// tag-stringified value.
pub(crate) fn find_attr<A: AttributeView>(
    attrs: impl Iterator<Item = A>,
    key: &str,
) -> Option<String> {
    for attr in attrs {
        if attr.key() == key.as_bytes() {
            return attr.value().and_then(|v| any_value_to_tag_string(&v));
        }
    }
    None
}

/// Adds the instrumentation scope name/version as `otel.library.*` tags.
pub(crate) fn add_scope_tags<S: InstrumentationScopeView>(line: &mut LineBuilder, scope: &S) {
    if let Some(name) = scope.name() {
        line.tag(TAG_OTEL_LIB_NAME, bytes_to_string(name));
    }
    if let Some(version) = scope.version() {
        line.tag(TAG_OTEL_LIB_VERSION, bytes_to_string(version));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_encode_lowercase() {
        assert_eq!(hex_encode(&[0x0a, 0xff, 0x00, 0x1b]), "0aff001b");
        assert_eq!(hex_encode(&[]), "");
    }

    #[test]
    fn bytes_to_string_lossy() {
        assert_eq!(bytes_to_string(b"hello"), "hello");
    }
}

#[cfg(test)]
pub(crate) mod test_util {
    use crate::exporters::influxdb_exporter::config::Config;
    use crate::exporters::influxdb_exporter::convert::ConvertStats;
    use crate::exporters::influxdb_exporter::line_protocol::LinesBatcher;
    use otap_df_pdata::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value};

    pub(crate) fn str_val(s: &str) -> AnyValue {
        AnyValue {
            value: Some(any_value::Value::StringValue(s.to_string())),
        }
    }

    pub(crate) fn int_val(i: i64) -> AnyValue {
        AnyValue {
            value: Some(any_value::Value::IntValue(i)),
        }
    }

    pub(crate) fn kv(k: &str, v: AnyValue) -> KeyValue {
        KeyValue {
            key: k.to_string(),
            value: Some(v),
        }
    }

    /// Builds a `Config` from the minimal required fields merged with `overrides`.
    pub(crate) fn cfg(overrides: serde_json::Value) -> Config {
        let mut base = serde_json::json!({
            "endpoint": "http://localhost:8086",
            "org": "o",
            "bucket": "b",
            "token": "t"
        });
        if let (Some(base_map), Some(over_map)) = (base.as_object_mut(), overrides.as_object()) {
            for (k, v) in over_map {
                let _ = base_map.insert(k.clone(), v.clone());
            }
        }
        serde_json::from_value(base).expect("valid config")
    }

    /// Runs a conversion closure and returns the rendered lines (order preserved).
    pub(crate) fn render_lines(
        config: &Config,
        f: impl FnOnce(&mut LinesBatcher, &mut ConvertStats),
    ) -> Vec<String> {
        render_lines_and_stats(config, f).0
    }

    /// Runs a conversion closure and returns the rendered lines plus the
    /// accumulated conversion stats.
    pub(crate) fn render_lines_and_stats(
        config: &Config,
        f: impl FnOnce(&mut LinesBatcher, &mut ConvertStats),
    ) -> (Vec<String>, ConvertStats) {
        let mut batcher = LinesBatcher::new(
            config.payload_max_lines,
            config.payload_max_bytes,
            config.precision,
        );
        let mut stats = ConvertStats::default();
        f(&mut batcher, &mut stats);
        let payloads = batcher.finish();
        let mut lines = Vec::new();
        for payload in payloads {
            for line in String::from_utf8(payload.to_vec()).expect("utf8").lines() {
                lines.push(line.to_string());
            }
        }
        (lines, stats)
    }
}
