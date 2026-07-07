// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Conversion of OTLP metrics to InfluxDB line protocol.
//!
//! Supports the `telegraf-prometheus-v1` (measurement per metric) and
//! `telegraf-prometheus-v2` (single `prometheus` measurement) schemas.
//! Exponential histograms are unsupported by both schemas and are dropped.

use otap_df_pdata_views::views::common::{AttributeView, InstrumentationScopeView};
use otap_df_pdata_views::views::metrics::{
    DataType, DataView, ExponentialHistogramView, GaugeView, HistogramDataPointView, HistogramView,
    MetricView, MetricsView, NumberDataPointView, ResourceMetricsView, ScopeMetricsView, SumView,
    SummaryDataPointView, SummaryView, Value, ValueAtQuantileView,
};
use otap_df_pdata_views::views::resource::ResourceView;

use super::super::config::{Config, MetricsSchema};
use super::super::line_protocol::{LineBuilder, LinesBatcher};
use super::{
    ConvertStats, MEASUREMENT_PROMETHEUS, TAG_OTEL_LIB_NAME, TAG_OTEL_LIB_VERSION,
    any_value_to_tag_string, bytes_to_string,
};

const FIELD_COUNT: &str = "count";
const FIELD_SUM: &str = "sum";
const TAG_LE: &str = "le";
const TAG_QUANTILE: &str = "quantile";
const BUCKET_INF: &str = "+Inf";

/// The role of a scalar number field under the v1 schema.
#[derive(Clone, Copy)]
enum NumberKind {
    Gauge,
    Counter,
}

impl NumberKind {
    const fn field_name(self) -> &'static str {
        match self {
            Self::Gauge => "gauge",
            Self::Counter => "counter",
        }
    }
}

/// Appends all metric data points in `metrics` to `batcher`.
pub fn append_metrics<M: MetricsView>(
    metrics: &M,
    cfg: &Config,
    batcher: &mut LinesBatcher,
    stats: &mut ConvertStats,
) {
    let schema = cfg.metrics_schema;
    for resource_metrics in metrics.resources() {
        let resource = resource_metrics.resource();
        let resource_tags = collect_resource_tags(resource.as_ref());
        for scope_metrics in resource_metrics.scopes() {
            let mut base_tags = resource_tags.clone();
            if let Some(scope) = scope_metrics.scope() {
                push_scope_tags(&mut base_tags, &scope);
            }
            for metric in scope_metrics.metrics() {
                append_metric(&metric, &base_tags, schema, batcher, stats);
            }
        }
    }
}

fn append_metric<Me: MetricView>(
    metric: &Me,
    base_tags: &[(String, String)],
    schema: MetricsSchema,
    batcher: &mut LinesBatcher,
    stats: &mut ConvertStats,
) {
    let name = bytes_to_string(metric.name());
    let Some(data) = metric.data() else {
        return;
    };

    match data.value_type() {
        DataType::Gauge => {
            if let Some(gauge) = data.as_gauge() {
                for dp in gauge.data_points() {
                    append_number_point(
                        &name,
                        &dp,
                        base_tags,
                        schema,
                        NumberKind::Gauge,
                        batcher,
                        stats,
                    );
                }
            }
        }
        DataType::Sum => {
            if let Some(sum) = data.as_sum() {
                let kind = if sum.is_monotonic() {
                    NumberKind::Counter
                } else {
                    NumberKind::Gauge
                };
                for dp in sum.data_points() {
                    append_number_point(&name, &dp, base_tags, schema, kind, batcher, stats);
                }
            }
        }
        DataType::Histogram => {
            if let Some(histogram) = data.as_histogram() {
                for dp in histogram.data_points() {
                    append_histogram_point(&name, &dp, base_tags, schema, batcher, stats);
                }
            }
        }
        DataType::Summary => {
            if let Some(summary) = data.as_summary() {
                for dp in summary.data_points() {
                    append_summary_point(&name, &dp, base_tags, schema, batcher);
                }
            }
        }
        DataType::ExponentialHistogram => {
            if let Some(exp) = data.as_exponential_histogram() {
                stats.exp_histograms_dropped += exp.data_points().count() as u64;
            }
        }
    }
}

fn append_number_point<D: NumberDataPointView>(
    name: &str,
    dp: &D,
    base_tags: &[(String, String)],
    schema: MetricsSchema,
    kind: NumberKind,
    batcher: &mut LinesBatcher,
    stats: &mut ConvertStats,
) {
    let Some(value) = dp.value() else {
        return;
    };

    let (measurement, field_key): (&str, &str) = match schema {
        MetricsSchema::TelegrafPrometheusV1 => (name, kind.field_name()),
        MetricsSchema::TelegrafPrometheusV2 => (MEASUREMENT_PROMETHEUS, name),
    };

    let mut line = LineBuilder::new(measurement);
    apply_tags(&mut line, base_tags);
    apply_attr_tags(&mut line, dp.attributes());
    line.timestamp(dp.time_unix_nano());

    match value {
        Value::Double(d) => {
            if d.is_finite() {
                line.field_f64(field_key, d);
            } else {
                stats.invalid_points_dropped += 1;
                return;
            }
        }
        Value::Integer(i) => line.field_i64(field_key, i),
    }
    batcher.push(line);
}

fn append_histogram_point<D: HistogramDataPointView>(
    name: &str,
    dp: &D,
    base_tags: &[(String, String)],
    schema: MetricsSchema,
    batcher: &mut LinesBatcher,
    stats: &mut ConvertStats,
) {
    let bounds: Vec<f64> = dp.explicit_bounds().collect();
    let counts: Vec<u64> = dp.bucket_counts().collect();

    match schema {
        MetricsSchema::TelegrafPrometheusV1 => {
            let mut line = LineBuilder::new(name);
            apply_tags(&mut line, base_tags);
            apply_attr_tags(&mut line, dp.attributes());
            line.timestamp(dp.time_unix_nano());
            line.field_u64(FIELD_COUNT, dp.count());
            if let Some(sum) = dp.sum() {
                if sum.is_finite() {
                    line.field_f64(FIELD_SUM, sum);
                } else {
                    stats.invalid_points_dropped += 1;
                }
            }
            let mut cumulative = 0u64;
            for (i, count) in counts.iter().enumerate() {
                cumulative = cumulative.saturating_add(*count);
                let key = bound_label(&bounds, i);
                line.field_u64(key, cumulative);
            }
            batcher.push(line);
        }
        MetricsSchema::TelegrafPrometheusV2 => {
            let mut line = LineBuilder::new(MEASUREMENT_PROMETHEUS);
            apply_tags(&mut line, base_tags);
            apply_attr_tags(&mut line, dp.attributes());
            line.timestamp(dp.time_unix_nano());
            line.field_u64(format!("{name}_{FIELD_COUNT}"), dp.count());
            if let Some(sum) = dp.sum() {
                if sum.is_finite() {
                    line.field_f64(format!("{name}_{FIELD_SUM}"), sum);
                } else {
                    stats.invalid_points_dropped += 1;
                }
            }
            batcher.push(line);

            let bucket_field = format!("{name}_bucket");
            let mut cumulative = 0u64;
            for (i, count) in counts.iter().enumerate() {
                cumulative = cumulative.saturating_add(*count);
                let mut bline = LineBuilder::new(MEASUREMENT_PROMETHEUS);
                apply_tags(&mut bline, base_tags);
                apply_attr_tags(&mut bline, dp.attributes());
                bline.tag(TAG_LE, bound_label(&bounds, i));
                bline.timestamp(dp.time_unix_nano());
                bline.field_u64(bucket_field.as_str(), cumulative);
                batcher.push(bline);
            }
        }
    }
}

fn append_summary_point<D: SummaryDataPointView>(
    name: &str,
    dp: &D,
    base_tags: &[(String, String)],
    schema: MetricsSchema,
    batcher: &mut LinesBatcher,
) {
    match schema {
        MetricsSchema::TelegrafPrometheusV1 => {
            let mut line = LineBuilder::new(name);
            apply_tags(&mut line, base_tags);
            apply_attr_tags(&mut line, dp.attributes());
            line.timestamp(dp.time_unix_nano());
            line.field_u64(FIELD_COUNT, dp.count());
            if dp.sum().is_finite() {
                line.field_f64(FIELD_SUM, dp.sum());
            }
            for quantile in dp.quantile_values() {
                if quantile.value().is_finite() {
                    line.field_f64(format_number(quantile.quantile()), quantile.value());
                }
            }
            batcher.push(line);
        }
        MetricsSchema::TelegrafPrometheusV2 => {
            let mut line = LineBuilder::new(MEASUREMENT_PROMETHEUS);
            apply_tags(&mut line, base_tags);
            apply_attr_tags(&mut line, dp.attributes());
            line.timestamp(dp.time_unix_nano());
            line.field_u64(format!("{name}_{FIELD_COUNT}"), dp.count());
            if dp.sum().is_finite() {
                line.field_f64(format!("{name}_{FIELD_SUM}"), dp.sum());
            }
            batcher.push(line);

            for quantile in dp.quantile_values() {
                if !quantile.value().is_finite() {
                    continue;
                }
                let mut qline = LineBuilder::new(MEASUREMENT_PROMETHEUS);
                apply_tags(&mut qline, base_tags);
                apply_attr_tags(&mut qline, dp.attributes());
                qline.tag(TAG_QUANTILE, format_number(quantile.quantile()));
                qline.timestamp(dp.time_unix_nano());
                qline.field_f64(name.to_string(), quantile.value());
                batcher.push(qline);
            }
        }
    }
}

fn collect_resource_tags<R: ResourceView>(resource: Option<&R>) -> Vec<(String, String)> {
    let mut tags = Vec::new();
    if let Some(resource) = resource {
        for attr in resource.attributes() {
            let key = bytes_to_string(attr.key());
            if let Some(value) = attr.value() {
                if let Some(rendered) = any_value_to_tag_string(&value) {
                    tags.push((key, rendered));
                }
            }
        }
    }
    tags
}

fn push_scope_tags<S: InstrumentationScopeView>(tags: &mut Vec<(String, String)>, scope: &S) {
    if let Some(name) = scope.name() {
        tags.push((TAG_OTEL_LIB_NAME.to_string(), bytes_to_string(name)));
    }
    if let Some(version) = scope.version() {
        tags.push((TAG_OTEL_LIB_VERSION.to_string(), bytes_to_string(version)));
    }
}

fn apply_tags(line: &mut LineBuilder, tags: &[(String, String)]) {
    for (key, value) in tags {
        line.tag(key.as_str(), value.as_str());
    }
}

fn apply_attr_tags<A: AttributeView>(line: &mut LineBuilder, attrs: impl Iterator<Item = A>) {
    for attr in attrs {
        let key = bytes_to_string(attr.key());
        if let Some(value) = attr.value() {
            if let Some(rendered) = any_value_to_tag_string(&value) {
                line.tag(key, rendered);
            }
        }
    }
}

/// Returns the `le`/bucket label for bucket index `i`. The overflow bucket
/// (index == bounds.len()) is labeled `+Inf`.
fn bound_label(bounds: &[f64], i: usize) -> String {
    match bounds.get(i) {
        Some(bound) => format_number(*bound),
        None => BUCKET_INF.to_string(),
    }
}

fn format_number(v: f64) -> String {
    if v.is_finite() {
        let mut buf = ryu::Buffer::new();
        buf.format_finite(v).to_string()
    } else if v.is_nan() {
        "NaN".to_string()
    } else if v > 0.0 {
        BUCKET_INF.to_string()
    } else {
        "-Inf".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exporters::influxdb_exporter::convert::test_util::{cfg, kv, str_val};
    use crate::exporters::influxdb_exporter::line_protocol::LinesBatcher;
    use otap_df_pdata::proto::opentelemetry::common::v1::InstrumentationScope;
    use otap_df_pdata::proto::opentelemetry::metrics::v1::{
        ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram, HistogramDataPoint,
        Metric, MetricsData, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, Summary,
        SummaryDataPoint, metric, number_data_point, summary_data_point,
    };
    use otap_df_pdata::proto::opentelemetry::resource::v1::Resource;
    use otap_df_pdata::views::otlp::bytes::metrics::RawMetricsData;

    fn wrap(
        resource: Option<Resource>,
        scope: Option<InstrumentationScope>,
        metric: Metric,
    ) -> MetricsData {
        MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource,
                scope_metrics: vec![ScopeMetrics {
                    scope,
                    metrics: vec![metric],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn render(md: MetricsData, schema: &str) -> Vec<String> {
        let config = cfg(serde_json::json!({ "metrics_schema": schema }));
        let mut batcher = LinesBatcher::new(
            config.payload_max_lines,
            config.payload_max_bytes,
            config.precision,
        );
        let mut stats = ConvertStats::default();
        let mut buf = Vec::new();
        prost::Message::encode(&md, &mut buf).expect("encode");
        let view = RawMetricsData::new(&buf);
        append_metrics(&view, &config, &mut batcher, &mut stats);
        let payloads = batcher.finish();
        let mut lines = Vec::new();
        for payload in payloads {
            for line in String::from_utf8(payload.to_vec()).expect("utf8").lines() {
                lines.push(line.to_string());
            }
        }
        lines
    }

    fn number_metric(name: &str, data: metric::Data) -> Metric {
        Metric {
            name: name.to_string(),
            data: Some(data),
            ..Default::default()
        }
    }

    #[test]
    fn gauge_v1_and_v2() {
        let dp = NumberDataPoint {
            value: Some(number_data_point::Value::AsDouble(21.5)),
            time_unix_nano: 7,
            attributes: vec![kv("host", str_val("h1"))],
            ..Default::default()
        };
        let metric = number_metric(
            "temp",
            metric::Data::Gauge(Gauge {
                data_points: vec![dp],
            }),
        );
        let md = wrap(
            Some(Resource {
                attributes: vec![kv("region", str_val("us"))],
                ..Default::default()
            }),
            Some(InstrumentationScope {
                name: "lib".to_string(),
                version: "1.0".to_string(),
                ..Default::default()
            }),
            metric,
        );

        let v1 = render(md.clone(), "telegraf-prometheus-v1");
        assert_eq!(
            v1,
            vec![
                "temp,host=h1,otel.library.name=lib,otel.library.version=1.0,region=us gauge=21.5 7"
                    .to_string()
            ]
        );

        let v2 = render(md, "telegraf-prometheus-v2");
        assert_eq!(
            v2,
            vec![
                "prometheus,host=h1,otel.library.name=lib,otel.library.version=1.0,region=us temp=21.5 7"
                    .to_string()
            ]
        );
    }

    #[test]
    fn monotonic_sum_v1_uses_counter_field() {
        let dp = NumberDataPoint {
            value: Some(number_data_point::Value::AsInt(10)),
            time_unix_nano: 8,
            ..Default::default()
        };
        let metric = number_metric(
            "reqs",
            metric::Data::Sum(Sum {
                data_points: vec![dp],
                aggregation_temporality: 2,
                is_monotonic: true,
            }),
        );
        let md = wrap(None, None, metric);
        assert_eq!(
            render(md, "telegraf-prometheus-v1"),
            vec!["reqs counter=10i 8".to_string()]
        );
    }

    #[test]
    fn histogram_v1() {
        let dp = HistogramDataPoint {
            count: 6,
            sum: Some(12.0),
            bucket_counts: vec![1, 2, 3],
            explicit_bounds: vec![10.0, 20.0],
            time_unix_nano: 9,
            ..Default::default()
        };
        let metric = number_metric(
            "lat",
            metric::Data::Histogram(Histogram {
                data_points: vec![dp],
                aggregation_temporality: 2,
            }),
        );
        let md = wrap(None, None, metric);
        assert_eq!(
            render(md, "telegraf-prometheus-v1"),
            vec!["lat count=6u,sum=12.0,10.0=1u,20.0=3u,+Inf=6u 9".to_string()]
        );
    }

    #[test]
    fn histogram_v2_emits_bucket_points() {
        let dp = HistogramDataPoint {
            count: 6,
            sum: Some(12.0),
            bucket_counts: vec![1, 2, 3],
            explicit_bounds: vec![10.0, 20.0],
            time_unix_nano: 9,
            ..Default::default()
        };
        let metric = number_metric(
            "lat",
            metric::Data::Histogram(Histogram {
                data_points: vec![dp],
                aggregation_temporality: 2,
            }),
        );
        let md = wrap(None, None, metric);
        assert_eq!(
            render(md, "telegraf-prometheus-v2"),
            vec![
                "prometheus lat_count=6u,lat_sum=12.0 9".to_string(),
                "prometheus,le=10.0 lat_bucket=1u 9".to_string(),
                "prometheus,le=20.0 lat_bucket=3u 9".to_string(),
                "prometheus,le=+Inf lat_bucket=6u 9".to_string(),
            ]
        );
    }

    #[test]
    fn summary_v1() {
        let dp = SummaryDataPoint {
            count: 4,
            sum: 8.0,
            quantile_values: vec![
                summary_data_point::ValueAtQuantile {
                    quantile: 0.5,
                    value: 1.0,
                },
                summary_data_point::ValueAtQuantile {
                    quantile: 0.99,
                    value: 5.0,
                },
            ],
            time_unix_nano: 11,
            ..Default::default()
        };
        let metric = number_metric(
            "sm",
            metric::Data::Summary(Summary {
                data_points: vec![dp],
            }),
        );
        let md = wrap(None, None, metric);
        assert_eq!(
            render(md, "telegraf-prometheus-v1"),
            vec!["sm count=4u,sum=8.0,0.5=1.0,0.99=5.0 11".to_string()]
        );
    }

    #[test]
    fn exponential_histogram_is_dropped_and_counted() {
        let metric = number_metric(
            "eh",
            metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![
                    ExponentialHistogramDataPoint::default(),
                    ExponentialHistogramDataPoint::default(),
                ],
                aggregation_temporality: 2,
            }),
        );
        let md = wrap(None, None, metric);

        let config = cfg(serde_json::json!({}));
        let mut batcher = LinesBatcher::new(
            config.payload_max_lines,
            config.payload_max_bytes,
            config.precision,
        );
        let mut stats = ConvertStats::default();
        let mut buf = Vec::new();
        prost::Message::encode(&md, &mut buf).expect("encode");
        let view = RawMetricsData::new(&buf);
        append_metrics(&view, &config, &mut batcher, &mut stats);

        assert!(batcher.finish().is_empty());
        assert_eq!(stats.exp_histograms_dropped, 2);
    }
}
