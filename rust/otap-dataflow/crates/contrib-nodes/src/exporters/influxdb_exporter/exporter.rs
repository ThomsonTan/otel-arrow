// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! InfluxDB exporter runtime: receive loop, conversion dispatch, sequential
//! writes and ack/nack handling.

use async_trait::async_trait;

use otap_df_config::SignalType;
use otap_df_engine::ConsumerEffectHandlerExtension;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::control::{AckMsg, NackMsg, NodeControlMsg};
use otap_df_engine::error::{Error as EngineError, ExporterErrorKind};
use otap_df_engine::local::exporter::{EffectHandler, Exporter};
use otap_df_engine::message::{ExporterInbox, Message};
use otap_df_engine::terminal_state::TerminalState;
use otap_df_telemetry::metrics::MetricSet;
use otap_df_telemetry::{otel_info, otel_warn};

use otap_df_otap::metrics::ExporterPDataMetrics;
use otap_df_otap::pdata::{Context, OtapPdata};
use otap_df_pdata::otlp::OtlpProtoBytes;
use otap_df_pdata::views::otap::{OtapLogsView, OtapMetricsView, OtapTracesView};
use otap_df_pdata::views::otlp::bytes::logs::RawLogsData;
use otap_df_pdata::views::otlp::bytes::metrics::RawMetricsData;
use otap_df_pdata::views::otlp::bytes::traces::RawTraceData;
use otap_df_pdata::{OtapArrowRecords, OtapPayload};

use super::client::InfluxdbClient;
use super::config::Config;
use super::convert::ConvertStats;
use super::convert::logs::append_logs;
use super::convert::metrics::append_metrics;
use super::convert::traces::append_traces;
use super::error::Error;
use super::line_protocol::LinesBatcher;
use super::metrics::InfluxdbExporterMetrics;

/// InfluxDB exporter.
pub struct InfluxdbExporter {
    config: Config,
    pdata_metrics: MetricSet<ExporterPDataMetrics>,
    influx_metrics: MetricSet<InfluxdbExporterMetrics>,
}

impl InfluxdbExporter {
    /// Builds a new exporter, validating the configuration up front.
    pub fn new(pipeline_ctx: PipelineContext, config: Config) -> Result<Self, Error> {
        config.validate()?;
        let pdata_metrics = pipeline_ctx.register_metrics::<ExporterPDataMetrics>();
        let influx_metrics = pipeline_ctx.register_metrics::<InfluxdbExporterMetrics>();
        Ok(Self {
            config,
            pdata_metrics,
            influx_metrics,
        })
    }

    async fn handle_pdata(
        &mut self,
        client: &InfluxdbClient,
        effect_handler: &EffectHandler<OtapPdata>,
        pdata: OtapPdata,
    ) -> Result<(), EngineError> {
        let signal_type = pdata.signal_type();
        self.pdata_metrics.inc_consumed(signal_type);

        let (context, payload) = pdata.into_parts();

        // Empty payloads carry no data: acknowledge without a write.
        if payload.is_empty() {
            let saved = saved_payload(&context, payload, signal_type);
            let _ = effect_handler
                .notify_ack(AckMsg::new(OtapPdata::new(context, saved)))
                .await;
            self.pdata_metrics.inc_exported(signal_type);
            return Ok(());
        }

        let mut batcher = LinesBatcher::new(
            self.config.payload_max_lines,
            self.config.payload_max_bytes,
            self.config.precision,
        );
        let mut stats = ConvertStats::default();

        let convert_result = convert_payload(&self.config, &payload, &mut batcher, &mut stats);

        // The payload is no longer needed for conversion; reclaim it for ack/nack.
        let saved = saved_payload(&context, payload, signal_type);

        if let Err(e) = convert_result {
            otel_warn!("influxdb.exporter.convert_failed", error = %e);
            let mut nack = NackMsg::new(e.to_string(), OtapPdata::new(context, saved));
            nack.permanent = true;
            let _ = effect_handler.notify_nack(nack).await;
            self.pdata_metrics.inc_failed(signal_type);
            return Ok(());
        }

        // Fold conversion stats into telemetry.
        self.influx_metrics
            .add_exp_histograms_dropped(stats.exp_histograms_dropped);
        self.influx_metrics
            .add_invalid_points_dropped(stats.invalid_points_dropped);
        if stats.exp_histograms_dropped > 0 {
            otel_warn!(
                "influxdb.exporter.exp_histograms_dropped",
                count = stats.exp_histograms_dropped
            );
        }

        let payloads = batcher.finish();
        self.influx_metrics.add_lines_written(batcher.total_lines());

        if payloads.is_empty() {
            let _ = effect_handler
                .notify_ack(AckMsg::new(OtapPdata::new(context, saved)))
                .await;
            self.pdata_metrics.inc_exported(signal_type);
            return Ok(());
        }

        // Sequential writes. On the first failure, nack the whole message and
        // stop; upstream retries are safe because InfluxDB points are
        // idempotent upserts on (measurement, tag set, timestamp).
        for body in payloads {
            self.influx_metrics.add_write_request();
            if let Err(e) = client.write(body).await {
                let retryable = e.is_retryable();
                if retryable {
                    self.influx_metrics.add_write_failure_retryable();
                } else {
                    self.influx_metrics.add_write_failure_permanent();
                }
                otel_warn!(
                    "influxdb.exporter.write_failed",
                    error = %e,
                    retryable = retryable
                );
                let mut nack = NackMsg::new(e.to_string(), OtapPdata::new(context, saved));
                nack.permanent = !retryable;
                let _ = effect_handler.notify_nack(nack).await;
                self.pdata_metrics.inc_failed(signal_type);
                return Ok(());
            }
        }

        let _ = effect_handler
            .notify_ack(AckMsg::new(OtapPdata::new(context, saved)))
            .await;
        self.pdata_metrics.inc_exported(signal_type);
        Ok(())
    }
}

/// Returns the payload to attach to ack/nack messages: the original payload when
/// the context requests it back, otherwise an empty placeholder.
fn saved_payload(context: &Context, payload: OtapPayload, signal_type: SignalType) -> OtapPayload {
    if context.may_return_payload() {
        payload
    } else {
        OtapPayload::empty(signal_type)
    }
}

fn convert_payload(
    cfg: &Config,
    payload: &OtapPayload,
    batcher: &mut LinesBatcher,
    stats: &mut ConvertStats,
) -> Result<(), Error> {
    match payload {
        OtapPayload::OtapArrowRecords(records) => match records {
            OtapArrowRecords::Logs(_) => {
                let view = OtapLogsView::try_from(records).map_err(|e| Error::ViewCreation {
                    signal: "logs",
                    source: e,
                })?;
                append_logs(&view, cfg, batcher, stats);
            }
            OtapArrowRecords::Metrics(_) => {
                let view = OtapMetricsView::try_from(records).map_err(|e| Error::ViewCreation {
                    signal: "metrics",
                    source: e,
                })?;
                append_metrics(&view, cfg, batcher, stats);
            }
            OtapArrowRecords::Traces(_) => {
                let view = OtapTracesView::try_from(records).map_err(|e| Error::ViewCreation {
                    signal: "traces",
                    source: e,
                })?;
                append_traces(&view, cfg, batcher, stats);
            }
        },
        OtapPayload::OtlpBytes(bytes) => match bytes {
            OtlpProtoBytes::ExportLogsRequest(b) => {
                let view = RawLogsData::new(b.as_ref());
                append_logs(&view, cfg, batcher, stats);
            }
            OtlpProtoBytes::ExportMetricsRequest(b) => {
                let view = RawMetricsData::new(b.as_ref());
                append_metrics(&view, cfg, batcher, stats);
            }
            OtlpProtoBytes::ExportTracesRequest(b) => {
                let view = RawTraceData::new(b.as_ref());
                append_traces(&view, cfg, batcher, stats);
            }
        },
    }
    Ok(())
}

#[async_trait(?Send)]
impl Exporter<OtapPdata> for InfluxdbExporter {
    async fn start(
        mut self: Box<Self>,
        mut msg_chan: ExporterInbox<OtapPdata>,
        effect_handler: EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, EngineError> {
        otel_info!(
            "influxdb.exporter.start",
            endpoint = self.config.endpoint.as_str(),
            org = self.config.org.as_str(),
            bucket = self.config.bucket.as_str()
        );

        let client =
            InfluxdbClient::new(&self.config)
                .await
                .map_err(|e| EngineError::ExporterError {
                    exporter: effect_handler.exporter_id(),
                    kind: ExporterErrorKind::Configuration,
                    error: "unable to initialize InfluxDB client".into(),
                    source_detail: e.to_string(),
                })?;

        loop {
            let msg = msg_chan.recv().await?;
            match msg {
                Message::Control(NodeControlMsg::Shutdown { deadline, reason }) => {
                    otel_info!("influxdb.exporter.shutdown", reason = reason);
                    return Ok(TerminalState::new(
                        deadline,
                        [
                            self.pdata_metrics.snapshot(),
                            self.influx_metrics.snapshot(),
                        ],
                    ));
                }
                Message::Control(NodeControlMsg::CollectTelemetry {
                    mut metrics_reporter,
                }) => {
                    let _ = metrics_reporter.report(&mut self.pdata_metrics);
                    let _ = metrics_reporter.report(&mut self.influx_metrics);
                }
                Message::PData(pdata) => {
                    self.handle_pdata(&client, &effect_handler, pdata).await?;
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use std::time::{Duration, Instant};

    use otap_df_engine::Interests;
    use otap_df_engine::control::PipelineCompletionMsg;
    use otap_df_engine::testing::exporter::{TestRuntime, create_exporter_from_factory};
    use otap_df_otap::pdata::OtapPdata;
    use otap_df_otap::testing::{TestCallData, next_ack, next_nack};
    use otap_df_pdata::OtapPayload;
    use otap_df_pdata::otlp::OtlpProtoBytes;
    use otap_df_pdata::proto::opentelemetry::common::v1::{AnyValue, any_value};
    use otap_df_pdata::proto::opentelemetry::logs::v1::{
        LogRecord, LogsData, ResourceLogs, ScopeLogs,
    };
    use prost::Message as _;
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::exporters::influxdb_exporter::INFLUXDB_EXPORTER;

    fn config_json(endpoint: &str) -> serde_json::Value {
        serde_json::json!({
            "endpoint": endpoint,
            "org": "o",
            "bucket": "b",
            "token": "secret-token"
        })
    }

    /// A logs payload with one record that yields a non-empty line.
    fn logs_payload() -> OtapPayload {
        let logs = LogsData {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: 1,
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("hi".to_string())),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let mut buf = Vec::new();
        logs.encode(&mut buf).expect("encode");
        OtlpProtoBytes::ExportLogsRequest(Bytes::from(buf)).into()
    }

    /// Starts a mock InfluxDB server on a dedicated multi-threaded runtime whose
    /// worker threads keep serving after setup returns. The returned runtime and
    /// server must be kept alive for the duration of the test.
    fn start_mock(mock: Mock) -> (String, MockServer, tokio::runtime::Runtime) {
        otap_df_otap::crypto::ensure_crypto_provider();
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        let server = rt.block_on(async {
            let server = MockServer::start().await;
            mock.mount(&server).await;
            server
        });
        let uri = server.uri();
        (uri, server, rt)
    }

    #[test]
    fn empty_payload_is_acked_without_write() {
        otap_df_otap::crypto::ensure_crypto_provider();
        let test_runtime = TestRuntime::new();
        let exporter =
            create_exporter_from_factory(&INFLUXDB_EXPORTER, config_json("http://127.0.0.1:9"))
                .expect("exporter");

        test_runtime
            .set_exporter(exporter)
            .run_test(|ctx| async move {
                let payload: OtapPayload = OtlpProtoBytes::ExportLogsRequest(Bytes::new()).into();
                let pdata = OtapPdata::new_default(payload).test_subscribe_to(
                    Interests::ACKS,
                    TestCallData::default().into(),
                    11,
                );
                ctx.send_pdata(pdata).await.unwrap();
                ctx.send_shutdown(Instant::now() + Duration::from_secs(1), "done")
                    .await
                    .unwrap();
            })
            .run_validation(|mut ctx, result| async move {
                result.expect("success");
                let mut rx = ctx.take_pipeline_completion_receiver().unwrap();
                match rx.recv().await.unwrap() {
                    PipelineCompletionMsg::DeliverAck { ack } => {
                        let (node_id, _) = next_ack(ack).expect("ack subscriber");
                        assert_eq!(node_id, 11);
                    }
                    PipelineCompletionMsg::DeliverNack { nack } => {
                        let (_, nack) = next_nack(nack).expect("nack subscriber");
                        panic!("unexpected nack: {}", nack.reason);
                    }
                }
            });
    }

    #[test]
    fn successful_write_is_acked() {
        // The mock only matches the fully-formed write request; a mismatch would
        // yield a 404 -> nack, so an ack proves the URL, query and auth header.
        let mock = Mock::given(method("POST"))
            .and(path("/api/v2/write"))
            .and(query_param("org", "o"))
            .and(query_param("bucket", "b"))
            .and(query_param("precision", "ns"))
            .and(header("authorization", "Token secret-token"))
            .respond_with(ResponseTemplate::new(204));
        let (uri, _server, _rt) = start_mock(mock);

        let test_runtime = TestRuntime::new();
        let exporter =
            create_exporter_from_factory(&INFLUXDB_EXPORTER, config_json(&uri)).expect("exporter");

        test_runtime
            .set_exporter(exporter)
            .run_test(|ctx| async move {
                let pdata = OtapPdata::new_default(logs_payload()).test_subscribe_to(
                    Interests::ACKS | Interests::NACKS,
                    TestCallData::default().into(),
                    12,
                );
                ctx.send_pdata(pdata).await.unwrap();
                ctx.send_shutdown(Instant::now() + Duration::from_secs(3), "done")
                    .await
                    .unwrap();
            })
            .run_validation(|mut ctx, result| async move {
                result.expect("success");
                let mut rx = ctx.take_pipeline_completion_receiver().unwrap();
                match rx.recv().await.unwrap() {
                    PipelineCompletionMsg::DeliverAck { ack } => {
                        let (node_id, _) = next_ack(ack).expect("ack subscriber");
                        assert_eq!(node_id, 12);
                    }
                    PipelineCompletionMsg::DeliverNack { nack } => {
                        let (_, nack) = next_nack(nack).expect("nack subscriber");
                        panic!("unexpected nack: {}", nack.reason);
                    }
                }
            });
    }

    fn assert_nack_for_status(status: u16, expect_permanent: bool) {
        let mock = Mock::given(method("POST"))
            .and(path("/api/v2/write"))
            .respond_with(ResponseTemplate::new(status));
        let (uri, _server, _rt) = start_mock(mock);

        let test_runtime = TestRuntime::new();
        let exporter =
            create_exporter_from_factory(&INFLUXDB_EXPORTER, config_json(&uri)).expect("exporter");

        test_runtime
            .set_exporter(exporter)
            .run_test(|ctx| async move {
                let pdata = OtapPdata::new_default(logs_payload()).test_subscribe_to(
                    Interests::NACKS,
                    TestCallData::default().into(),
                    13,
                );
                ctx.send_pdata(pdata).await.unwrap();
                ctx.send_shutdown(Instant::now() + Duration::from_secs(3), "done")
                    .await
                    .unwrap();
            })
            .run_validation(move |mut ctx, result| async move {
                result.expect("success");
                let mut rx = ctx.take_pipeline_completion_receiver().unwrap();
                match rx.recv().await.unwrap() {
                    PipelineCompletionMsg::DeliverNack { nack } => {
                        let (node_id, nack) = next_nack(nack).expect("nack subscriber");
                        assert_eq!(node_id, 13);
                        assert_eq!(
                            nack.permanent, expect_permanent,
                            "status {status}: unexpected permanent flag; reason: {}",
                            nack.reason
                        );
                    }
                    PipelineCompletionMsg::DeliverAck { .. } => {
                        panic!("unexpected ack for status {status}")
                    }
                }
            });
    }

    #[test]
    fn server_error_500_yields_retryable_nack() {
        assert_nack_for_status(500, false);
    }

    #[test]
    fn client_error_400_yields_permanent_nack() {
        assert_nack_for_status(400, true);
    }

    #[test]
    fn rate_limited_429_yields_retryable_nack() {
        assert_nack_for_status(429, false);
    }
}
