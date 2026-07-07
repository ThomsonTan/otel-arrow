// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! InfluxDB exporter for OTAP.
//!
//! Writes OpenTelemetry metrics, logs, and traces to InfluxDB using the v2 write
//! API (`POST {endpoint}/api/v2/write`). The line-protocol mapping follows the
//! OpenTelemetry Collector Contrib `influxdbexporter`, which is built on
//! influxdata's `otel2influx` library.
//!
//! # Signals
//!
//! - **Metrics**: both `telegraf-prometheus-v1` (default) and
//!   `telegraf-prometheus-v2` schemas. Exponential histograms are unsupported
//!   and dropped.
//! - **Logs**: mapped to the `logs` measurement.
//! - **Traces**: spans to the `spans` measurement, span events to `logs`, span
//!   links to `span-links`.
//!
//! # Design
//!
//! Conversion is written once against the backend-agnostic view traits
//! (`MetricsView` / `LogsDataView` / `TracesView`) and dispatched over both the
//! OTLP-bytes and OTAP-Arrow backends, so no encode-to-OTLP-bytes fallback is
//! needed.
//!
//! Writes are sequential: each pdata message is converted and POSTed
//! synchronously, then acked on success or nacked (with a `permanent` flag) on
//! failure. Retries are delegated upstream via nack, mirroring the OTLP/HTTP
//! exporter.

use std::sync::Arc;

use linkme::distributed_slice;
use otap_df_config::error::Error as ConfigError;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::ExporterFactory;
use otap_df_engine::config::ExporterConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::exporter::ExporterWrapper;
use otap_df_engine::node::NodeId;
use otap_df_engine::wiring_contract::WiringContract;
use otap_df_otap::OTAP_EXPORTER_FACTORIES;
use otap_df_otap::pdata::OtapPdata;

/// Configuration types for the InfluxDB exporter.
pub mod config;
mod error;
mod exporter;
mod line_protocol;
/// Metrics types for the InfluxDB exporter.
pub mod metrics;

mod client;
mod convert;

pub use config::Config;
pub use error::Error;
pub use exporter::InfluxdbExporter;

/// URN identifying the InfluxDB exporter in configuration pipelines.
pub const INFLUXDB_EXPORTER_URN: &str = "urn:otel:exporter:influxdb";

/// Register the InfluxDB exporter with the OTAP exporter factory.
///
/// Uses the `distributed_slice` macro for automatic discovery by the dataflow
/// engine.
#[allow(unsafe_code)]
#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
pub static INFLUXDB_EXPORTER: ExporterFactory<OtapPdata> = ExporterFactory {
    name: INFLUXDB_EXPORTER_URN,
    create: factory_create,
    wiring_contract: WiringContract::UNRESTRICTED,
    validate_config: otap_df_config::validation::validate_typed_config::<Config>,
};

fn factory_create(
    pipeline_ctx: PipelineContext,
    node: NodeId,
    node_config: Arc<NodeUserConfig>,
    exporter_config: &ExporterConfig,
    _capabilities: &otap_df_engine::capability::registry::Capabilities,
) -> Result<ExporterWrapper<OtapPdata>, ConfigError> {
    let cfg: Config = serde_json::from_value(node_config.config.clone()).map_err(|e| {
        ConfigError::InvalidUserConfig {
            error: e.to_string(),
        }
    })?;

    Ok(ExporterWrapper::local(
        InfluxdbExporter::new(pipeline_ctx, cfg).map_err(|e| ConfigError::InvalidUserConfig {
            error: e.to_string(),
        })?,
        node,
        node_config,
        exporter_config,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_urn_constant() {
        assert_eq!(INFLUXDB_EXPORTER_URN, "urn:otel:exporter:influxdb");
    }
}
