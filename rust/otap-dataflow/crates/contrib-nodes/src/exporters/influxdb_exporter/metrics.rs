// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Telemetry metrics for the InfluxDB exporter.

use otap_df_telemetry::instrument::Counter;
use otap_df_telemetry_macros::metric_set;

/// Metrics specific to the InfluxDB exporter, in addition to the generic
/// `exporter.pdata` metrics. Namespace: `influxdb.exporter`.
#[metric_set(name = "influxdb.exporter")]
#[derive(Debug, Default, Clone)]
pub struct InfluxdbExporterMetrics {
    /// Number of write requests issued to the InfluxDB v2 write API.
    #[metric(unit = "{request}")]
    pub write_requests: Counter<u64>,

    /// Number of write requests that failed with a retryable error.
    #[metric(unit = "{request}")]
    pub write_failures_retryable: Counter<u64>,

    /// Number of write requests that failed with a permanent error.
    #[metric(unit = "{request}")]
    pub write_failures_permanent: Counter<u64>,

    /// Number of line-protocol lines written.
    #[metric(unit = "{line}")]
    pub lines_written: Counter<u64>,

    /// Number of exponential-histogram data points dropped (unsupported).
    #[metric(unit = "{datapoint}")]
    pub exp_histograms_dropped: Counter<u64>,

    /// Number of points dropped for having no representable field value.
    #[metric(unit = "{point}")]
    pub invalid_points_dropped: Counter<u64>,
}

impl InfluxdbExporterMetrics {
    /// Records an issued write request.
    pub const fn add_write_request(&mut self) {
        self.write_requests.inc();
    }

    /// Records a retryable write failure.
    pub const fn add_write_failure_retryable(&mut self) {
        self.write_failures_retryable.inc();
    }

    /// Records a permanent write failure.
    pub const fn add_write_failure_permanent(&mut self) {
        self.write_failures_permanent.inc();
    }

    /// Adds to the number of line-protocol lines written.
    pub const fn add_lines_written(&mut self, count: u64) {
        self.lines_written.add(count);
    }

    /// Adds to the number of dropped exponential-histogram data points.
    pub const fn add_exp_histograms_dropped(&mut self, count: u64) {
        self.exp_histograms_dropped.add(count);
    }

    /// Adds to the number of dropped invalid points.
    pub const fn add_invalid_points_dropped(&mut self, count: u64) {
        self.invalid_points_dropped.add(count);
    }
}
