// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

/// Geneva Exporter for Microsoft telemetry backend
#[cfg(feature = "geneva-exporter")]
pub mod geneva_exporter;

/// Azure Monitor Exporter for Azure Logs Ingestion API
#[cfg(feature = "azure-monitor-exporter")]
pub mod azure_monitor_exporter;

/// InfluxDB Exporter writing OTLP data to the InfluxDB v2 write API
#[cfg(feature = "influxdb-exporter")]
pub mod influxdb_exporter;
