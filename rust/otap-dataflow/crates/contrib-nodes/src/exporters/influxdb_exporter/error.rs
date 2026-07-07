// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Error types for the InfluxDB exporter.

/// Errors produced by the InfluxDB exporter.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Invalid or inconsistent configuration.
    #[error("Configuration error: {0}")]
    Config(String),

    /// Failed to build a backend view over the inbound pdata payload.
    #[error("Failed to create {signal} view: {source}")]
    ViewCreation {
        /// The signal whose view failed to build ("logs", "metrics", "traces").
        signal: &'static str,
        /// The underlying pdata error.
        #[source]
        source: otap_df_pdata::error::Error,
    },

    /// Failed to serialize a complex attribute value to a JSON string.
    #[error("Failed to serialize attribute value: {0}")]
    Serialize(#[source] serde_json::Error),

    /// Failed to construct the HTTP client from the configured settings.
    #[error("Failed to create HTTP client: {0}")]
    CreateClient(String),

    /// A write request to the InfluxDB v2 API failed.
    #[error(transparent)]
    Write(#[from] WriteError),
}

/// Errors originating from the InfluxDB v2 write request path.
///
/// Each variant carries a `retryable` flag used by the exporter to decide
/// whether the corresponding nack should be marked permanent.
#[derive(thiserror::Error, Debug)]
pub enum WriteError {
    /// The HTTP request could not be completed (connection refused, timeout,
    /// TLS error, ...). No HTTP status was received.
    #[error("HTTP request error: {message}")]
    Request {
        /// Human-readable description of the transport failure.
        message: String,
        /// Whether the request may be retried.
        retryable: bool,
    },

    /// The server returned a non-success HTTP status code.
    #[error("InfluxDB returned HTTP {status}: {body}")]
    Server {
        /// The HTTP status code returned by the server.
        status: u16,
        /// A bounded snippet of the response body (InfluxDB returns JSON error
        /// details).
        body: String,
        /// Whether the request may be retried.
        retryable: bool,
    },
}

impl WriteError {
    /// Returns whether the failed write may be retried.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Request { retryable, .. } | Self::Server { retryable, .. } => *retryable,
        }
    }
}

impl Error {
    /// Returns whether the error may be retried by an upstream component.
    ///
    /// Only transport/server write failures classified as retryable qualify;
    /// configuration and conversion errors are always permanent.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Write(w) => w.is_retryable(),
            _ => false,
        }
    }
}
