// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! HTTP client for the InfluxDB v2 write API.

use bytes::Bytes;
use http::header::{AUTHORIZATION, CONTENT_ENCODING, CONTENT_TYPE, HeaderMap, HeaderValue};
use reqwest::{Client, StatusCode};
use secrecy::ExposeSecret;

use otap_df_otap::compression::CompressionMethod;
use otap_df_otap::otlp_http::client_settings::HttpClientSettings;

use super::config::Config;
use super::error::{Error, WriteError};

/// Line-protocol content type expected by the InfluxDB write API.
const LINE_PROTOCOL_CONTENT_TYPE: &str = "text/plain; charset=utf-8";

/// Maximum number of response-body bytes retained in an error message.
const MAX_ERROR_BODY_LEN: usize = 1024;

/// A client bound to a single InfluxDB v2 write endpoint.
pub struct InfluxdbClient {
    client: Client,
    write_url: reqwest::Url,
    compression: Option<CompressionMethod>,
}

impl InfluxdbClient {
    /// Builds a client from the exporter configuration.
    pub async fn new(cfg: &Config) -> Result<Self, Error> {
        let write_url = cfg.write_url()?;
        let authorization = cfg
            .authorization_header()
            .ok_or_else(|| Error::Config("invalid authorization token".to_string()))?;

        let mut default_headers = build_static_headers(&cfg.http)?;
        // Our managed headers win over any user-provided values.
        let _ = default_headers.insert(AUTHORIZATION, authorization);
        let _ = default_headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static(LINE_PROTOCOL_CONTENT_TYPE),
        );

        let client = cfg
            .http
            .client_builder()
            .await
            .map_err(|e| Error::CreateClient(e.to_string()))?
            .default_headers(default_headers)
            .build()
            .map_err(|e| Error::CreateClient(e.to_string()))?;

        Ok(Self {
            client,
            write_url,
            compression: cfg.http.compression(),
        })
    }

    /// Writes a line-protocol payload to the configured bucket.
    ///
    /// Returns `Ok(())` on a 2xx response (the v2 API returns 204). Errors carry
    /// a `retryable` classification: 429 and 5xx responses, and connect/timeout
    /// transport errors, are retryable; other 4xx responses are permanent.
    pub async fn write(&self, body: Bytes) -> Result<(), WriteError> {
        let mut request = self.client.post(self.write_url.clone());

        request = match self.compression {
            Some(method) => {
                let mut compressed = Vec::new();
                method
                    .encode(&body, &mut compressed)
                    .map_err(|e| WriteError::Request {
                        message: format!("failed to compress payload: {e}"),
                        retryable: false,
                    })?;
                request
                    .body(compressed)
                    .header(CONTENT_ENCODING, method.as_http_content_encoding())
            }
            None => request.body(body),
        };

        let response = match request.send().await {
            Ok(response) => response,
            Err(e) => {
                let retryable = e.is_connect() || e.is_timeout();
                return Err(WriteError::Request {
                    message: e.to_string(),
                    retryable,
                });
            }
        };

        let status = response.status();
        if status.is_success() {
            return Ok(());
        }

        let retryable = status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error();
        let body = read_body_snippet(response).await;
        Err(WriteError::Server {
            status: status.as_u16(),
            body,
            retryable,
        })
    }
}

/// Builds the user-configured static headers, marking each value sensitive so
/// it is redacted in `Debug` output and excluded from HPACK indexing. Mirrors
/// the OTLP/HTTP exporter's `build_static_headers`.
fn build_static_headers(settings: &HttpClientSettings) -> Result<HeaderMap, Error> {
    let mut headers = HeaderMap::with_capacity(settings.headers.len() + 2);
    for (name, value) in &settings.headers {
        let header_name = http::HeaderName::from_bytes(name.as_bytes())
            .map_err(|e| Error::Config(format!("invalid header name \"{name}\": {e}")))?;
        let mut header_value = HeaderValue::from_str(value.expose_secret())
            .map_err(|e| Error::Config(format!("invalid value for header \"{name}\": {e}")))?;
        header_value.set_sensitive(true);
        let _ = headers.insert(header_name, header_value);
    }
    Ok(headers)
}

async fn read_body_snippet(response: reqwest::Response) -> String {
    let text = response.text().await.unwrap_or_default();
    truncate_snippet(text, MAX_ERROR_BODY_LEN)
}

fn truncate_snippet(mut s: String, max: usize) -> String {
    if s.len() > max {
        let mut end = max;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        s.truncate(end);
        s.push_str("...");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_snippet_keeps_short_strings() {
        assert_eq!(truncate_snippet("short".to_string(), 1024), "short");
    }

    #[test]
    fn truncate_snippet_truncates_long_strings() {
        let s = "a".repeat(2000);
        let out = truncate_snippet(s, 1024);
        assert_eq!(out.len(), 1024 + 3);
        assert!(out.ends_with("..."));
    }
}
