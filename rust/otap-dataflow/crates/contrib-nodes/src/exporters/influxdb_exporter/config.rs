// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the InfluxDB exporter.

use std::collections::HashSet;

use http::HeaderValue;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;

use otap_df_otap::otlp_http::client_settings::HttpClientSettings;

use super::error::Error;

/// Configuration for the InfluxDB exporter.
///
/// Data is written to the InfluxDB v2 write API at
/// `{endpoint}/api/v2/write?org=&bucket=&precision=` with an
/// `Authorization: Token <token>` header. This also covers InfluxDB 3.x
/// endpoints that expose the v2-compatible write API.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Base URL of the InfluxDB server, including scheme, host and port but not
    /// the write path. `/api/v2/write` is appended automatically.
    ///
    /// Example: `http://localhost:8086`
    pub endpoint: String,

    /// The destination organization for writes.
    pub org: String,

    /// The destination bucket for writes.
    pub bucket: String,

    /// The authentication token. Rendered redacted in `Debug` output and sent
    /// as `Authorization: Token <token>`.
    pub token: SecretString,

    /// The metrics schema to use when converting metrics to line protocol.
    #[serde(default)]
    pub metrics_schema: MetricsSchema,

    /// Span attributes (resolved against resource then span attributes) to
    /// promote to line-protocol tags for the `spans` measurement.
    #[serde(default = "default_span_dimensions")]
    pub span_dimensions: Vec<String>,

    /// Log record attributes (resolved against resource then log-record
    /// attributes) to promote to line-protocol tags for the `logs` measurement.
    #[serde(default = "default_log_record_dimensions")]
    pub log_record_dimensions: Vec<String>,

    /// Maximum number of line-protocol lines per write request. A single pdata
    /// message may be split across multiple requests.
    #[serde(default = "default_payload_max_lines")]
    pub payload_max_lines: usize,

    /// Maximum number of bytes per write request body.
    #[serde(default = "default_payload_max_bytes")]
    pub payload_max_bytes: usize,

    /// Timestamp precision used for the write request and line-protocol
    /// timestamps.
    #[serde(default)]
    pub precision: Precision,

    /// HTTP client settings (TLS, timeouts, extra headers, compression).
    #[serde(default)]
    pub http: HttpClientSettings,
}

/// The metrics-to-line-protocol schema.
///
/// Mirrors the OpenTelemetry Collector Contrib `influxdbexporter`
/// `metrics_schema` option.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
pub enum MetricsSchema {
    /// One measurement per metric; the field name identifies the metric kind
    /// (`gauge`, `counter`, ...). This is the default.
    #[default]
    #[serde(rename = "telegraf-prometheus-v1")]
    TelegrafPrometheusV1,

    /// A single `prometheus` measurement; the field name is the metric name.
    #[serde(rename = "telegraf-prometheus-v2")]
    TelegrafPrometheusV2,
}

/// Timestamp precision for the InfluxDB v2 write API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Precision {
    /// Nanoseconds (default).
    #[default]
    Ns,
    /// Microseconds.
    Us,
    /// Milliseconds.
    Ms,
    /// Seconds.
    S,
}

impl Precision {
    /// The value used for the `precision` query parameter.
    #[must_use]
    pub const fn as_query_value(&self) -> &'static str {
        match self {
            Self::Ns => "ns",
            Self::Us => "us",
            Self::Ms => "ms",
            Self::S => "s",
        }
    }

    /// The divisor used to convert a nanosecond timestamp to this precision.
    #[must_use]
    pub const fn divisor(&self) -> u64 {
        match self {
            Self::Ns => 1,
            Self::Us => 1_000,
            Self::Ms => 1_000_000,
            Self::S => 1_000_000_000,
        }
    }
}

fn default_span_dimensions() -> Vec<String> {
    vec!["service.name".to_string(), "span.name".to_string()]
}

fn default_log_record_dimensions() -> Vec<String> {
    vec!["service.name".to_string()]
}

const fn default_payload_max_lines() -> usize {
    10_000
}

const fn default_payload_max_bytes() -> usize {
    10_000_000
}

impl Config {
    /// Validates the configuration, returning an [`Error::Config`] describing
    /// the first problem found.
    pub fn validate(&self) -> Result<(), Error> {
        // endpoint must parse as an http/https URL.
        let url = reqwest::Url::parse(&self.endpoint)
            .map_err(|e| Error::Config(format!("invalid endpoint URL: {e}")))?;
        if url.scheme() != "http" && url.scheme() != "https" {
            return Err(Error::Config(format!(
                "endpoint scheme must be http or https, got \"{}\"",
                url.scheme()
            )));
        }

        if self.org.trim().is_empty() {
            return Err(Error::Config("org must not be empty".to_string()));
        }
        if self.bucket.trim().is_empty() {
            return Err(Error::Config("bucket must not be empty".to_string()));
        }
        if self.token.expose_secret().is_empty() {
            return Err(Error::Config("token must not be empty".to_string()));
        }

        // The token must be representable as an HTTP header value.
        if self.authorization_header().is_none() {
            return Err(Error::Config(
                "token contains characters that cannot be represented as an HTTP header value"
                    .to_string(),
            ));
        }

        if self.payload_max_lines == 0 {
            return Err(Error::Config(
                "payload_max_lines must be greater than 0".to_string(),
            ));
        }
        if self.payload_max_bytes == 0 {
            return Err(Error::Config(
                "payload_max_bytes must be greater than 0".to_string(),
            ));
        }

        check_no_duplicates("span_dimensions", &self.span_dimensions)?;
        check_no_duplicates("log_record_dimensions", &self.log_record_dimensions)?;

        self.http
            .validate()
            .map_err(|e| Error::Config(e.to_string()))?;

        Ok(())
    }

    /// Builds the `Authorization: Token <token>` header value, marked sensitive
    /// so it is redacted in `Debug` output and excluded from HPACK indexing.
    ///
    /// Returns `None` if the token cannot be represented as a header value.
    #[must_use]
    pub fn authorization_header(&self) -> Option<HeaderValue> {
        let mut value =
            HeaderValue::from_str(&format!("Token {}", self.token.expose_secret())).ok()?;
        value.set_sensitive(true);
        Some(value)
    }

    /// Builds the fully-qualified write URL, including the `org`, `bucket` and
    /// `precision` query parameters (percent-encoded).
    pub fn write_url(&self) -> Result<reqwest::Url, Error> {
        // Trim a single trailing slash so we don't produce a `//api/...` path.
        let base = self.endpoint.trim_end_matches('/');
        let mut url = reqwest::Url::parse(&format!("{base}/api/v2/write"))
            .map_err(|e| Error::Config(format!("invalid endpoint URL: {e}")))?;
        let _ = url
            .query_pairs_mut()
            .append_pair("org", &self.org)
            .append_pair("bucket", &self.bucket)
            .append_pair("precision", self.precision.as_query_value());
        Ok(url)
    }
}

fn check_no_duplicates(field: &str, values: &[String]) -> Result<(), Error> {
    let mut seen = HashSet::with_capacity(values.len());
    for v in values {
        if !seen.insert(v.as_str()) {
            return Err(Error::Config(format!(
                "{field} contains duplicate entry \"{v}\""
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> Config {
        Config {
            endpoint: "http://localhost:8086".to_string(),
            org: "my-org".to_string(),
            bucket: "my-bucket".to_string(),
            token: SecretString::from("my-token"),
            metrics_schema: MetricsSchema::default(),
            span_dimensions: default_span_dimensions(),
            log_record_dimensions: default_log_record_dimensions(),
            payload_max_lines: default_payload_max_lines(),
            payload_max_bytes: default_payload_max_bytes(),
            precision: Precision::default(),
            http: HttpClientSettings::default(),
        }
    }

    #[test]
    fn defaults_from_minimal_json() {
        let cfg: Config = serde_json::from_value(serde_json::json!({
            "endpoint": "http://localhost:8086",
            "org": "o",
            "bucket": "b",
            "token": "t"
        }))
        .expect("deserialize");
        assert_eq!(cfg.metrics_schema, MetricsSchema::TelegrafPrometheusV1);
        assert_eq!(cfg.precision, Precision::Ns);
        assert_eq!(cfg.payload_max_lines, 10_000);
        assert_eq!(cfg.payload_max_bytes, 10_000_000);
        assert_eq!(cfg.span_dimensions, vec!["service.name", "span.name"]);
        assert_eq!(cfg.log_record_dimensions, vec!["service.name"]);
        cfg.validate().expect("valid");
    }

    #[test]
    fn metrics_schema_parses_both_variants() {
        let v1: MetricsSchema =
            serde_json::from_value(serde_json::json!("telegraf-prometheus-v1")).unwrap();
        let v2: MetricsSchema =
            serde_json::from_value(serde_json::json!("telegraf-prometheus-v2")).unwrap();
        assert_eq!(v1, MetricsSchema::TelegrafPrometheusV1);
        assert_eq!(v2, MetricsSchema::TelegrafPrometheusV2);
    }

    #[test]
    fn precision_query_and_divisor() {
        assert_eq!(Precision::Ns.as_query_value(), "ns");
        assert_eq!(Precision::Us.as_query_value(), "us");
        assert_eq!(Precision::Ms.as_query_value(), "ms");
        assert_eq!(Precision::S.as_query_value(), "s");
        assert_eq!(Precision::Ns.divisor(), 1);
        assert_eq!(Precision::S.divisor(), 1_000_000_000);
    }

    #[test]
    fn write_url_has_expected_query() {
        let mut cfg = base_config();
        cfg.precision = Precision::Ms;
        let url = cfg.write_url().expect("url");
        assert_eq!(url.path(), "/api/v2/write");
        let pairs: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(pairs.get("org").map(String::as_str), Some("my-org"));
        assert_eq!(pairs.get("bucket").map(String::as_str), Some("my-bucket"));
        assert_eq!(pairs.get("precision").map(String::as_str), Some("ms"));
    }

    #[test]
    fn write_url_trims_trailing_slash() {
        let mut cfg = base_config();
        cfg.endpoint = "http://localhost:8086/".to_string();
        let url = cfg.write_url().expect("url");
        assert_eq!(url.path(), "/api/v2/write");
    }

    #[test]
    fn empty_org_bucket_token_rejected() {
        let mut cfg = base_config();
        cfg.org = String::new();
        assert!(cfg.validate().is_err());

        let mut cfg = base_config();
        cfg.bucket = "  ".to_string();
        assert!(cfg.validate().is_err());

        let mut cfg = base_config();
        cfg.token = SecretString::from("");
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn bad_endpoint_rejected() {
        let mut cfg = base_config();
        cfg.endpoint = "not a url".to_string();
        assert!(cfg.validate().is_err());

        let mut cfg = base_config();
        cfg.endpoint = "ftp://localhost".to_string();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn zero_payload_limits_rejected() {
        let mut cfg = base_config();
        cfg.payload_max_lines = 0;
        assert!(cfg.validate().is_err());

        let mut cfg = base_config();
        cfg.payload_max_bytes = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn duplicate_dimensions_rejected() {
        let mut cfg = base_config();
        cfg.span_dimensions = vec!["a".to_string(), "a".to_string()];
        assert!(cfg.validate().is_err());

        let mut cfg = base_config();
        cfg.log_record_dimensions = vec!["x".to_string(), "y".to_string(), "x".to_string()];
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn unknown_fields_rejected() {
        let res: Result<Config, _> = serde_json::from_value(serde_json::json!({
            "endpoint": "http://localhost:8086",
            "org": "o",
            "bucket": "b",
            "token": "t",
            "bogus": true
        }));
        assert!(res.is_err());
    }

    #[test]
    fn token_absent_from_debug_output() {
        let cfg = base_config();
        let debug = format!("{cfg:?}");
        assert!(
            !debug.contains("my-token"),
            "token leaked in debug output: {debug}"
        );
    }

    #[test]
    fn authorization_header_is_sensitive() {
        let cfg = base_config();
        let header = cfg.authorization_header().expect("header");
        assert!(header.is_sensitive());
    }
}
