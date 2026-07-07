// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Hand-rolled InfluxDB line-protocol serialization.
//!
//! [`LineBuilder`] accumulates a single measurement's tags and fields;
//! [`LinesBatcher`] renders built lines and packs them into `\n`-separated
//! request payloads bounded by a maximum line count and byte size.
//!
//! Escaping follows the InfluxDB line-protocol specification:
//! - measurement: escape `,` and space
//! - tag key / tag value / field key: escape `,`, `=` and space
//! - string field value: escape `"` and `\`, wrapped in double quotes
//!
//! Integer fields are suffixed with `i`, unsigned fields with `u`.

use bytes::Bytes;

use super::config::Precision;

/// A single line-protocol field value.
#[derive(Debug, Clone, PartialEq)]
enum FieldValue {
    /// IEEE-754 double, rendered without a type suffix.
    Float(f64),
    /// Signed integer, rendered with an `i` suffix.
    Int(i64),
    /// Unsigned integer, rendered with a `u` suffix.
    UInt(u64),
    /// Boolean, rendered as `true`/`false`.
    Bool(bool),
    /// String, rendered double-quoted with `"`/`\` escaped.
    Str(String),
}

/// Builder for a single line-protocol line.
///
/// Tags are collected and sorted by key at render time. Lines with zero fields
/// are dropped by [`LinesBatcher::push`].
#[derive(Debug)]
pub struct LineBuilder {
    measurement: String,
    tags: Vec<(String, String)>,
    fields: Vec<(String, FieldValue)>,
    timestamp: Option<u64>,
}

impl LineBuilder {
    /// Creates a new builder for the given measurement.
    #[must_use]
    pub fn new(measurement: impl Into<String>) -> Self {
        Self {
            measurement: measurement.into(),
            tags: Vec::new(),
            fields: Vec::new(),
            timestamp: None,
        }
    }

    /// Adds a tag. Tags with an empty key or empty value are ignored, since the
    /// line protocol does not permit empty tag keys or values.
    pub fn tag(&mut self, key: impl Into<String>, value: impl Into<String>) {
        let key = key.into();
        let value = value.into();
        if !key.is_empty() && !value.is_empty() {
            self.tags.push((key, value));
        }
    }

    /// Adds a string field.
    pub fn field_str(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.push_field(key, FieldValue::Str(value.into()));
    }

    /// Adds a floating-point field. Non-finite values (NaN, +/-Inf) are ignored
    /// because InfluxDB rejects them; the caller is expected to count these as
    /// dropped points.
    pub fn field_f64(&mut self, key: impl Into<String>, value: f64) {
        if value.is_finite() {
            self.push_field(key, FieldValue::Float(value));
        }
    }

    /// Adds a signed-integer field (rendered with an `i` suffix).
    pub fn field_i64(&mut self, key: impl Into<String>, value: i64) {
        self.push_field(key, FieldValue::Int(value));
    }

    /// Adds an unsigned-integer field (rendered with a `u` suffix).
    pub fn field_u64(&mut self, key: impl Into<String>, value: u64) {
        self.push_field(key, FieldValue::UInt(value));
    }

    /// Adds a boolean field.
    pub fn field_bool(&mut self, key: impl Into<String>, value: bool) {
        self.push_field(key, FieldValue::Bool(value));
    }

    /// Sets the timestamp, expressed in nanoseconds since the Unix epoch. The
    /// value is scaled to the configured precision when the line is rendered.
    pub fn timestamp(&mut self, unix_nanos: u64) {
        self.timestamp = Some(unix_nanos);
    }

    fn push_field(&mut self, key: impl Into<String>, value: FieldValue) {
        let key = key.into();
        if !key.is_empty() {
            self.fields.push((key, value));
        }
    }
}

/// Accumulates rendered lines into `\n`-separated request payloads bounded by
/// `max_lines` and `max_bytes`.
///
/// A single pdata message may therefore produce multiple payloads. A single
/// line larger than `max_bytes` is emitted as its own (oversized) payload
/// rather than being dropped or split.
#[derive(Debug)]
pub struct LinesBatcher {
    max_lines: usize,
    max_bytes: usize,
    precision: Precision,
    current: Vec<u8>,
    current_lines: usize,
    completed: Vec<Bytes>,
    total_lines: u64,
    scratch: Vec<u8>,
}

impl LinesBatcher {
    /// Creates a new batcher. `max_lines` and `max_bytes` are clamped to at
    /// least 1 to guarantee forward progress.
    #[must_use]
    pub fn new(max_lines: usize, max_bytes: usize, precision: Precision) -> Self {
        Self {
            max_lines: max_lines.max(1),
            max_bytes: max_bytes.max(1),
            precision,
            current: Vec::new(),
            current_lines: 0,
            completed: Vec::new(),
            total_lines: 0,
            scratch: Vec::new(),
        }
    }

    /// Renders and appends a line. Lines with no fields or an empty measurement
    /// are dropped.
    pub fn push(&mut self, mut line: LineBuilder) {
        if line.fields.is_empty() || line.measurement.is_empty() {
            return;
        }

        self.scratch.clear();
        escape_into(
            &line.measurement,
            EscapeKind::Measurement,
            &mut self.scratch,
        );

        line.tags.sort_by(|a, b| a.0.cmp(&b.0));
        for (key, value) in &line.tags {
            self.scratch.push(b',');
            escape_into(key, EscapeKind::Tag, &mut self.scratch);
            self.scratch.push(b'=');
            escape_into(value, EscapeKind::Tag, &mut self.scratch);
        }

        self.scratch.push(b' ');
        for (i, (key, value)) in line.fields.iter().enumerate() {
            if i > 0 {
                self.scratch.push(b',');
            }
            escape_into(key, EscapeKind::Tag, &mut self.scratch);
            self.scratch.push(b'=');
            render_field_value(value, &mut self.scratch);
        }

        if let Some(ts) = line.timestamp {
            let scaled = i64::try_from(ts / self.precision.divisor()).unwrap_or(i64::MAX);
            self.scratch.push(b' ');
            let mut buf = itoa::Buffer::new();
            self.scratch
                .extend_from_slice(buf.format(scaled).as_bytes());
        }

        let line_len = self.scratch.len();
        let would_exceed_bytes = self.current.len() + 1 + line_len > self.max_bytes;
        if !self.current.is_empty() && (self.current_lines >= self.max_lines || would_exceed_bytes)
        {
            self.flush_current();
        }

        if !self.current.is_empty() {
            self.current.push(b'\n');
        }
        self.current.extend_from_slice(&self.scratch);
        self.current_lines += 1;
        self.total_lines += 1;
    }

    /// Flushes any buffered line into the completed set and returns all
    /// completed payloads, leaving the batcher empty.
    pub fn finish(&mut self) -> Vec<Bytes> {
        self.flush_current();
        std::mem::take(&mut self.completed)
    }

    /// Total number of lines rendered so far (excluding dropped lines).
    #[must_use]
    pub fn total_lines(&self) -> u64 {
        self.total_lines
    }

    fn flush_current(&mut self) {
        if !self.current.is_empty() {
            self.completed
                .push(Bytes::from(std::mem::take(&mut self.current)));
            self.current_lines = 0;
        }
    }
}

/// The escaping profile to apply to a token.
#[derive(Clone, Copy)]
enum EscapeKind {
    /// Measurement name: escape `,` and space.
    Measurement,
    /// Tag key, tag value, or field key: escape `,`, `=` and space.
    Tag,
}

fn escape_into(input: &str, kind: EscapeKind, out: &mut Vec<u8>) {
    for &b in input.as_bytes() {
        let escape = match kind {
            EscapeKind::Measurement => b == b',' || b == b' ',
            EscapeKind::Tag => b == b',' || b == b'=' || b == b' ',
        };
        if escape {
            out.push(b'\\');
        }
        out.push(b);
    }
}

fn render_field_value(value: &FieldValue, out: &mut Vec<u8>) {
    match value {
        FieldValue::Float(f) => {
            let mut buf = ryu::Buffer::new();
            out.extend_from_slice(buf.format_finite(*f).as_bytes());
        }
        FieldValue::Int(i) => {
            let mut buf = itoa::Buffer::new();
            out.extend_from_slice(buf.format(*i).as_bytes());
            out.push(b'i');
        }
        FieldValue::UInt(u) => {
            let mut buf = itoa::Buffer::new();
            out.extend_from_slice(buf.format(*u).as_bytes());
            out.push(b'u');
        }
        FieldValue::Bool(b) => {
            out.extend_from_slice(if *b { b"true" } else { b"false" });
        }
        FieldValue::Str(s) => {
            out.push(b'"');
            for &b in s.as_bytes() {
                if b == b'"' || b == b'\\' {
                    out.push(b'\\');
                }
                out.push(b);
            }
            out.push(b'"');
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Render a single line to a String using a batcher with generous limits.
    fn render_one(line: LineBuilder, precision: Precision) -> String {
        let mut batcher = LinesBatcher::new(1_000, 10_000_000, precision);
        batcher.push(line);
        let payloads = batcher.finish();
        assert!(payloads.len() <= 1);
        payloads
            .first()
            .map(|b| String::from_utf8(b.to_vec()).expect("utf8"))
            .unwrap_or_default()
    }

    #[test]
    fn measurement_escapes_comma_and_space_not_equals() {
        let mut line = LineBuilder::new("m,a b=c");
        line.field_i64("f", 1);
        let out = render_one(line, Precision::Ns);
        assert_eq!(out, "m\\,a\\ b=c f=1i");
    }

    #[test]
    fn tags_escaped_and_sorted() {
        let mut line = LineBuilder::new("m");
        line.tag("b key", "b,v");
        line.tag("a", "x=y");
        line.field_i64("f", 1);
        let out = render_one(line, Precision::Ns);
        // sorted by key: a before "b key"
        assert_eq!(out, "m,a=x\\=y,b\\ key=b\\,v f=1i");
    }

    #[test]
    fn empty_tag_value_skipped() {
        let mut line = LineBuilder::new("m");
        line.tag("k", "");
        line.tag("", "v");
        line.field_i64("f", 1);
        let out = render_one(line, Precision::Ns);
        assert_eq!(out, "m f=1i");
    }

    #[test]
    fn field_types_render_with_suffixes() {
        let mut line = LineBuilder::new("m");
        line.field_i64("i", -7);
        line.field_u64("u", 8);
        line.field_bool("b", true);
        line.field_f64("f", 1.5);
        let out = render_one(line, Precision::Ns);
        assert_eq!(out, "m i=-7i,u=8u,b=true,f=1.5");
    }

    #[test]
    fn string_field_escapes_quote_and_backslash() {
        let mut line = LineBuilder::new("m");
        line.field_str("s", "a\"b\\c");
        let out = render_one(line, Precision::Ns);
        assert_eq!(out, "m s=\"a\\\"b\\\\c\"");
    }

    #[test]
    fn non_finite_float_field_skipped() {
        let mut line = LineBuilder::new("m");
        line.field_f64("bad", f64::NAN);
        line.field_f64("inf", f64::INFINITY);
        // No finite fields remain, so the zero-field line is dropped.
        let out = render_one(line, Precision::Ns);
        assert_eq!(out, "");
    }

    #[test]
    fn zero_field_line_dropped() {
        let mut line = LineBuilder::new("m");
        line.tag("k", "v");
        let out = render_one(line, Precision::Ns);
        assert_eq!(out, "");
    }

    #[test]
    fn timestamp_scaled_by_precision() {
        let mut line = LineBuilder::new("m");
        line.field_i64("f", 1);
        line.timestamp(1_500_000_000);
        let out = render_one(line, Precision::Ms);
        // 1_500_000_000 ns / 1_000_000 = 1500 ms
        assert_eq!(out, "m f=1i 1500");
    }

    #[test]
    fn timestamp_nanoseconds_unscaled() {
        let mut line = LineBuilder::new("m");
        line.field_i64("f", 1);
        line.timestamp(1_700_000_000_000_000_123);
        let out = render_one(line, Precision::Ns);
        assert_eq!(out, "m f=1i 1700000000000000123");
    }

    #[test]
    fn batcher_splits_at_max_lines() {
        let mut batcher = LinesBatcher::new(2, 10_000_000, Precision::Ns);
        for i in 0..5 {
            let mut line = LineBuilder::new("m");
            line.field_i64("f", i);
            batcher.push(line);
        }
        let payloads = batcher.finish();
        // 5 lines, 2 per payload -> 3 payloads (2,2,1)
        assert_eq!(payloads.len(), 3);
        assert_eq!(batcher.total_lines(), 5);
        let first = String::from_utf8(payloads[0].to_vec()).unwrap();
        assert_eq!(first.lines().count(), 2);
        let last = String::from_utf8(payloads[2].to_vec()).unwrap();
        assert_eq!(last.lines().count(), 1);
    }

    #[test]
    fn batcher_splits_at_max_bytes() {
        // Each line "m f=Ni" is ~6 bytes. Set a small byte cap.
        let mut batcher = LinesBatcher::new(1_000, 12, Precision::Ns);
        for i in 0..4 {
            let mut line = LineBuilder::new("m");
            line.field_i64("f", i);
            batcher.push(line);
        }
        let payloads = batcher.finish();
        assert!(payloads.len() > 1);
        for p in &payloads {
            // No single payload should exceed the cap for these small lines.
            assert!(p.len() <= 12, "payload too large: {}", p.len());
        }
    }

    #[test]
    fn oversized_single_line_emitted_alone() {
        let big = "x".repeat(100);
        let mut batcher = LinesBatcher::new(1_000, 10, Precision::Ns);
        let mut line = LineBuilder::new("m");
        line.field_str("s", big);
        batcher.push(line);
        let payloads = batcher.finish();
        assert_eq!(payloads.len(), 1);
        assert!(payloads[0].len() > 10);
    }
}
