// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! A minimal HTTP receiver for learning purposes.
//!
//! Listens on a configurable address and accepts `POST /ingest` requests.
//! The request body (any format) is wrapped as an OTLP logs payload and
//! forwarded into the pipeline as `OtapPdata`.
//!
//! # Example config (YAML)
//!
//! ```yaml
//! receiver:
//!   type: receiver:simple_http
//!   config:
//!     listening_addr: "127.0.0.1:9090"
//! ```

use crate::pdata::OtapPdata;
use crate::OTAP_RECEIVER_FACTORIES;

use async_trait::async_trait;
use bytes::Bytes;
use http::{Method, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use linkme::distributed_slice;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::config::ReceiverConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::control::NodeControlMsg;
use otap_df_engine::error::Error;
use otap_df_engine::local::receiver::{ControlChannel, EffectHandler, Receiver};
use otap_df_engine::node::NodeId;
use otap_df_engine::receiver::ReceiverWrapper;
use otap_df_engine::terminal_state::TerminalState;
use otap_df_engine::ReceiverFactory;
use otap_df_pdata::OtapPayload;
use otap_df_pdata::OtlpProtoBytes;
use serde::Deserialize;
use std::cell::Cell;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;

/// Ingest endpoint path.
const INGEST_PATH: &str = "/ingest";

/// URN that identifies this receiver in pipeline configuration.
pub const SIMPLE_HTTP_RECEIVER_URN: &str = "urn:otel:receiver:simple_http";

/// Configuration for the simple HTTP receiver.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Address to listen on (e.g. `127.0.0.1:9090`).
    pub listening_addr: SocketAddr,
}

/// A minimal HTTP receiver that forwards POST bodies as OTLP logs.
struct SimpleHttpReceiver {
    config: Config,
}

impl SimpleHttpReceiver {
    fn from_config(config: &serde_json::Value) -> Result<Self, otap_df_config::error::Error> {
        let config: Config =
            serde_json::from_value(config.clone()).map_err(|e| {
                otap_df_config::error::Error::InvalidUserConfig {
                    error: e.to_string(),
                }
            })?;
        Ok(Self { config })
    }
}

// ── Factory registration ────────────────────────────────────────────
#[allow(unsafe_code)]
#[distributed_slice(OTAP_RECEIVER_FACTORIES)]
/// Factory that registers the simple HTTP receiver with the pipeline engine.
pub static SIMPLE_HTTP_RECEIVER_FACTORY: ReceiverFactory<OtapPdata> = ReceiverFactory {
    name: SIMPLE_HTTP_RECEIVER_URN,
    create: |_pipeline_ctx: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             recv_cfg: &ReceiverConfig| {
        Ok(ReceiverWrapper::local(
            SimpleHttpReceiver::from_config(&node_config.config)?,
            node,
            node_config,
            recv_cfg,
        ))
    },
    wiring_contract: otap_df_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otap_df_config::validation::validate_typed_config::<Config>,
};

// ── Receiver trait implementation ───────────────────────────────────
#[async_trait(?Send)]
impl Receiver<OtapPdata> for SimpleHttpReceiver {
    async fn start(
        self: Box<Self>,
        mut ctrl_chan: ControlChannel<OtapPdata>,
        effect_handler: EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        // Create a TCP listener via the effect handler (gets SO_REUSEPORT for free).
        let listener = effect_handler.tcp_listener(self.config.listening_addr)?;

        // Shared shutdown flag for spawned connection tasks.
        let shutdown = Rc::new(Cell::new(false));

        loop {
            tokio::select! {
                biased;

                // Priority: handle control messages first.
                ctrl = ctrl_chan.recv() => {
                    match ctrl? {
                        NodeControlMsg::Shutdown { .. } => {
                            shutdown.set(true);
                            return Ok(TerminalState::default());
                        }
                        _ => {}
                    }
                }

                // Accept incoming TCP connections.
                accept = listener.accept() => {
                    let (stream, _peer) = accept.map_err(|e| Error::IoError {
                        node: effect_handler.receiver_id(),
                        error: e,
                    })?;

                    let eh = effect_handler.clone();
                    let shutdown = shutdown.clone();

                    // Spawn a per-connection task on the local (single-threaded) runtime.
                    drop(tokio::task::spawn_local(async move {
                        if shutdown.get() {
                            return;
                        }

                        let io = TokioIo::new(stream);

                        // Serve HTTP/1.1 on this connection.
                        let result = hyper::server::conn::http1::Builder::new()
                            .serve_connection(
                                io,
                                service_fn(|req| handle_request(req, eh.clone())),
                            )
                            .await;

                        if let Err(err) = result {
                            eprintln!("simple_http_receiver: connection error: {err}");
                        }
                    }));
                }
            }
        }
    }
}

/// Handle a single HTTP request.
async fn handle_request(
    req: http::Request<Incoming>,
    effect_handler: EffectHandler<OtapPdata>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    // Only accept POST /ingest.
    if req.method() != Method::POST || req.uri().path() != INGEST_PATH {
        return Ok(response(StatusCode::NOT_FOUND, "Not Found"));
    }

    // Read the full request body.
    let body = match req.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => {
            return Ok(response(
                StatusCode::BAD_REQUEST,
                "Failed to read body",
            ));
        }
    };

    // Wrap body bytes as an OTLP logs payload and send downstream.
    let payload = OtapPayload::OtlpBytes(OtlpProtoBytes::ExportLogsRequest(body));
    let pdata = OtapPdata::new_todo_context(payload);

    match effect_handler.send_message(pdata).await {
        Ok(()) => Ok(response(StatusCode::OK, "OK")),
        Err(_) => Ok(response(
            StatusCode::SERVICE_UNAVAILABLE,
            "Pipeline unavailable",
        )),
    }
}

/// Build a simple text response.
fn response(status: StatusCode, body: &str) -> Response<Full<Bytes>> {
    let mut resp = Response::new(Full::new(Bytes::from(body.to_owned())));
    *resp.status_mut() = status;
    resp
}
