use fusion_derive::{LogicalTask, MapLogicTask, SrcLogicTask};
use fusion_unit_sdk::{
    GraphUnitPlugin, UnitManifest,
    graph::types::{InitUnit, SourceUnit, TaskContext, UnitMeta},
    proto::transfer::{Column, DataType, Frame},
    runtime::{UnitError, UnitResult},
    units::config_util::UnitConfigExt,
};
use http_body_util::{BodyExt, Full};
use hyper::{
    body::{Bytes, Incoming},
    header,
    server::conn::http1,
    service::service_fn,
    Request, Response, StatusCode,
};
use hyper_util::rt::TokioIo;
use protobuf::EnumOrUnknown;
use std::{
    convert::Infallible,
    future::Future,
    sync::{Arc, Mutex},
};
use tokio::{
    net::TcpListener,
    sync::{oneshot, Semaphore},
};

#[cfg(feature = "cdylib")]
#[unsafe(no_mangle)]
pub extern "C" fn init_plugin() -> Box<dyn GraphUnitPlugin + Send + Sync> {
    Box::new(NetUnitPlugin {})
}

pub struct NetUnitPlugin {}

impl GraphUnitPlugin for NetUnitPlugin {
    fn register_units(&self) -> fusion_unit_sdk::UnitManifest {
        let mut unit_manifest = UnitManifest::default();
        HttpEndpointUnitTask::register_unit(&mut unit_manifest, &self.plugin_version());
        unit_manifest
    }
}

#[derive(Default, SrcLogicTask)]
pub struct HttpEndpointUnitTask {
    meta: UnitMeta,
    max_connections: Option<u32>,
    max_content_length: Option<u32>,
    port: Option<u16>,
    uri: Option<String>,
    api_key: Option<String>,
    remote_shutdown_enabled: Option<bool>,
}

impl InitUnit for HttpEndpointUnitTask {
    fn init(
        &mut self,
        unit: fusion_unit_sdk::graph::types::ComputingUnit,
    ) -> fusion_unit_sdk::runtime::UnitResult<()> {
        self.max_connections = Some(3);
        self.max_content_length = Some(u32::MAX);
        self.port = None;
        // meta is not populated until after init — use the unit id directly.
        self.uri = Some(format!("/source/{}", unit.get_id()));
        self.api_key = None;
        self.remote_shutdown_enabled = Some(false);

        if let Some(Err(err)) = unit.get_config().map::<UnitResult<()>, _>(|c| {
            self.max_connections = c.extract_u32("max_connections")?.or(self.max_connections);
            self.max_content_length = c
                .extract_u32("max_content_length")?
                .or(self.max_content_length);

            // port arrives as a number, or as a string when the config
            // was Tera-rendered (template values are strings).
            if let Some(val) = c.get("port") {
                if let Some(n) = val.as_u64() {
                    self.port = Some(n as u16);
                } else if let Some(s) = val.as_str() {
                    self.port = Some(s.parse::<u16>().map_err(|e| {
                        UnitError::config_parse_error(format!("Could not parse field port: {e}"))
                    })?);
                }
            }
            self.uri = c.extract_string("uri")?.or(self.uri.clone());
            self.api_key = c.extract_string("api_key")?.or(self.api_key.clone());
            self.remote_shutdown_enabled = c
                .extract_bool("remote_shutdown_enabled")?
                .or(self.remote_shutdown_enabled);
            Ok(())
        }) {
            return Err(err);
        }
        Ok(())
    }
}

/// Per-server state shared by the accept loop and per-connection handlers.
struct HttpEndpointState {
    node_id: String,
    ctx: Arc<TaskContext>,
    /// Ingest path — requests to other paths get 404.
    uri: String,
    /// Optional `x-api-key` header check.
    api_key: Option<String>,
    /// Max body size; larger bodies get 413.
    max_content_length: usize,
    /// Remote shutdown: `{uri}/shutdown` fires the oneshot (taken once).
    /// `None` = shutdown endpoint disabled.
    shutdown: Option<Arc<Mutex<Option<oneshot::Sender<()>>>>>,
}

impl SourceUnit for HttpEndpointUnitTask {
    fn launch(
        &self,
        ctx: Arc<TaskContext>,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send> {
        let node_id = self.meta.get_id();
        let uri = self
            .uri
            .clone()
            .unwrap_or_else(|| format!("/source/{node_id}"));
        let uri = uri.trim_end_matches('/').to_string();
        let port = self.port.unwrap_or(0);
        let max_connections = self.max_connections.unwrap_or(3) as usize;
        let max_content_length = self.max_content_length.unwrap_or(u32::MAX) as usize;
        let api_key = self.api_key.clone();

        // Remote shutdown: the accept loop breaks when the oneshot fires.
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
        let shutdown = if self.remote_shutdown_enabled.unwrap_or(false) {
            Some(Arc::new(Mutex::new(Some(shutdown_tx))))
        } else {
            None
        };

        let state = Arc::new(HttpEndpointState {
            node_id: node_id.clone(),
            ctx,
            uri: uri.clone(),
            api_key,
            max_content_length,
            shutdown,
        });

        Ok(async move {
            let listener = TcpListener::bind(("0.0.0.0", port))
                .await
                .map_err(|e| UnitError::unknown(format!("bind 0.0.0.0:{port}: {e}")))?;
            let actual = listener
                .local_addr()
                .map_err(|e| UnitError::unknown(format!("local_addr: {e}")))?;
            log::info!(
                "HttpEndpointUnitTask `{node_id}` listening on {actual}, ingest uri `{uri}`"
            );
            if state.shutdown.is_none() {
                log::warn!(
                    "HttpEndpointUnitTask `{node_id}`: remote_shutdown_enabled=false — \
                     the graph never completes (no cancellation in the engine)"
                );
            }

            let semaphore = Arc::new(Semaphore::new(max_connections));
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    accepted = listener.accept() => {
                        let (stream, peer) = match accepted {
                            Ok(accepted) => accepted,
                            Err(e) => {
                                log::warn!("HttpEndpointUnitTask `{node_id}` accept error: {e}");
                                continue;
                            }
                        };
                        match semaphore.clone().try_acquire_owned() {
                            Ok(permit) => {
                                let io = TokioIo::new(stream);
                                let handler_state = state.clone();
                                let svc = service_fn(move |req: Request<Incoming>| {
                                    let st = handler_state.clone();
                                    async move { handle_request(st, req).await }
                                });
                                tokio::spawn(async move {
                                    let _ = http1::Builder::new()
                                        .serve_connection(io, svc)
                                        .await;
                                    drop(permit);
                                });
                            }
                            Err(_) => {
                                // At capacity — the stream closes on drop.
                                log::warn!(
                                    "HttpEndpointUnitTask `{node_id}`: connection from \
                                     {peer} rejected (max_connections={max_connections})"
                                );
                            }
                        }
                    }
                }
            }
            Ok(())
        })
    }
}

/// Handle one HTTP request: produce a frame downstream (or a control
/// response for the shutdown / auth / size paths).
async fn handle_request(
    state: Arc<HttpEndpointState>,
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let path = req.uri().path().to_string();

    // Remote shutdown endpoint: `{uri}/shutdown` (when enabled).
    if let Some(shutdown) = &state.shutdown {
        if path == format!("{}/shutdown", state.uri) {
            if let Some(tx) = shutdown.lock().unwrap().take() {
                let _ = tx.send(());
            }
            return Ok(response(StatusCode::OK, "ok"));
        }
    }

    // Only the configured ingest path produces rows.
    if path != state.uri {
        return Ok(response(StatusCode::NOT_FOUND, "not found"));
    }

    // Optional api_key check.
    if let Some(key) = &state.api_key {
        let provided = req
            .headers()
            .get("x-api-key")
            .and_then(|v| v.to_str().ok());
        if provided != Some(key.as_str()) {
            return Ok(response(StatusCode::UNAUTHORIZED, "unauthorized"));
        }
    }

    // Content-Type determines the frame column type — read before the
    // body is moved out of the request.
    let content_type = req
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Read the whole body, enforcing max_content_length while draining.
    let mut body = req.into_body();
    let mut buf: Vec<u8> = Vec::new();
    loop {
        match body.frame().await {
            Some(Ok(frame)) => {
                if let Some(data) = frame.data_ref() {
                    buf.extend_from_slice(data);
                    if buf.len() > state.max_content_length {
                        return Ok(response(
                            StatusCode::PAYLOAD_TOO_LARGE,
                            "body too large",
                        ));
                    }
                }
            }
            Some(Err(e)) => {
                log::warn!(
                    "HttpEndpointUnitTask `{}`: body read failed (max {}): {e}",
                    state.node_id,
                    state.max_content_length
                );
                return Ok(response(StatusCode::PAYLOAD_TOO_LARGE, "body too large"));
            }
            None => break,
        }
    }
    let body = buf;

    // Empty body → no row (keeps probes out of the stream).
    if body.is_empty() {
        return Ok(response(StatusCode::NO_CONTENT, ""));
    }

    let frame = frame_from_parts(content_type.as_deref(), &body);
    state.ctx.send(frame).await;

    Ok(response(StatusCode::ACCEPTED, "ok"))
}

fn response(status: StatusCode, body: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap()
}

/// Build a single-column frame from an HTTP body.
///
/// JSON content types (`application/json`, any `*+json`) map to
/// `DataType::json` with the raw text in `str_val`; everything else is
/// `DataType::str`. Non-UTF-8 bodies are converted lossily.
fn frame_from_parts(content_type: Option<&str>, body: &[u8]) -> Frame {
    let is_json = content_type
        .is_some_and(|ct| ct.contains("application/json") || ct.ends_with("+json"));
    let mut column = Column::new();
    column.field = "body".into();
    column.str_val = String::from_utf8_lossy(body).into_owned();
    column.dt = EnumOrUnknown::new(if is_json {
        DataType::json
    } else {
        DataType::str
    });
    let mut frame = Frame::default();
    frame.columns.push(column);
    frame
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dt(frame: &Frame) -> DataType {
        frame.columns[0].dt.unwrap()
    }

    #[test]
    fn test_json_content_types() {
        for ct in [
            "application/json",
            "application/json; charset=utf-8",
            "application/ld+json",
        ] {
            let frame = frame_from_parts(Some(ct), br#"{"a":1}"#);
            assert_eq!(dt(&frame), DataType::json, "content-type: {ct}");
            assert_eq!(frame.columns[0].str_val, r#"{"a":1}"#);
        }
    }

    #[test]
    fn test_plain_text_content_type() {
        let frame = frame_from_parts(Some("text/plain"), b"hello world");
        assert_eq!(dt(&frame), DataType::str);
        assert_eq!(frame.columns[0].str_val, "hello world");
    }

    #[test]
    fn test_missing_content_type_defaults_to_text() {
        let frame = frame_from_parts(None, b"raw");
        assert_eq!(dt(&frame), DataType::str);
    }

    #[test]
    fn test_invalid_utf8_is_lossy() {
        let frame = frame_from_parts(None, &[0xff, 0xfe, b'a']);
        assert_eq!(dt(&frame), DataType::str);
        assert_eq!(frame.columns[0].str_val, "\u{FFFD}\u{FFFD}a");
    }
}
