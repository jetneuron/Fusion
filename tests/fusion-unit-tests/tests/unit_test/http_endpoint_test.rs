//! End-to-end test for `HttpEndpointUnitTask` (HTTP ingest source).
//!
//! Starts the graph with a Tera-templated ephemeral port, POSTs one
//! text and one JSON body, then shuts the server down via the remote
//! shutdown endpoint — the graph completes only when the source's
//! `launch()` future returns.

use crate::execute_with_env;
use anyhow::anyhow;
use serde_json::json;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

/// Reserve a free port: bind ephemeral, read the port, drop the listener.
async fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Minimal HTTP/1.1 request over a raw TcpStream (zero extra deps).
/// Returns the full response text; the server closes after `Connection: close`.
async fn http_request(
    addr: &str,
    method: &str,
    path: &str,
    content_type: Option<&str>,
    body: &str,
) -> anyhow::Result<String> {
    let mut stream = TcpStream::connect(addr).await?;
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    if let Some(ct) = content_type {
        req.push_str(&format!("Content-Type: {ct}\r\n"));
    }
    req.push_str(&format!("Content-Length: {}\r\n\r\n{body}", body.len()));
    stream.write_all(req.as_bytes()).await?;
    let mut resp = String::new();
    stream.read_to_string(&mut resp).await?;
    Ok(resp)
}

#[tokio::test]
async fn test_http_endpoint_source() -> anyhow::Result<()> {
    let port = free_port().await;
    let addr = format!("127.0.0.1:{port}");

    // Fresh output file for data assertions.
    let out_path = format!(
        "{}/tests/output/http_endpoint_out.log",
        env!("CARGO_MANIFEST_DIR")
    );
    let _ = std::fs::remove_file(&out_path);

    // The graph never completes by itself — the server must be shut
    // down remotely. Run it in a task so we can drive the HTTP side.
    let graph_task = tokio::spawn(async move {
        execute_with_env("http_endpoint_source.yaml", Some(json!({"port": port.to_string()})))
            .await
    });

    // Wait for the server to come up (bounded).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match TcpStream::connect(&addr).await {
            Ok(_) => break,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => {
                return Err(anyhow!(
                    "server never came up on {addr}: {e} (graph task: {:?})",
                    graph_task.await
                ));
            }
        }
    }

    // Text body → 202, JSON body → 202, wrong path → 404.
    let resp = http_request(&addr, "POST", "/source/http", Some("text/plain"), "hello stream").await?;
    assert!(resp.starts_with("HTTP/1.1 202"), "text response: {resp}");

    let resp = http_request(
        &addr,
        "POST",
        "/source/http",
        Some("application/json"),
        r#"{"a":1,"b":"x"}"#,
    )
    .await?;
    assert!(resp.starts_with("HTTP/1.1 202"), "json response: {resp}");

    let resp = http_request(&addr, "POST", "/other", None, "").await?;
    assert!(resp.starts_with("HTTP/1.1 404"), "404 response: {resp}");

    // Remote shutdown → launch() returns → the graph completes.
    let resp = http_request(&addr, "GET", "/source/http/shutdown", None, "").await?;
    assert!(resp.starts_with("HTTP/1.1 200"), "shutdown response: {resp}");

    timeout(Duration::from_secs(10), graph_task)
        .await
        .map_err(|_| anyhow!("graph did not finish within 10s"))?
        .map_err(|e| anyhow!("graph task panicked: {e}"))??;

    // Both bodies must have reached the file sink (one row per frame).
    let content = std::fs::read_to_string(&out_path)?;
    assert!(
        content.contains("hello stream"),
        "text body missing from output: {content:?}"
    );
    assert!(
        content.contains(r#"{"a":1,"b":"x"}"#),
        "json body missing from output: {content:?}"
    );

    Ok(())
}

/// Manual test: starts the graph on a fixed port (18080) and keeps it
/// running until the remote shutdown endpoint is hit. Drive it with an
/// external HTTP client (Postman, curl, …):
///
/// ```text
/// POST http://127.0.0.1:18080/source/http          # body → DebugOutput row
/// GET  http://127.0.0.1:18080/source/http/shutdown # ends the graph, test completes
/// ```
///
/// Run with: `cargo test -p fusion-unit-tests --test plugin_base_test
/// http_endpoint_manual -- --ignored`
#[tokio::test]
#[ignore = "manual — needs an external HTTP client (e.g. Postman); shut down via /shutdown"]
async fn test_http_endpoint_manual() -> anyhow::Result<()> {
    // The graph completes only when /source/http/shutdown is hit — a
    // bind failure (port 18080 already in use) surfaces here as Err.
    crate::execute("http_endpoint_manual.yaml").await?;
    Ok(())
}
