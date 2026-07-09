// TCP/HTTP/WebSocket transport, ported from server.cpp + session.cpp.
//
// Semantics preserved: complete responses carry `Server: Nonolith Connect`,
// `Content-Type: application/json`, `Connection: close`; streaming CSV
// responses are chunked text/plain; `GET /` is a 301 to the marketing URL;
// the origin policy applies to plain HTTP only (SPEC.md Q9 kept — WebSocket
// upgrades bypass it, matching the C++).

use crate::device::{ClientHandle, OutMsg, ServerState};
use crate::rest::{handle_json_request, RestBody, RestRequest};
use crate::ws::WsConn;
use axum::body::Body;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Request, State, WebSocketUpgrade};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::Response;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;

const REDIR_URL: &str =
    "http://www.nonolithlabs.com/connect/?utm_source=connect&utm_medium=app&utm_campaign=server-redir";

pub async fn run(state: Arc<ServerState>, port: u16, allow_remote: bool) -> std::io::Result<()> {
    let addr = if allow_remote {
        std::net::SocketAddr::from(([0, 0, 0, 0], port))
    } else {
        std::net::SocketAddr::from(([127, 0, 0, 1], port))
    };

    let app = Router::new().fallback(handle).with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    eprintln!("Listening on {addr}");

    // Serve through hyper-util directly (not axum::serve) so we can emit
    // Title-Case headers like the Beast server did and apply the 30-second
    // initial-request read timeout from the C++ session layer.
    loop {
        let (socket, _peer) = listener.accept().await?;
        let app = app.clone();
        tokio::spawn(async move {
            let service =
                hyper::service::service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                    use tower::Service;
                    app.clone().call(req.map(Body::new))
                });
            let mut builder = hyper::server::conn::http1::Builder::new();
            builder
                .title_case_headers(true)
                .header_read_timeout(std::time::Duration::from_secs(30))
                .timer(hyper_util::rt::TokioTimer::new());
            let io = hyper_util::rt::TokioIo::new(socket);
            let _ = builder.serve_connection(io, service).with_upgrades().await;
        });
    }
}

fn origin_allowed(origin: &str) -> bool {
    if origin.is_empty() || origin == "null" {
        return true;
    }
    let rest = origin
        .strip_prefix("http://")
        .or_else(|| origin.strip_prefix("https://"));
    let Some(host) = rest else { return false };

    // ^https?://localhost(:[0-9]+)?$
    if host == "localhost" {
        return true;
    }
    if let Some(p) = host.strip_prefix("localhost:") {
        if !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()) {
            return true;
        }
    }

    // ^https?://[\w.-]*?nonolithlabs.com$
    if let Some(prefix) = host.strip_suffix("nonolithlabs.com") {
        if prefix
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.' || b == b'-')
        {
            return true;
        }
    }

    false
}

async fn handle(State(state): State<Arc<ServerState>>, req: Request) -> Response {
    let target = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| "/".to_string());
    let origin = req
        .headers()
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let method = req.method().as_str().to_string();

    // WebSocket upgrade on a first path segment of "ws"; upgrades on other
    // paths fall through and are treated as plain HTTP.
    let first_seg = target
        .trim_start_matches('/')
        .split(['/', '?'])
        .next()
        .unwrap_or("");
    let req = if first_seg == "ws" {
        use axum::extract::FromRequestParts;
        let (mut parts, body) = req.into_parts();
        match WebSocketUpgrade::from_request_parts(&mut parts, &()).await {
            Ok(ws) => {
                let state = state.clone();
                return ws.on_upgrade(move |socket| handle_socket(state, socket));
            }
            Err(_) => Request::from_parts(parts, body),
        }
    } else {
        req
    };

    // Origin policy (HTTP only)
    if !state.allow_any_origin && !origin_allowed(&origin) {
        eprintln!("Rejected client with unknown origin {origin}");
        return complete(StatusCode::FORBIDDEN, "Origin not allowed", &[]);
    }

    if first_seg.is_empty() {
        return complete(
            StatusCode::MOVED_PERMANENTLY,
            "",
            &[(header::LOCATION.as_str(), REDIR_URL)],
        );
    }

    if first_seg == "rest" {
        let body = axum::body::to_bytes(req.into_body(), 16 * 1024 * 1024)
            .await
            .map(|b| String::from_utf8_lossy(&b).into_owned())
            .unwrap_or_default();
        let rreq = RestRequest::new(&method, &target, &body);
        let resp = handle_json_request(&state, &rreq);

        // Every REST response echoes the request Origin (even when empty)
        return match resp.body {
            RestBody::Full(s) => {
                let status =
                    StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                complete(status, &s, &[("Access-Control-Allow-Origin", &origin)])
            }
            RestBody::Stream(rx) => {
                let stream = futures_util::stream::unfold(rx, |mut rx| async move {
                    rx.recv()
                        .await
                        .map(|s| (Ok::<_, std::convert::Infallible>(bytes::Bytes::from(s)), rx))
                });
                let mut r = Response::builder()
                    .status(StatusCode::OK)
                    .header(header::SERVER, "Nonolith Connect")
                    .header(header::CONTENT_TYPE, "text/plain");
                if let Ok(v) = HeaderValue::from_str(&origin) {
                    r = r.header("Access-Control-Allow-Origin", v);
                }
                r.body(Body::from_stream(stream)).unwrap()
            }
        };
    }

    complete(StatusCode::NOT_FOUND, "Not found", &[])
}

fn complete(status: StatusCode, body: &str, extra: &[(&str, &str)]) -> Response {
    let mut r = Response::builder()
        .status(status)
        .header(header::SERVER, "Nonolith Connect")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CONNECTION, "close");
    for (k, v) in extra {
        if let Ok(hv) = HeaderValue::from_str(v) {
            r = r.header(*k, hv);
        }
    }
    r.body(Body::from(body.to_string())).unwrap()
}

async fn handle_socket(state: Arc<ServerState>, socket: WebSocket) {
    let (mut sink, mut stream) = socket.split();
    let (client, mut rx) = ClientHandle::pair();
    let debug = state.debug;

    let writer = tokio::spawn(async move {
        while let Some(m) = rx.recv().await {
            let msg = match m {
                OutMsg::Json(v) => {
                    let s = v.to_string();
                    if debug {
                        println!("TXD: {s}");
                    }
                    Message::Text(s.into())
                }
                OutMsg::Binary(b) => {
                    if debug {
                        println!("TXD: <binary frame, {} bytes>", b.len());
                    }
                    Message::Binary(b.into())
                }
            };
            if sink.send(msg).await.is_err() {
                break;
            }
        }
    });

    let mut conn = WsConn::new(&state, client);
    let mut dlc = state.device_list_changed.subscribe();

    loop {
        tokio::select! {
            m = stream.next() => match m {
                Some(Ok(Message::Text(t))) => conn.on_message(&state, t.as_str()),
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(_)) => {} // binary/ping/pong from clients: ignored
                Some(Err(_)) => break,
            },
            r = dlc.recv() => {
                if r.is_ok() {
                    conn.send_device_list(&state);
                }
            }
        }
    }

    conn.on_close();
    writer.abort();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origin_policy() {
        assert!(origin_allowed(""));
        assert!(origin_allowed("null"));
        assert!(origin_allowed("http://localhost"));
        assert!(origin_allowed("http://localhost:5173"));
        assert!(origin_allowed("https://localhost:9003"));
        assert!(origin_allowed("https://www.nonolithlabs.com"));
        assert!(origin_allowed("http://nonolithlabs.com"));
        assert!(origin_allowed("http://apps.nonolithlabs.com"));
        assert!(!origin_allowed("http://evil.example.com"));
        assert!(!origin_allowed("http://localhost.evil.com"));
        assert!(!origin_allowed("http://nonolithlabs.com.evil.com"));
        assert!(!origin_allowed("ftp://localhost"));
        assert!(!origin_allowed("http://localhost:abc"));
    }
}
