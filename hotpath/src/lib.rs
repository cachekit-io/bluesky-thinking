//! Skyline hot-path Worker (LAB-746) — the Rust-WASM leg of the edge.
//!
//! Pure compute lives in [`compute`] (target-independent, natively tested);
//! everything below the cfg line is the Cloudflare Workers HTTP surface that
//! Stage 3 binds into the TS serving path.
//!
//! Endpoints:
//! - `GET  /` — service info + locked contract summary
//! - `GET  /v1/key/:operation/:window` — interop/v1 key derivation
//! - `POST /v1/verify[?expected=<16-hex>]` — xxHash3-64 + interop validity of the raw body
//! - `POST /v1/merge` — merge count-map window slices (JSON: `{slices: [base64…], top}`)
//! - `GET  /v1/cache/:operation/:window` — derive key, fetch via CachekitIO,
//!   verify + decode (503 until the Stage-3 `CACHEKIT_API_KEY` secret exists)

pub mod compute;

#[cfg(target_arch = "wasm32")]
mod edge {
    use base64::Engine as _;
    use serde::Deserialize;
    use serde_json::json;
    use worker::*;

    use crate::compute;

    #[event(fetch)]
    async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
        Router::new()
            .get("/", |_, _| info())
            .get("/v1/key/:operation/:window", |_, ctx| {
                key_handler(&ctx)
            })
            .post_async("/v1/verify", |req, _| async move { verify_handler(req).await })
            .post_async("/v1/merge", |req, _| async move { merge_handler(req).await })
            .get_async("/v1/cache/:operation/:window", |_, ctx| async move {
                cache_handler(&ctx).await
            })
            .run(req, env)
            .await
    }

    fn info() -> Result<Response> {
        Response::from_json(&json!({
            "component": "skyline-hotpath",
            "version": env!("CARGO_PKG_VERSION"),
            "sdk": "cachekit-rs 0.5.0 (crates.io, wasm32-unknown-unknown)",
            "namespace": compute::NAMESPACE,
            "operations": compute::OPERATIONS,
            "windows": { "5m": 60, "1h": 300, "24h": 900 },
            "endpoints": [
                "GET /v1/key/:operation/:window",
                "POST /v1/verify[?expected=<16-hex>]",
                "POST /v1/merge",
                "GET /v1/cache/:operation/:window",
            ],
        }))
    }

    fn json_error(status: u16, message: &str) -> Result<Response> {
        Response::from_json(&json!({ "error": message })).map(|r| r.with_status(status))
    }

    /// Both params are guaranteed by the route pattern; the error arm is
    /// unreachable in practice but stays explicit rather than panicking.
    fn path_params(ctx: &RouteContext<()>) -> Result<(&str, &str)> {
        match (ctx.param("operation"), ctx.param("window")) {
            (Some(op), Some(window)) => Ok((op.as_str(), window.as_str())),
            _ => Err(Error::RustError("route params missing".into())),
        }
    }

    fn key_handler(ctx: &RouteContext<()>) -> Result<Response> {
        let (operation, window) = path_params(ctx)?;
        match compute::derive_key(operation, window) {
            Ok(key) => Response::from_json(&json!({
                "key": key,
                "namespace": compute::NAMESPACE,
                "operation": operation,
                "window": window,
                "ttl_seconds": compute::window_ttl_seconds(window),
            })),
            Err(e) => json_error(400, &e),
        }
    }

    async fn verify_handler(mut req: Request) -> Result<Response> {
        let expected = match req.url()?.query_pairs().find(|(k, _)| k == "expected") {
            Some((_, v)) => match compute::parse_checksum_hex(&v) {
                Ok(bytes) => Some(bytes),
                Err(e) => return json_error(400, &e),
            },
            None => None,
        };
        let body = req.bytes().await?;
        let report = compute::verify_payload(&body, expected);
        Response::from_json(&json!({
            "size_bytes": report.size_bytes,
            "xxh3_64": report.xxh3_64,
            "valid_interop_value": report.valid_interop_value,
            "interop_error": report.interop_error,
            "matches_expected": report.matches_expected,
        }))
    }

    #[derive(Deserialize)]
    struct MergeRequest {
        /// Base64-encoded interop/v1 MessagePack count-map documents.
        slices: Vec<String>,
        #[serde(default = "default_top")]
        top: usize,
    }

    fn default_top() -> usize {
        50
    }

    async fn merge_handler(mut req: Request) -> Result<Response> {
        let body: MergeRequest = match req.json().await {
            Ok(b) => b,
            Err(e) => return json_error(400, &format!("invalid merge request: {e}")),
        };
        let mut slices = Vec::with_capacity(body.slices.len());
        for (i, encoded) in body.slices.iter().enumerate() {
            match base64::engine::general_purpose::STANDARD.decode(encoded) {
                Ok(bytes) => slices.push(bytes),
                Err(e) => return json_error(400, &format!("slice {i}: invalid base64: {e}")),
            }
        }
        match compute::merge_count_slices(&slices, body.top) {
            Ok(result) => Response::from_json(&json!({
                "top": result.top,
                "canonical_msgpack_base64":
                    base64::engine::general_purpose::STANDARD.encode(&result.canonical_msgpack),
                "total_keys": result.total_keys,
                "slice_count": result.slice_count,
            })),
            Err(e) => json_error(400, &e),
        }
    }

    /// Raw CachekitIO GET over `worker::Fetch`.
    ///
    /// LAB-1079 workaround: `WorkersCachekitIO` (cachekit-rs ≤ 0.8.0) panics on
    /// every request — its `session_headers()` calls `SystemTime::now()`, which
    /// is unimplemented on wasm32-unknown-unknown. Until the SDK fix ships,
    /// the hot path does the one HTTP verb it needs directly; key derivation,
    /// strict interop/v1 decode and the xxHash3 checksum stay on cachekit-rs /
    /// cachekit-core. Swap back to `WorkersCachekitIO::builder()` (with
    /// `.api_url(...).allow_custom_host(true)` for the dev instance) once
    /// LAB-1079 is fixed and published.
    async fn backend_get(api_url: &str, api_key: &str, key: &str) -> std::result::Result<Option<Vec<u8>>, String> {
        let url = format!("{}/v1/cache/{}", api_url.trim_end_matches('/'), urlencoding::encode(key));
        let mut headers = Headers::new();
        headers
            .set("Authorization", &format!("Bearer {api_key}"))
            .map_err(|e| format!("failed to set auth header: {e}"))?;
        // SDK-class keys require an L1 status on every /v1/cache request.
        headers
            .set("X-CacheKit-L1-Status", "disabled")
            .map_err(|e| format!("failed to set header: {e}"))?;

        let mut init = RequestInit::new();
        init.with_method(Method::Get).with_headers(headers);
        let request = Request::new_with_init(&url, &init).map_err(|e| format!("failed to build request: {e}"))?;
        let mut resp = Fetch::Request(request)
            .send()
            .await
            .map_err(|e| format!("fetch failed: {e}"))?;

        match resp.status_code() {
            200 => Ok(Some(resp.bytes().await.map_err(|e| format!("failed to read body: {e}"))?)),
            404 => Ok(None),
            // Never echo the response body: it is not ours and error bodies
            // can carry request context. Status code only.
            status => Err(format!("backend returned HTTP {status}")),
        }
    }

    async fn cache_handler(ctx: &RouteContext<()>) -> Result<Response> {
        let (operation, window) = path_params(ctx)?;
        let key = match compute::derive_key(operation, window) {
            Ok(key) => key,
            Err(e) => return json_error(400, &e),
        };
        let Ok(api_key) = ctx.secret("CACHEKIT_API_KEY") else {
            return json_error(
                503,
                "CACHEKIT_API_KEY secret not configured — see docs/architecture.md#credentials",
            );
        };
        let api_url = ctx
            .var("CACHEKIT_API_URL")
            .map(|v| v.to_string())
            .unwrap_or_else(|_| "https://api.cachekit.io".to_string());
        let bytes = match backend_get(&api_url, &api_key.to_string(), &key).await {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                return Response::from_json(&json!({ "key": key, "found": false }))
                    .map(|r| r.with_status(404));
            }
            Err(e) => return json_error(502, &format!("CachekitIO error: {e}")),
        };
        let report = compute::verify_payload(&bytes, None);
        let value = cachekit::interop::deserialize::<serde_json::Value>(&bytes).ok();
        Response::from_json(&json!({
            "key": key,
            "found": true,
            "size_bytes": report.size_bytes,
            "xxh3_64": report.xxh3_64,
            "valid_interop_value": report.valid_interop_value,
            "interop_error": report.interop_error,
            "value": value,
        }))
    }
}
