//! LAB-735 spike: hello-world Worker proving cachekit-rs on wasm32 / CF Workers.
//!
//! Two modes, so the deploy proves the target even before CachekitIO
//! credentials exist:
//! - No CACHEKIT_API_KEY secret: compute the interop/v1 key on the edge and
//!   return it — must byte-match cachekit-py's key for the same
//!   namespace/operation/args (cross-SDK keygen proof).
//! - Secret present: full set/get round-trip against api.cachekit.io.
use cachekit::interop::{interop_key, InteropValue};
use worker::*;

#[event(fetch)]
async fn fetch(_req: Request, env: Env, _ctx: Context) -> Result<Response> {
    // interop/v1 key — byte-identical to what cachekit-py/-ts derive for the
    // same namespace/operation/args.
    let key = interop_key(
        "bluesky-thinking",
        "posts_per_minute",
        &[InteropValue::Str("5m".into())],
    )
    .map_err(|e| Error::RustError(e.to_string()))?;

    let Ok(api_key) = env.secret("CACHEKIT_API_KEY") else {
        return Response::ok(format!(
            "LAB-735 spike (no CACHEKIT_API_KEY yet)\nedge-computed interop key: {key}\n"
        ));
    };

    let backend = cachekit::backend::workers::WorkersCachekitIO::builder()
        .api_key(api_key.to_string())
        .build()
        .map_err(|e| Error::RustError(e.to_string()))?;
    let shared: cachekit::client::SharedBackend = std::rc::Rc::new(backend);
    let client = cachekit::client::CacheKit::builder()
        .backend(shared)
        .build()
        .map_err(|e| Error::RustError(e.to_string()))?;

    client
        .set(&key, &42u64)
        .await
        .map_err(|e| Error::RustError(e.to_string()))?;
    let got: Option<u64> = client
        .get(&key)
        .await
        .map_err(|e| Error::RustError(e.to_string()))?;

    Response::ok(format!("LAB-735 spike\nkey={key}\nroundtrip={got:?}\n"))
}
