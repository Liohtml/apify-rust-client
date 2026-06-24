//! # apify-rust-client
//!
//! Async Rust client for the [Apify Cloud API](https://docs.apify.com/api/v2).
//! Run any Apify actor, poll its status, download dataset items — generic
//! over the dataset item type.
//!
//! ## Features
//!
//! - 🚀 Submit a run with arbitrary JSON input (or any `Serialize` type)
//! - ⏳ Poll the run status with configurable interval + timeout
//! - 📦 Download dataset items in pages, deserialized into your own struct,
//!   with an optional cap to bound memory use
//! - 🔁 **Multi-key fallback** — supply several Apify tokens; if one runs out
//!   of credit / fails at submit time, the next one is tried automatically
//! - ♻️ Automatic retry with exponential backoff for transient errors
//!   (`429`, `5xx`, network blips)
//! - 🔐 Tokens sent via the `Authorization` header, never in the URL
//! - 🪵 `tracing` integration for structured logging
//!
//! ## Quick start
//!
//! ```no_run
//! use apify_rust_client::{ApifyClient, RunInput};
//! use serde::Deserialize;
//! use serde_json::json;
//!
//! #[derive(Debug, Deserialize)]
//! struct Place {
//!     title: String,
//!     website: Option<String>,
//! }
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let client = ApifyClient::new([std::env::var("APIFY_API_KEY")?]);
//!
//! let input = json!({
//!     "searchStringsArray": ["coffee shop Berlin"],
//!     "maxCrawledPlacesPerSearch": 20,
//! });
//!
//! let places: Vec<Place> = client
//!     .run_actor("compass~crawler-google-places", RunInput::new(input))
//!     .await?
//!     .wait_for_dataset()
//!     .await?;
//!
//! for p in places {
//!     println!("{} – {:?}", p.title, p.website);
//! }
//! # Ok(()) }
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::fmt;
use std::time::{Duration, Instant};

use reqwest::{Client, RequestBuilder, StatusCode};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tracing::{info, warn};

const DEFAULT_API_BASE: &str = "https://api.apify.com/v2";
/// Maximum number of items requested per dataset page.
const MAX_PAGE: usize = 1000;

/// All errors this crate produces.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// All provided API keys failed.
    #[error("all keys failed; last error: {0}")]
    AllKeysFailed(String),

    /// HTTP transport error.
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    /// JSON serialization or deserialization error.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// Apify returned an HTTP error response.
    #[error("Apify HTTP {status}: {body}")]
    ApiStatus {
        /// The HTTP status code.
        status: u16,
        /// The response body (truncated).
        body: String,
    },

    /// The actor run terminated with a non-success status.
    #[error("actor run finished with status {0}")]
    RunFailed(String),

    /// Polling for run completion exceeded the timeout.
    #[error("actor run timed out after {0:?}")]
    Timeout(Duration),

    /// The supplied `actor_id` contains characters that are not allowed.
    #[error("invalid actor id: {0:?}")]
    InvalidActorId(String),

    /// No API keys were provided.
    #[error("no API keys provided")]
    NoKeys,
}

/// Result type used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Wrapper for the JSON input passed to an actor.
///
/// Shape and required fields depend entirely on the target actor.
/// See its README on [apify.com/store](https://apify.com/store).
#[derive(Debug, Clone)]
pub struct RunInput(pub serde_json::Value);

impl RunInput {
    /// Build a `RunInput` from a [`serde_json::Value`].
    pub fn new(v: serde_json::Value) -> Self {
        Self(v)
    }

    /// Build a `RunInput` from any [`Serialize`] type, e.g. a typed input struct.
    ///
    /// ```
    /// # use apify_rust_client::RunInput;
    /// #[derive(serde::Serialize)]
    /// struct In { query: String, max_items: u32 }
    /// let input = RunInput::from_serialize(In { query: "rust".into(), max_items: 10 }).unwrap();
    /// ```
    pub fn from_serialize<T: Serialize>(value: T) -> Result<Self> {
        Ok(Self(serde_json::to_value(value)?))
    }
}

/// The Apify client. Holds the HTTP client + ordered list of tokens.
#[derive(Clone)]
pub struct ApifyClient {
    http: Client,
    api_keys: Vec<String>,
    api_base: String,
    poll_interval: Duration,
    max_wait: Duration,
    max_retries: u32,
    max_items: Option<usize>,
}

// Manual `Debug` impl: the auto-derived one would print the API tokens in
// plaintext anywhere the client is formatted with `{:?}` (tracing fields,
// `dbg!`, panic messages). Redact them.
impl fmt::Debug for ApifyClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApifyClient")
            .field(
                "api_keys",
                &format_args!("[{} key(s) redacted]", self.api_keys.len()),
            )
            .field("api_base", &self.api_base)
            .field("poll_interval", &self.poll_interval)
            .field("max_wait", &self.max_wait)
            .field("max_retries", &self.max_retries)
            .field("max_items", &self.max_items)
            .finish()
    }
}

impl ApifyClient {
    /// Construct a client with one or more API tokens.
    /// Tokens are tried in order; the first that succeeds is used.
    ///
    /// # Panics
    /// Panics if the underlying HTTP client cannot be built (e.g. TLS backend
    /// initialization failure). Use [`ApifyClient::try_new`] to handle that
    /// case gracefully.
    pub fn new<I, S>(api_keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::try_new(api_keys).expect("failed to build reqwest client")
    }

    /// Fallible constructor: returns [`Error::Http`] instead of panicking if
    /// the underlying HTTP client cannot be built.
    pub fn try_new<I, S>(api_keys: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Ok(Self {
            http: Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .map_err(Error::Http)?,
            api_keys: api_keys.into_iter().map(Into::into).collect(),
            api_base: DEFAULT_API_BASE.to_string(),
            poll_interval: Duration::from_secs(20),
            max_wait: Duration::from_secs(60 * 60),
            max_retries: 3,
            max_items: None,
        })
    }

    /// Override the polling interval (default: 20 s).
    pub fn poll_interval(mut self, d: Duration) -> Self {
        self.poll_interval = d;
        self
    }

    /// Override the maximum wait for a run to complete (default: 1 h).
    pub fn max_wait(mut self, d: Duration) -> Self {
        self.max_wait = d;
        self
    }

    /// Number of retry attempts for transient errors — `429`, `5xx` and
    /// network errors (default: 3). Set to 0 to disable retries.
    pub fn max_retries(mut self, n: u32) -> Self {
        self.max_retries = n;
        self
    }

    /// Cap the total number of dataset items downloaded by
    /// [`RunHandle::fetch_dataset_items`] / [`RunHandle::wait_for_dataset`].
    ///
    /// Without a cap, the entire dataset is buffered in memory, which can
    /// OOM the process on very large datasets. Default: no cap.
    pub fn max_items(mut self, n: usize) -> Self {
        self.max_items = Some(n);
        self
    }

    /// Override the API base URL (rarely needed; for testing).
    ///
    /// A trailing slash is stripped. For security, non-HTTPS bases are
    /// rejected (and ignored, keeping the previous value) unless they point
    /// at `localhost`/`127.0.0.1`, since the `Authorization` header would
    /// otherwise be transmitted in plaintext.
    pub fn api_base(mut self, base: impl Into<String>) -> Self {
        let base = base.into();
        let trimmed = base.trim_end_matches('/').to_string();
        if is_secure_base(&trimmed) {
            self.api_base = trimmed;
        } else {
            warn!(
                rejected = %trimmed,
                kept = %self.api_base,
                "api_base is not HTTPS (and not localhost); ignoring it"
            );
        }
        self
    }

    /// Start an actor run. Returns a `RunHandle` which you then `.wait_for_dataset()` on.
    ///
    /// `actor_id` is the slug like `"compass~crawler-google-places"`
    /// or the numeric `actId`.
    pub async fn run_actor(&self, actor_id: &str, input: RunInput) -> Result<RunHandle<'_>> {
        if self.api_keys.is_empty() {
            return Err(Error::NoKeys);
        }
        validate_actor_id(actor_id)?;
        let mut last_err: Option<String> = None;
        for (i, key) in self.api_keys.iter().enumerate() {
            info!(key_index = i, actor_id, "submitting run");
            match self.submit_run(actor_id, &input.0, key).await {
                Ok(r) => {
                    info!(run_id = %r.run_id, dataset_id = %r.dataset_id, "run started");
                    return Ok(RunHandle {
                        client: self,
                        api_key: key.clone(),
                        run_id: r.run_id,
                        dataset_id: r.dataset_id,
                    });
                }
                // Only rotate to the next key when the failure is plausibly
                // key-related. A 404/400 means a bad actor id or input — the
                // same request will fail identically for every key, so surface
                // it immediately instead of masking it as `AllKeysFailed`.
                Err(e) if should_rotate_key(&e) => {
                    warn!(key_index = i, "submit failed (key issue): {e}");
                    last_err = Some(e.to_string());
                }
                Err(e) => return Err(e),
            }
        }
        Err(Error::AllKeysFailed(
            last_err.unwrap_or_else(|| "unknown".into()),
        ))
    }

    async fn submit_run(
        &self,
        actor_id: &str,
        input: &serde_json::Value,
        api_key: &str,
    ) -> Result<RunInfo> {
        let url = format!("{}/acts/{}/runs", self.api_base, actor_id);
        let resp = self
            .send_with_retry(|| self.http.post(&url).bearer_auth(api_key).json(input))
            .await?;
        let body = resp.text().await?;
        let parsed: ApiResp<RunData> = serde_json::from_str(&body)?;
        Ok(RunInfo {
            run_id: parsed.data.id,
            dataset_id: parsed.data.default_dataset_id,
        })
    }

    /// Send a request, retrying transient failures with exponential backoff.
    ///
    /// `make` is called once per attempt so the request (and its body) can be
    /// rebuilt. On a successful (2xx) response the [`reqwest::Response`] is
    /// returned. Non-transient HTTP errors return [`Error::ApiStatus`]
    /// immediately without retrying.
    async fn send_with_retry<F>(&self, make: F) -> Result<reqwest::Response>
    where
        F: Fn() -> RequestBuilder,
    {
        let mut attempt: u32 = 0;
        loop {
            match make().send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        return Ok(resp);
                    }
                    if is_transient_status(status) && attempt < self.max_retries {
                        warn!(%status, attempt, "transient HTTP error; retrying");
                        backoff(attempt).await;
                        attempt += 1;
                        continue;
                    }
                    let body = resp.text().await.unwrap_or_default();
                    // Here `unwrap_or_default` is intentional: we already know
                    // the request failed with `status`; a body-read error must
                    // not shadow that more useful status code.
                    return Err(Error::ApiStatus {
                        status: status.as_u16(),
                        body: body.chars().take(400).collect(),
                    });
                }
                Err(e) => {
                    if attempt < self.max_retries {
                        warn!(error = %e, attempt, "transient network error; retrying");
                        backoff(attempt).await;
                        attempt += 1;
                        continue;
                    }
                    return Err(e.into());
                }
            }
        }
    }
}

/// Handle to a started actor run. Use it to poll for completion and
/// fetch the dataset.
pub struct RunHandle<'c> {
    client: &'c ApifyClient,
    api_key: String,
    /// The run ID assigned by Apify.
    pub run_id: String,
    /// The default dataset ID of the run.
    pub dataset_id: String,
}

// Manual `Debug` that omits the secret `api_key` (and the noisy client ref).
impl fmt::Debug for RunHandle<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunHandle")
            .field("run_id", &self.run_id)
            .field("dataset_id", &self.dataset_id)
            .finish_non_exhaustive()
    }
}

impl RunHandle<'_> {
    /// Poll until the actor run is `SUCCEEDED`, then download all dataset
    /// items (subject to the client's `max_items` cap, if any), deserialized
    /// into `Vec<T>`.
    pub async fn wait_for_dataset<T: DeserializeOwned>(&self) -> Result<Vec<T>> {
        self.wait_for_status().await?;
        self.fetch_dataset_items().await
    }

    /// Poll until the actor run reaches a terminal status.
    pub async fn wait_for_status(&self) -> Result<()> {
        let started = Instant::now();
        let url = format!("{}/actor-runs/{}", self.client.api_base, self.run_id);
        loop {
            // Poll first, then sleep: a run that finished quickly is detected
            // immediately instead of always paying one `poll_interval`.
            let resp = self
                .client
                .send_with_retry(|| self.client.http.get(&url).bearer_auth(&self.api_key))
                .await?;
            let body = resp.text().await?;
            let parsed: ApiResp<StatusData> = serde_json::from_str(&body)?;
            info!(run_id = %self.run_id, status = %parsed.data.status, "run status");
            match parsed.data.status.as_str() {
                "SUCCEEDED" => return Ok(()),
                "FAILED" | "ABORTED" | "TIMED-OUT" | "TIMED_OUT" => {
                    return Err(Error::RunFailed(parsed.data.status));
                }
                _ => {}
            }
            // Not terminal yet: check the deadline, then sleep (never past it).
            let remaining = match self.client.max_wait.checked_sub(started.elapsed()) {
                Some(r) if !r.is_zero() => r,
                _ => return Err(Error::Timeout(self.client.max_wait)),
            };
            tokio::time::sleep(remaining.min(self.client.poll_interval)).await;
        }
    }

    /// Fetch dataset items in pages, deserialized into `T`.
    ///
    /// Downloads the whole dataset unless the client was configured with
    /// [`ApifyClient::max_items`], in which case at most that many items are
    /// returned.
    pub async fn fetch_dataset_items<T: DeserializeOwned>(&self) -> Result<Vec<T>> {
        let started = Instant::now();
        let mut out: Vec<T> = Vec::new();
        let mut offset: usize = 0;
        loop {
            // Bound the download phase by `max_wait` so a huge dataset cannot
            // loop indefinitely. Note this is a per-phase budget: a full
            // `wait_for_dataset` (poll + download) may take up to ~2×max_wait.
            if started.elapsed() >= self.client.max_wait {
                return Err(Error::Timeout(self.client.max_wait));
            }
            let remaining = match self.client.max_items {
                Some(cap) => cap.saturating_sub(out.len()),
                None => usize::MAX,
            };
            if remaining == 0 {
                break;
            }
            let page = remaining.min(MAX_PAGE);
            let url = format!(
                "{}/datasets/{}/items?format=json&clean=true&limit={}&offset={}",
                self.client.api_base, self.dataset_id, page, offset
            );
            let resp = self
                .client
                .send_with_retry(|| self.client.http.get(&url).bearer_auth(&self.api_key))
                .await?;
            let chunk: Vec<T> = resp.json().await?;
            let n = chunk.len();
            out.extend(chunk);
            info!(offset, batch = n, total_so_far = out.len(), "dataset chunk");
            if n < page {
                break;
            }
            offset += n;
        }
        Ok(out)
    }
}

// ───────── helpers ─────────

/// Validate an actor id / slug, rejecting values that could escape the
/// intended URL path (e.g. `../`).
fn validate_actor_id(id: &str) -> Result<()> {
    let ok = !id.is_empty()
        && !id.contains("..")
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '~' | '_' | '-' | '/' | '.'));
    if ok {
        Ok(())
    } else {
        Err(Error::InvalidActorId(id.to_string()))
    }
}

/// Whether a base URL is safe to transmit credentials over: HTTPS, or a
/// local loopback address (allowed for testing).
fn is_secure_base(base: &str) -> bool {
    if base.starts_with("https://") {
        return true;
    }
    // Allow loopback over plain HTTP, but only when the host is *exactly*
    // localhost/127.0.0.1 — i.e. followed by a port, a path, or end-of-string.
    // This rejects look-alikes such as `http://localhost.evil.com`.
    for prefix in ["http://localhost", "http://127.0.0.1"] {
        if let Some(rest) = base.strip_prefix(prefix) {
            if rest.is_empty() || rest.starts_with(':') || rest.starts_with('/') {
                return true;
            }
        }
    }
    false
}

/// HTTP status codes that warrant a retry.
fn is_transient_status(status: StatusCode) -> bool {
    matches!(status.as_u16(), 429 | 500 | 502 | 503 | 504)
}

/// Whether a submit failure justifies trying the next API key.
///
/// Bad-request errors (wrong actor id / malformed input) are identical for
/// every key, so they surface immediately. Everything else — auth (401/403),
/// exhausted credit (402), rate limit (429), server errors (5xx), and
/// transport/parse failures — may be key- or attempt-specific, so the next
/// key is worth trying. This preserves the advertised multi-key fallback for
/// the "key ran out of credit" case while still failing fast on a typo'd
/// actor id.
fn should_rotate_key(e: &Error) -> bool {
    match e {
        Error::ApiStatus { status, .. } => !matches!(status, 400 | 404 | 405 | 422),
        _ => true,
    }
}

/// Exponential backoff: 200ms, 400ms, 800ms, … capped at 10s.
async fn backoff(attempt: u32) {
    let millis = 200u64.saturating_mul(1u64 << attempt.min(6));
    tokio::time::sleep(Duration::from_millis(millis.min(10_000))).await;
}

// ───────── internal API types ─────────

#[derive(Deserialize)]
struct ApiResp<T> {
    data: T,
}

#[derive(Deserialize)]
struct RunData {
    id: String,
    #[serde(rename = "defaultDatasetId")]
    default_dataset_id: String,
    #[allow(dead_code)]
    status: Option<String>,
}

#[derive(Deserialize)]
struct StatusData {
    status: String,
}

struct RunInfo {
    run_id: String,
    dataset_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_keys_returns_error() {
        let c = ApifyClient::new(Vec::<String>::new());
        // We can't exercise the async path in a sync test, but the client
        // can still be constructed and configured.
        assert_eq!(c.api_keys.len(), 0);
    }

    #[test]
    fn builder_chains() {
        let c = ApifyClient::new(["abc"])
            .poll_interval(Duration::from_secs(5))
            .max_wait(Duration::from_secs(900))
            .max_retries(5)
            .max_items(50)
            .api_base("https://example.com/v2");
        assert_eq!(c.api_keys, vec!["abc"]);
        assert_eq!(c.poll_interval, Duration::from_secs(5));
        assert_eq!(c.max_wait, Duration::from_secs(900));
        assert_eq!(c.max_retries, 5);
        assert_eq!(c.max_items, Some(50));
        assert_eq!(c.api_base, "https://example.com/v2");
    }

    #[test]
    fn api_base_strips_trailing_slash() {
        let c = ApifyClient::new(["k"]).api_base("https://api.apify.com/v2/");
        assert_eq!(c.api_base, "https://api.apify.com/v2");
    }

    #[test]
    fn api_base_rejects_insecure() {
        let c = ApifyClient::new(["k"]).api_base("http://evil.example.com/v2");
        // insecure base ignored, default retained
        assert_eq!(c.api_base, DEFAULT_API_BASE);
    }

    #[test]
    fn api_base_allows_localhost_http() {
        let c = ApifyClient::new(["k"]).api_base("http://localhost:8080/v2");
        assert_eq!(c.api_base, "http://localhost:8080/v2");
    }

    #[test]
    fn is_secure_base_rejects_lookalike_loopback() {
        // exact loopback hosts are fine
        assert!(is_secure_base("http://localhost"));
        assert!(is_secure_base("http://localhost:8080/v2"));
        assert!(is_secure_base("http://127.0.0.1/v2"));
        assert!(is_secure_base("https://api.apify.com/v2"));
        // look-alike subdomains must NOT pass
        assert!(!is_secure_base("http://localhost.evil.com/v2"));
        assert!(!is_secure_base("http://127.0.0.1.evil.com/v2"));
        assert!(!is_secure_base("http://evil.com/v2"));
    }

    #[test]
    fn actor_id_validation() {
        assert!(validate_actor_id("compass~crawler-google-places").is_ok());
        assert!(validate_actor_id("apify/web-scraper").is_ok());
        assert!(validate_actor_id("aBc123_~-.").is_ok());
        assert!(validate_actor_id("").is_err());
        assert!(validate_actor_id("../../admin").is_err());
        assert!(validate_actor_id("foo bar").is_err());
        assert!(validate_actor_id("foo?token=x").is_err());
    }

    #[test]
    fn run_input_from_serialize() {
        #[derive(Serialize)]
        struct In {
            query: String,
            max_items: u32,
        }
        let input = RunInput::from_serialize(In {
            query: "rust".into(),
            max_items: 10,
        })
        .unwrap();
        assert_eq!(input.0["query"], "rust");
        assert_eq!(input.0["max_items"], 10);
    }

    #[test]
    fn transient_status_classification() {
        for code in [429u16, 500, 502, 503, 504] {
            assert!(is_transient_status(StatusCode::from_u16(code).unwrap()));
        }
        for code in [200u16, 400, 401, 403, 404] {
            assert!(!is_transient_status(StatusCode::from_u16(code).unwrap()));
        }
    }

    #[test]
    fn try_new_builds_client() {
        let c = ApifyClient::try_new(["k"]).expect("client builds");
        assert_eq!(c.api_keys, vec!["k"]);
    }

    #[test]
    fn should_rotate_key_only_on_non_request_errors() {
        let api = |status| Error::ApiStatus {
            status,
            body: String::new(),
        };
        // key-scoped / credit / rate-limit / server / transport → rotate
        assert!(should_rotate_key(&api(401)));
        assert!(should_rotate_key(&api(402))); // exhausted credit — headline case
        assert!(should_rotate_key(&api(403)));
        assert!(should_rotate_key(&api(429)));
        assert!(should_rotate_key(&api(500)));
        assert!(should_rotate_key(&api(503)));
        // bad-request errors → surface immediately, identical for every key
        assert!(!should_rotate_key(&api(400)));
        assert!(!should_rotate_key(&api(404)));
        assert!(!should_rotate_key(&api(405)));
        assert!(!should_rotate_key(&api(422)));
    }

    #[test]
    fn debug_redacts_api_keys() {
        let c = ApifyClient::new(["super-secret-token"]);
        let s = format!("{c:?}");
        assert!(!s.contains("super-secret-token"), "token leaked: {s}");
        assert!(s.contains("redacted"));

        let h = RunHandle {
            client: &c,
            api_key: "super-secret-token".into(),
            run_id: "run123".into(),
            dataset_id: "ds456".into(),
        };
        let s = format!("{h:?}");
        assert!(!s.contains("super-secret-token"), "token leaked: {s}");
        assert!(s.contains("run123") && s.contains("ds456"));
    }
}
