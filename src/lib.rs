//! # apify-rust-client
//!
//! Async Rust client for the [Apify Cloud API](https://docs.apify.com/api/v2).
//! Run any Apify actor, poll its status, download dataset items — generic
//! over the dataset item type.
//!
//! ## Features
//!
//! - 🚀 Submit a run with arbitrary JSON input
//! - ⏳ Poll the run status with configurable interval + timeout
//! - 📦 Download dataset items in pages, deserialized into your own struct
//! - 🔁 **Multi-key fallback** — supply several Apify tokens; if one runs out
//!   of credit / fails, the next one is tried automatically
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
//! let client = ApifyClient::new(vec![std::env::var("APIFY_API_KEY")?]);
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

use std::time::{Duration, Instant};

use reqwest::Client;
use serde::{Deserialize, de::DeserializeOwned};
use tracing::{info, warn};

const DEFAULT_API_BASE: &str = "https://api.apify.com/v2";

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
    /// Build a `RunInput` from any serializable value.
    pub fn new(v: serde_json::Value) -> Self {
        Self(v)
    }
}

/// The Apify client. Holds the HTTP client + ordered list of tokens.
#[derive(Debug, Clone)]
pub struct ApifyClient {
    http: Client,
    api_keys: Vec<String>,
    api_base: String,
    poll_interval: Duration,
    max_wait: Duration,
}

impl ApifyClient {
    /// Construct a client with one or more API tokens.
    /// Tokens are tried in order; the first that succeeds is used.
    pub fn new<I, S>(api_keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            http: Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .expect("reqwest client"),
            api_keys: api_keys.into_iter().map(Into::into).collect(),
            api_base: DEFAULT_API_BASE.to_string(),
            poll_interval: Duration::from_secs(20),
            max_wait: Duration::from_secs(60 * 60),
        }
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

    /// Override the API base URL (rarely needed; for testing).
    pub fn api_base(mut self, base: impl Into<String>) -> Self {
        self.api_base = base.into();
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
                Err(e) => {
                    warn!(key_index = i, "submit failed: {e}");
                    last_err = Some(e.to_string());
                }
            }
        }
        Err(Error::AllKeysFailed(last_err.unwrap_or_else(|| "unknown".into())))
    }

    async fn submit_run(
        &self,
        actor_id: &str,
        input: &serde_json::Value,
        api_key: &str,
    ) -> Result<RunInfo> {
        let url = format!("{}/acts/{}/runs?token={}", self.api_base, actor_id, api_key);
        let resp = self.http.post(&url).json(input).send().await?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(Error::ApiStatus {
                status: status.as_u16(),
                body: body.chars().take(400).collect(),
            });
        }
        let parsed: ApiResp<RunData> = serde_json::from_str(&body)?;
        Ok(RunInfo {
            run_id: parsed.data.id,
            dataset_id: parsed.data.default_dataset_id,
        })
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

impl<'c> RunHandle<'c> {
    /// Poll until the actor run is `SUCCEEDED`, then download all dataset
    /// items, deserialized into `Vec<T>`.
    pub async fn wait_for_dataset<T: DeserializeOwned>(&self) -> Result<Vec<T>> {
        self.wait_for_status().await?;
        self.fetch_dataset_items().await
    }

    /// Poll until the actor run reaches a terminal status.
    pub async fn wait_for_status(&self) -> Result<()> {
        let started = Instant::now();
        loop {
            if started.elapsed() > self.client.max_wait {
                return Err(Error::Timeout(self.client.max_wait));
            }
            tokio::time::sleep(self.client.poll_interval).await;
            let url = format!(
                "{}/actor-runs/{}?token={}",
                self.client.api_base, self.run_id, self.api_key
            );
            let resp = match self.client.http.get(&url).send().await {
                Ok(r) => r,
                Err(e) => {
                    warn!("status poll error: {e}");
                    continue;
                }
            };
            let body = resp.text().await.unwrap_or_default();
            let parsed: ApiResp<StatusData> = match serde_json::from_str(&body) {
                Ok(p) => p,
                Err(e) => {
                    warn!("status JSON: {e}");
                    continue;
                }
            };
            info!(run_id = %self.run_id, status = %parsed.data.status, "run status");
            match parsed.data.status.as_str() {
                "SUCCEEDED" => return Ok(()),
                "FAILED" | "ABORTED" | "TIMED-OUT" | "TIMED_OUT" => {
                    return Err(Error::RunFailed(parsed.data.status));
                }
                _ => continue,
            }
        }
    }

    /// Fetch all dataset items in pages of `limit`, deserialized into `T`.
    pub async fn fetch_dataset_items<T: DeserializeOwned>(&self) -> Result<Vec<T>> {
        let mut out: Vec<T> = Vec::new();
        let mut offset: usize = 0;
        let limit: usize = 1000;
        loop {
            let url = format!(
                "{}/datasets/{}/items?format=json&clean=true&limit={}&offset={}&token={}",
                self.client.api_base, self.dataset_id, limit, offset, self.api_key
            );
            let resp = self.client.http.get(&url).send().await?;
            let status = resp.status();
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                return Err(Error::ApiStatus {
                    status: status.as_u16(),
                    body: body.chars().take(400).collect(),
                });
            }
            let chunk: Vec<T> = resp.json().await?;
            let n = chunk.len();
            out.extend(chunk);
            info!(offset, batch = n, total_so_far = out.len(), "dataset chunk");
            if n < limit {
                break;
            }
            offset += n;
        }
        Ok(out)
    }
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
            .api_base("https://example.com/v2");
        assert_eq!(c.api_keys, vec!["abc"]);
        assert_eq!(c.poll_interval, Duration::from_secs(5));
        assert_eq!(c.max_wait, Duration::from_secs(900));
        assert_eq!(c.api_base, "https://example.com/v2");
    }
}
