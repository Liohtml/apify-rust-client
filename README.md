# apify-rust-client

[![crates.io](https://img.shields.io/crates/v/apify-rust-client.svg)](https://crates.io/crates/apify-rust-client)
[![docs.rs](https://docs.rs/apify-rust-client/badge.svg)](https://docs.rs/apify-rust-client)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

Async Rust client for the [Apify Cloud API](https://docs.apify.com/api/v2). Run any actor, poll its status, download dataset items — generic over the dataset-item type and with built-in multi-key fallback.

## Why

Apify has hundreds of pre-built scrapers ("actors") on their [store](https://apify.com/store) — Google Maps, LinkedIn, Twitter, e-commerce, you name it. Until now there was no idiomatic Rust client for the API. This crate is the missing piece.

## Features

- 🚀 **Submit** an actor run with arbitrary JSON input.
- ⏳ **Poll** the run status with configurable interval + timeout.
- 📦 **Download** dataset items in pages, deserialized straight into your own struct.
- 🔁 **Multi-key fallback** — supply several Apify tokens; if one runs out of credit, the next is tried automatically.
- 🪵 `tracing` integration for structured logging.

## Installation

```toml
[dependencies]
apify-rust-client = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
serde = { version = "1", features = ["derive"] }
```

## Quick start

```rust,no_run
use apify_rust_client::{ApifyClient, RunInput};
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
struct Place {
    title: String,
    website: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ApifyClient::new([std::env::var("APIFY_API_KEY")?]);

    let input = RunInput::new(json!({
        "searchStringsArray": ["coffee shop Berlin"],
        "maxCrawledPlacesPerSearch": 20,
    }));

    let places: Vec<Place> = client
        .run_actor("compass~crawler-google-places", input)
        .await?
        .wait_for_dataset()
        .await?;

    for p in places {
        println!("{} – {:?}", p.title, p.website);
    }
    Ok(())
}
```

## Multi-key fallback

```rust,no_run
# use apify_rust_client::ApifyClient;
let client = ApifyClient::new([
    std::env::var("APIFY_KEY_1").unwrap(),
    std::env::var("APIFY_KEY_2").unwrap(),
]);
// First key is tried; on submit failure (e.g. exhausted credit),
// the next key is automatically used.
```

## Configuration

```rust,no_run
# use apify_rust_client::ApifyClient;
# use std::time::Duration;
let client = ApifyClient::new(["your-key"])
    .poll_interval(Duration::from_secs(15))     // default 20s
    .max_wait(Duration::from_secs(2 * 60 * 60)); // default 1h
```

## Examples

```bash
APIFY_API_KEY=apify_api_xxx cargo run --example run_actor
```

## Popular actors to try

| Actor ID | Use case |
|---|---|
| `compass~crawler-google-places` | Google Maps scraping |
| `apify~web-scraper` | Generic JS-rendered scraping |
| `apify~rag-web-browser` | LLM-friendly URL → markdown |
| `pintostudio~tiktok-scraper` | TikTok |
| `streamers~linkedin-jobs-scraper` | LinkedIn jobs |

Browse all on [apify.com/store](https://apify.com/store).

## Pricing & free tier

Apify's free tier gives **$5/month in credits**. The cost per actor varies (typically $1–10 per 1000 results). With this crate's multi-key fallback you can chain multiple free accounts to extend that budget.

## Error handling

All errors are returned as `apify_rust_client::Error`:

- `Error::AllKeysFailed(s)` — every supplied key was rejected.
- `Error::RunFailed(s)` — actor terminated with `FAILED`/`ABORTED`/`TIMED-OUT`.
- `Error::Timeout(d)` — polling exceeded `max_wait`.
- `Error::ApiStatus { status, body }` — non-2xx HTTP response.
- `Error::Http(e)` / `Error::Json(e)` — transport / parsing.

## Roadmap

- Sync run endpoint (`/run-sync-get-dataset-items`) for short-lived actors
- Streaming dataset download (`tokio::Stream`)
- Webhook builder for completion notifications
- Built-in input validation against actor schema

PRs welcome.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT License ([LICENSE-MIT](LICENSE-MIT))

at your option.
