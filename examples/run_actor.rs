//! Run with:
//!   APIFY_API_KEY=... cargo run --example run_actor

use apify_rust_client::{ApifyClient, RunInput};
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
struct Place {
    title: Option<String>,
    website: Option<String>,
    address: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let key = std::env::var("APIFY_API_KEY").expect("APIFY_API_KEY env var not set");
    let client = ApifyClient::new([key]);

    let input = RunInput::new(json!({
        "searchStringsArray": ["coffee shop Berlin"],
        "language": "en",
        "maxCrawledPlacesPerSearch": 10,
    }));

    let handle = client
        .run_actor("compass~crawler-google-places", input)
        .await?;
    println!("run_id = {}", handle.run_id);

    let places: Vec<Place> = handle.wait_for_dataset().await?;
    println!("Got {} places", places.len());
    for p in places.iter().take(5) {
        println!(" - {:?} @ {:?}  → {:?}", p.title, p.address, p.website);
    }

    Ok(())
}
