//! End-to-end tests against a mocked Apify API (wiremock).
//!
//! These exercise the async code paths that the in-crate unit tests can't:
//! key rotation, retry/backoff, polling transitions, pagination and the
//! `max_items` cap. Covers issues #16 and #25.

use std::time::Duration;

use apify_rust_client::{ApifyClient, Error, RunInput};
use serde::Deserialize;
use serde_json::json;
use wiremock::matchers::{body_json_string, header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[derive(Debug, Deserialize)]
struct Item {
    v: i64,
}

/// A client pointed at the mock server, with a tiny poll interval so polling
/// loops don't actually wait.
fn client<const N: usize>(server: &MockServer, keys: [&str; N]) -> ApifyClient {
    ApifyClient::new(keys)
        .api_base(server.uri()) // http://127.0.0.1:PORT — allowed by is_secure_base
        .poll_interval(Duration::from_millis(1))
        .max_wait(Duration::from_secs(10))
}

fn run_started(dataset_id: &str) -> ResponseTemplate {
    ResponseTemplate::new(201)
        .set_body_json(json!({ "data": { "id": "run1", "defaultDatasetId": dataset_id } }))
}

#[tokio::test]
async fn run_actor_sends_bearer_token_and_returns_handle() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/acts/me~actor/runs"))
        .and(header("authorization", "Bearer key1"))
        .and(body_json_string(r#"{"foo":"bar"}"#))
        .respond_with(run_started("ds1"))
        .expect(1)
        .mount(&server)
        .await;

    let c = client(&server, ["key1"]);
    let handle = c
        .run_actor("me~actor", RunInput::new(json!({ "foo": "bar" })))
        .await
        .expect("run starts");
    assert_eq!(handle.run_id, "run1");
    assert_eq!(handle.dataset_id, "ds1");
}

#[tokio::test]
async fn multi_key_fallback_rotates_on_401() {
    let server = MockServer::start().await;
    // First token is rejected …
    Mock::given(method("POST"))
        .and(path("/acts/me~actor/runs"))
        .and(header("authorization", "Bearer key1"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&server)
        .await;
    // … the second succeeds.
    Mock::given(method("POST"))
        .and(path("/acts/me~actor/runs"))
        .and(header("authorization", "Bearer key2"))
        .respond_with(run_started("ds1"))
        .mount(&server)
        .await;

    let c = client(&server, ["key1", "key2"]);
    let handle = c
        .run_actor("me~actor", RunInput::new(json!({})))
        .await
        .expect("second key works");
    assert_eq!(handle.run_id, "run1");
}

#[tokio::test]
async fn bad_request_404_is_surfaced_not_rotated() {
    let server = MockServer::start().await;
    // 404 for every key — a typo'd actor id. Must surface immediately as
    // ApiStatus, not be masked as AllKeysFailed (issue #22).
    Mock::given(method("POST"))
        .and(path("/acts/typo~actor/runs"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1) // tried once, not once-per-key
        .mount(&server)
        .await;

    let c = client(&server, ["key1", "key2"]);
    let err = c
        .run_actor("typo~actor", RunInput::new(json!({})))
        .await
        .expect_err("404 surfaces");
    match err {
        Error::ApiStatus { status, .. } => assert_eq!(status, 404),
        other => panic!("expected ApiStatus 404, got {other:?}"),
    }
}

#[tokio::test]
async fn submit_retries_on_429_then_succeeds() {
    let server = MockServer::start().await;
    // 429 the first two attempts (higher priority, exhausted after 2 calls) …
    Mock::given(method("POST"))
        .and(path("/acts/me~actor/runs"))
        .respond_with(ResponseTemplate::new(429))
        .up_to_n_times(2)
        .with_priority(1)
        .mount(&server)
        .await;
    // … then succeed.
    Mock::given(method("POST"))
        .and(path("/acts/me~actor/runs"))
        .respond_with(run_started("ds1"))
        .with_priority(2)
        .mount(&server)
        .await;

    let c = client(&server, ["key1"]);
    let handle = c
        .run_actor("me~actor", RunInput::new(json!({})))
        .await
        .expect("retry recovers");
    assert_eq!(handle.run_id, "run1");
}

#[tokio::test]
async fn wait_for_dataset_polls_then_downloads() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/acts/me~actor/runs"))
        .respond_with(run_started("ds1"))
        .mount(&server)
        .await;
    // First poll RUNNING, then SUCCEEDED.
    Mock::given(method("GET"))
        .and(path("/actor-runs/run1"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "data": { "status": "RUNNING" } })),
        )
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/actor-runs/run1"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "data": { "status": "SUCCEEDED" } })),
        )
        .with_priority(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/datasets/ds1/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([{ "v": 1 }, { "v": 2 }])))
        .mount(&server)
        .await;

    let c = client(&server, ["key1"]);
    let items: Vec<Item> = c
        .run_actor("me~actor", RunInput::new(json!({})))
        .await
        .expect("run starts")
        .wait_for_dataset()
        .await
        .expect("dataset downloads");
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].v, 1);
}

#[tokio::test]
async fn wait_for_status_maps_failed_run() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/acts/me~actor/runs"))
        .respond_with(run_started("ds1"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/actor-runs/run1"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "data": { "status": "FAILED" } })),
        )
        .mount(&server)
        .await;

    let c = client(&server, ["key1"]);
    let handle = c
        .run_actor("me~actor", RunInput::new(json!({})))
        .await
        .unwrap();
    let err = handle
        .wait_for_dataset::<Item>()
        .await
        .expect_err("failed run errors");
    match err {
        Error::RunFailed(s) => assert_eq!(s, "FAILED"),
        other => panic!("expected RunFailed, got {other:?}"),
    }
}

#[tokio::test]
async fn fetch_dataset_paginates_until_short_page() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/acts/me~actor/runs"))
        .respond_with(run_started("ds1"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/actor-runs/run1"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "data": { "status": "SUCCEEDED" } })),
        )
        .mount(&server)
        .await;
    // Page 1: a full page of 1000 → the client must request another page.
    let full: Vec<_> = (0..1000).map(|i| json!({ "v": i })).collect();
    Mock::given(method("GET"))
        .and(path("/datasets/ds1/items"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!(full)))
        .mount(&server)
        .await;
    // Page 2: a short page of 5 → terminates pagination.
    let tail: Vec<_> = (1000..1005).map(|i| json!({ "v": i })).collect();
    Mock::given(method("GET"))
        .and(path("/datasets/ds1/items"))
        .and(query_param("offset", "1000"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!(tail)))
        .mount(&server)
        .await;

    let c = client(&server, ["key1"]);
    let items: Vec<Item> = c
        .run_actor("me~actor", RunInput::new(json!({})))
        .await
        .unwrap()
        .wait_for_dataset()
        .await
        .expect("paginates");
    assert_eq!(items.len(), 1005);
    assert_eq!(items[1004].v, 1004);
}

#[tokio::test]
async fn max_items_caps_download() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/acts/me~actor/runs"))
        .respond_with(run_started("ds1"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/actor-runs/run1"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "data": { "status": "SUCCEEDED" } })),
        )
        .mount(&server)
        .await;
    // With max_items(3) the client requests limit=3; the server returns exactly
    // a full page of 3, so the cap (not a short page) stops the download.
    Mock::given(method("GET"))
        .and(path("/datasets/ds1/items"))
        .and(query_param("limit", "3"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!([{ "v": 1 }, { "v": 2 }, { "v": 3 }])),
        )
        .expect(1)
        .mount(&server)
        .await;

    let c = client(&server, ["key1"]).max_items(3);
    let items: Vec<Item> = c
        .run_actor("me~actor", RunInput::new(json!({})))
        .await
        .unwrap()
        .wait_for_dataset()
        .await
        .expect("capped download");
    assert_eq!(items.len(), 3);
}
