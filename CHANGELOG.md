# Changelog

## [Unreleased]

### Security
- `Debug` for `ApifyClient` and `RunHandle` is now hand-written and redacts the
  API tokens, so they no longer leak via `{:?}` / `tracing` / `dbg!` (#28, #26).
- API tokens are now sent via the `Authorization: Bearer` header instead of the
  URL query string, so they no longer leak into server/proxy access logs (#1).
- `actor_id` is validated against an allowlist to prevent URL path injection (#5).
- `api_base` rejects non-HTTPS URLs (except `localhost`/`127.0.0.1`) to avoid
  transmitting credentials in plaintext (#4).

### Tests
- Add wiremock-based integration tests for the async paths: multi-key fallback
  on 401, immediate surfacing of 404 (no rotation), retry on 429, polling
  transitions (RUNNING→SUCCEEDED, FAILED), dataset pagination, and the
  `max_items` cap (#16, #25).

### CI
- Add a `release` workflow that runs `cargo publish` on a pushed `v*` tag
  (using the `CRATES_IO_TOKEN` secret) and verifies the tag matches the
  Cargo.toml version; README/badges switched to the crates.io form in
  preparation for the first publish (#27).
- Pin all GitHub Actions to commit SHAs and add Dependabot (github-actions +
  cargo) to keep them current — closes the tag-repoint supply-chain risk (#15).
- Add an MSRV job that builds on Rust 1.85 so the declared `rust-version` is
  actually verified, not just `stable` (#29).
- Add a `cargo doc` step with `RUSTDOCFLAGS=-D warnings` so broken doc examples
  and dead intra-doc links fail CI (#32).

### Added
- `ApifyClient::try_new` — fallible constructor returning `Error::Http` instead
  of panicking when the HTTP client cannot be built (#19).
- `ApifyClient::max_retries` — automatic retry with exponential backoff for
  transient errors (`429`, `5xx`, network blips); default 3 (#9).
- `ApifyClient::max_items` — cap the number of dataset items downloaded to
  bound memory use on large datasets (#6).
- `RunInput::from_serialize` — build input from any `Serialize` type (#10).
- GitHub Actions CI: fmt, clippy, test, build examples (#7).

### Fixed
- `wait_for_status` now surfaces HTTP errors (401/429/500/…) instead of masking
  them as `Timeout`, and clamps its sleep to the remaining `max_wait` so the
  timeout is enforced even when `poll_interval > max_wait` (#2, #3).
- `wait_for_status` polls before sleeping, so fast runs are detected
  immediately instead of always waiting one `poll_interval` (#21, #24).
- `run_actor` surfaces bad-request failures (400/404/405/422 — wrong actor id
  or input) immediately instead of masking them as `AllKeysFailed`, while still
  rotating to the next key on auth/credit (401/402/403), rate-limit, server, and
  transport errors so the multi-key fallback still covers exhausted credit (#22).
- `fetch_dataset_items` now enforces the `max_wait` deadline so a huge dataset
  cannot loop indefinitely (#23).
- Body-read transport errors are propagated as `Error::Http` instead of being
  silently turned into a misleading JSON parse error (#30).
- `api_base` strips a trailing slash to avoid malformed double-slash URLs (#11).
- Crate-level doc example uses array syntax `ApifyClient::new([…])` to match the
  README and signal that any `IntoIterator` is accepted (#31).

## [0.1.0] - 2026-05-02

### Added
- Initial release.
- `ApifyClient` with builder API for poll interval, max wait, and API base.
- `run_actor` returns a `RunHandle` with `wait_for_dataset` / `wait_for_status`.
- Generic `fetch_dataset_items::<T>()` deserializes any item shape.
- Multi-key fallback on submit failure.
