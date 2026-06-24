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

## [0.1.0] - 2026-05-02

### Added
- Initial release.
- `ApifyClient` with builder API for poll interval, max wait, and API base.
- `run_actor` returns a `RunHandle` with `wait_for_dataset` / `wait_for_status`.
- Generic `fetch_dataset_items::<T>()` deserializes any item shape.
- Multi-key fallback on submit failure.
