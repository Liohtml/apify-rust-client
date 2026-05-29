# Changelog

## [Unreleased]

### Security
- API tokens are now sent via the `Authorization: Bearer` header instead of the
  URL query string, so they no longer leak into server/proxy access logs (#1).
- `actor_id` is validated against an allowlist to prevent URL path injection (#5).
- `api_base` rejects non-HTTPS URLs (except `localhost`/`127.0.0.1`) to avoid
  transmitting credentials in plaintext (#4).

### Added
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
- `api_base` strips a trailing slash to avoid malformed double-slash URLs (#11).

## [0.1.0] - 2026-05-02

### Added
- Initial release.
- `ApifyClient` with builder API for poll interval, max wait, and API base.
- `run_actor` returns a `RunHandle` with `wait_for_dataset` / `wait_for_status`.
- Generic `fetch_dataset_items::<T>()` deserializes any item shape.
- Multi-key fallback on submit failure.
