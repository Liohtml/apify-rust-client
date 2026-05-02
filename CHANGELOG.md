# Changelog

## [Unreleased]

## [0.1.0] - 2026-05-02

### Added
- Initial release.
- `ApifyClient` with builder API for poll interval, max wait, and API base.
- `run_actor` returns a `RunHandle` with `wait_for_dataset` / `wait_for_status`.
- Generic `fetch_dataset_items::<T>()` deserializes any item shape.
- Multi-key fallback on submit failure.
