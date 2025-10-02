# Changelog

All notable changes to the Backfill project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project will adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) after reaching version 1.0.0.

## [Unreleased]

### Added
- Implemented `WorkerRunner::process_available_jobs()` for batch processing and testing scenarios
- Comprehensive Dead Letter Queue (DLQ) system with full CRUD operations
- DLQ processor for automatic migration of failed jobs to the DLQ table
- Admin HTTP API with DLQ management endpoints (experimental, feature-gated behind `axum`)
- Exponential backoff retry system with jitter to prevent thundering herds
- Three preset retry policies: `fast()`, `aggressive()`, `conservative()`
- Priority-based job scheduling with six priority levels
- Named queue support (Fast, Bulk, Custom)
- Comprehensive metrics using the `metrics` facade crate
- Structured logging via the `log` crate
- Complete documentation suite (8 major guides + examples)
- Five working examples demonstrating key usage patterns
- Integration test suite with isolated schema testing pattern (55 tests, 74.62% coverage)
- Code coverage reporting in CI with badges
- SQLx compile-time query verification

### Changed
- Switched from `tracing` to `log` crate for logging (tracing still used for instrumentation)
- Metrics now use Prometheus-compatible naming with underscores

### Fixed
- Enforced `#![deny(clippy::unwrap_used)]` to eliminate `.unwrap()` calls
- Fixed admin_server example compilation with feature gates

### Documentation
- Added API status table to Admin API docs documenting experimental endpoints
- Added Known Limitations section to DLQ guide
- Documented queue name and payload visibility limitations
- Added comprehensive testing guide with isolated schema pattern
- Created CLAUDE.md for AI pair programming guidance
- Added DLQ migrations guide
- Added metrics integration guide

### Security
- Added `cargo-audit` for vulnerability checking in CI
- No `unsafe` code allowed (`#![deny(unsafe_code)]`)

## [0.1.0] - UNRELEASED

Initial development version. Not yet recommended for production use.

### Core Features
- PostgreSQL-backed async job queue using GraphileWorker
- Job enqueueing with priorities, queues, and scheduling
- Worker runner with graceful shutdown and background task patterns
- Dead Letter Queue for failed job management
- Retry policies with exponential backoff
- Metrics and observability support

---

## Release Checklist for 1.0.0

Before releasing 1.0.0, the following items should be completed:

- [ ] Finalize Admin API (complete stub endpoints or mark as experimental)
- [ ] Resolve DLQ queue_name tracking limitation
- [ ] Resolve DLQ payload visibility limitation (or document workaround)
- [ ] Complete documentation restructuring (quick start → tutorials → reference)
- [ ] Production testing in real workloads
- [ ] Performance benchmarking and tuning
- [ ] Security audit
- [ ] Publish to crates.io

---

For the complete project history and detailed commit messages, see the Git log:
```bash
git log --oneline --graph
```
