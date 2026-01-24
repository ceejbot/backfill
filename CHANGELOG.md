# Changelog

All notable changes to the Backfill project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project will adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) after reaching version 1.0.0.

## [Unreleased]

### Breaking Changes
- **Queue API redesigned for parallel-by-default execution**
  - Removed `Queue::Fast`, `Queue::Bulk`, `Queue::DeadLetter`, `Queue::Custom` variants
  - Added `Queue::Parallel` (now the default) and `Queue::Serial(String)`
  - Jobs now execute **in parallel** by default across all workers
  - Use `Queue::serial("name")` or `Queue::serial_for("entity", id)` when you need serialization

  **Migration guide:**
  ```rust
  // Before (v1.x): Named queues caused unintended serialization
  JobSpec { queue: Queue::Fast, .. }      // ALL "fast" jobs ran one at a time!
  JobSpec { queue: Queue::Bulk, .. }      // ALL "bulk" jobs ran one at a time!
  JobSpec { queue: Queue::Custom("x".into()), .. }

  // After (v2.x): Parallel by default, explicit serialization
  JobSpec { queue: Queue::Parallel, .. }  // Jobs run concurrently (default)
  JobSpec { queue: Queue::serial("rate-limit-api"), .. }  // Explicit serialization
  JobSpec { queue: Queue::serial_for("user", user_id), .. }  // Per-entity serialization
  ```

### Added
- `Queue::serial(name)` - create a named serial queue
- `Queue::serial_for(entity, id)` - create per-entity serial queues (e.g., "user:123")
- `Queue::is_parallel()` and `Queue::is_serial()` helper methods
- `enqueue_serial()` convenience function for explicit serial execution
- Admin API endpoints for lock diagnostics: `GET /locks/status`, `POST /locks/cleanup`
- Docker Compose configuration for test database
- Comprehensive queue behavior tests (parallel vs serial execution)
- Implemented `WorkerRunner::process_available_jobs()` for batch processing and testing scenarios
- Comprehensive Dead Letter Queue (DLQ) system with full CRUD operations
- DLQ processor for automatic migration of failed jobs to the DLQ table
- Admin HTTP API with DLQ management endpoints (experimental, feature-gated behind `axum`)
- Exponential backoff retry system with jitter to prevent thundering herds
- Three preset retry policies: `fast()`, `aggressive()`, `conservative()`
- Priority-based job scheduling with six priority levels
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
