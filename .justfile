set dotenv-load := true

# Docker test database URL (port 5433 to avoid conflicts)
docker_db := "postgresql://backfill:backfill@localhost:5433/backfill_test"

_help:
	just -l

# --------------------------------------------------------------------------
# Docker-based Testing (recommended)
# --------------------------------------------------------------------------

# Start the test database container
db-up:
	docker compose up -d --wait
	@echo "PostgreSQL ready at localhost:5433"

# Stop the test database container
db-down:
	docker compose down

# Stop and remove all test data
db-clean:
	docker compose down -v
	@echo "Test database and volumes removed"

# Show test database logs
db-logs:
	docker compose logs -f postgres

# Run all tests against Docker PostgreSQL
test-docker: db-up
	#!/usr/bin/env bash
	export DATABASE_URL="{{docker_db}}"
	cargo nextest run -F axum

# Run specific tests against Docker PostgreSQL
test-docker-one ARG: db-up
	#!/usr/bin/env bash
	export DATABASE_URL="{{docker_db}}"
	cargo nextest run -F axum {{ARG}}

# Run queue behavior tests (parallel vs serial)
test-queues: db-up
	#!/usr/bin/env bash
	export DATABASE_URL="{{docker_db}}"
	cargo nextest run -F axum -E 'test(queue) | test(parallel) | test(serial)'

# --------------------------------------------------------------------------
# Local PostgreSQL Testing (requires local pg installation)
# --------------------------------------------------------------------------

# Run all tests using nextest (local PostgreSQL)
test:
	#!/usr/bin/env bash
	if ! psql -lqtA | grep -q "^backfill_test|" ; then
		createdb backfill_test
	fi
	export DATABASE_URL="postgresql://localhost:5432/backfill_test"
	cargo nextest run -F axum

# Run the named tests using nextest (local PostgreSQL)
test-one ARG:
	#!/usr/bin/env bash
	if ! psql -lqtA | grep -q "^backfill_test|" ; then
		createdb backfill_test
	fi
	export DATABASE_URL="postgresql://localhost:5432/backfill_test"
	cargo nextest run -F axum {{ARG}}

# --------------------------------------------------------------------------
# Coverage
# --------------------------------------------------------------------------

# Get a testing coverage report (Docker)
coverage-docker: db-up
	#!/usr/bin/env bash
	export DATABASE_URL="{{docker_db}}"
	cargo llvm-cov nextest --all-targets -F axum --workspace --summary-only

# Get a testing coverage report (local PostgreSQL)
coverage:
	#!/usr/bin/env bash
	if ! psql -lqtA | grep -q "^backfill_test|" ; then
		createdb backfill_test
	fi
	export DATABASE_URL="postgresql://localhost:5432/backfill_test"
	cargo llvm-cov nextest --all-targets -F axum --workspace --summary-only

# Get the coverage percentage only (local PostgreSQL)
coverage-pct:
	#!/usr/bin/env bash
	if ! psql -lqtA | grep -q "^backfill_test|" ; then
		createdb backfill_test
	fi
	export DATABASE_URL="postgresql://localhost:5432/backfill_test"
	cargo llvm-cov --quiet --all-targets -F axum --workspace --summary-only 2>&1 | tail -1 | cut -f4 -w

# --------------------------------------------------------------------------
# Formatting and Linting
# --------------------------------------------------------------------------

# Run the nightly formatter
fmt:
	cargo +nightly fmt

# Auto-fix clippy complaints
lint:
	cargo clippy --fix --all-targets -F axum

# --------------------------------------------------------------------------
# CI
# --------------------------------------------------------------------------

# Run the same checks we run in CI (uses Docker)
ci: test-docker fmt
	cargo clippy --all-targets -F axum
	cargo test --doc

# Run CI checks with local PostgreSQL
ci-local: test fmt
	cargo clippy --all-targets -F axum
	cargo test --doc

# --------------------------------------------------------------------------
# Setup and Release
# --------------------------------------------------------------------------

# Install required tools
setup:
	brew tap ceejbot/tap
	brew install fzf cargo-nextest tomato semver-bump
	rustup install nightly

# Tag a new version for release
version BUMP:
	#!/usr/bin/env bash
	set -e
	current=$(tomato get package.version Cargo.toml)
	version=$(semver-bump {{BUMP}} "$current")
	tomato set package.version "$version" Cargo.toml &> /dev/null
	cargo generate-lockfile
	git commit Cargo.toml Cargo.lock -m "v${version}"
	git tag "v${version}"
	echo "Release tagged for version v${version}"

# Publish to crates.io
release:
	cargo publish
