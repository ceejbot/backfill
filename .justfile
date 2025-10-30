set dotenv-load := true

_help:
	just -l

# Run all tests using nextest.
test:
	#!/usr/bin/env bash
	if ! psql -lqtA | grep -q "^backfill_test|" ; then
		createdb backfill_test
	fi
	export DATABASE_URL="postgresql://localhost:5432/backfill_test"
	cargo nextest run -F axum

# Run the named tests using nextest.
test-one ARG:
	#!/usr/bin/env bash
	if ! psql -lqtA | grep -q "^backfill_test|" ; then
		createdb backfill_test
	fi
	export DATABASE_URL="postgresql://localhost:5432/backfill_test"
	cargo nextest run -F axum {{ARG}}

# get a testing coverage report
coverage:
	#!/usr/bin/env bash
	if ! psql -lqtA | grep -q "^backfill_test|" ; then
		createdb backfill_test
	fi
	export DATABASE_URL="postgresql://localhost:5432/backfill_test"
	cargo llvm-cov nextest --all-targets -F axum --workspace --summary-only

# get the coverage percentage only
coverage-pct:
	#!/usr/bin/env bash
	if ! psql -lqtA | grep -q "^backfill_test|" ; then
		createdb backfill_test
	fi
	export DATABASE_URL="postgresql://localhost:5432/backfill_test"
	cargo llvm-cov --quiet --all-targets -F axum --workspace --summary-only 2>&1 | tail -1 | cut -f4 -w

# Run the nightly formatter
fmt:
	cargo +nightly fmt

# Run the same checks we run in CI. Requires nightly.
ci: test fmt
	cargo clippy --all-targets  -F axum
	cargo test --doc

# Auto-fix clippy complaints.
lint:
	cargo clippy --fix --all-targets  -F axum

# Install required tools
setup:
	brew tap ceejbot/tap
	brew install fzf cargo-nextest tomato semver-bump
	rustup install nightly

# Tag a new version for release.
version BUMP:
	#!/usr/bin/env bash
	set -e
	current=$(tomato get package.version Cargo.toml)
	version=$(echo "$current" | semver-bump {{BUMP}})
	tomato set package.version "$version" Cargo.toml &> /dev/null
	cargo generate-lockfile
	git commit Cargo.toml Cargo.lock -m "v${version}"
	git tag "v${version}"
	echo "Release tagged for version v${version}"

# publish to crates.io
release:
	cargo publish
