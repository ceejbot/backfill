set dotenv-load := true

_help:
	just -l

# Run all tests using nextest.
test:
	cargo nextest run

# get a testing coverage report
coverage:
	cargo llvm-cov --all-targets --workspace --summary-only

# Run the nightly formatter
fmt:
	cargo +nightly fmt

# Run the same checks we run in CI. Requires nightly.
ci: test fmt
	cargo clippy --all-targets

# Auto-fix clippy complaints.
lint:
	cargo clippy --fix --all-targets

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
