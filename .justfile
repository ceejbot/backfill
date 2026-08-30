set dotenv-load := true

# Docker test database URL (port 5433 to avoid conflicts)
docker_db := "postgresql://backfill:backfill@localhost:5433/backfill_test"

_help:
	just -l

# --------------------------------------------------------------------------
# Docker-based Testing (recommended)
# --------------------------------------------------------------------------

# Start the test database container, or reuse one that's already healthy.
# Fails if localhost:5433 is taken by something that is not this stack.
db-up:
	#!/usr/bin/env bash
	set -euo pipefail
	if docker compose exec -T postgres pg_isready -U backfill -d backfill_test >/dev/null 2>&1; then
		echo "Reusing running test Postgres on localhost:5433"
		exit 0
	fi
	if nc -z localhost 5433 >/dev/null 2>&1; then
		echo "localhost:5433 is in use, but this project's test Postgres is not healthy." >&2
		echo "Stop the other listener, or run: just db-down" >&2
		exit 1
	fi
	docker compose up -d
	for _ in $(seq 1 30); do
		if docker compose exec -T postgres pg_isready -U backfill -d backfill_test >/dev/null 2>&1; then
			echo "PostgreSQL ready at localhost:5433"
			exit 0
		fi
		sleep 1
	done
	echo "Test Postgres did not become ready on localhost:5433" >&2
	docker compose logs postgres >&2 || true
	exit 1

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

# Bucketing: each commit since the previous tag is assigned to ONE section
# based on which paths it touched. Precedence is src > examples > tests >
# docs > build/config — a commit that touches both src and docs is
# primarily a library change and lands under Library. Empty sections are
# omitted.
#
# Workflow:
#   just prerelease minor   # writes scaffold, opens editor
#   <edit narrative + migration notes>
#   git add CHANGELOG.md && git commit -m "Prepare $version"
#   just version minor      # bumps Cargo.toml + tags
#
# Set NO_EDIT=1 to skip launching $EDITOR (useful for previewing).

# Generate a CHANGELOG scaffold for the next release, then open $EDITOR
prerelease BUMP:
	#!/usr/bin/env bash
	set -euo pipefail

	# Refuse if CHANGELOG has uncommitted changes — we'd clobber them
	# during scaffold insertion.
	if ! git diff --quiet -- CHANGELOG.md || ! git diff --cached --quiet -- CHANGELOG.md; then
		echo "CHANGELOG.md has uncommitted changes; stash or commit first." >&2
		exit 1
	fi

	current=$(tomato get package.version Cargo.toml)
	next=$(semver-bump {{BUMP}} "$current")
	prev_tag=$(git describe --tags --abbrev=0 2>/dev/null || true)

	if [ -z "$prev_tag" ]; then
		range="HEAD"
		echo "No previous tag found; including full history."
	else
		range="${prev_tag}..HEAD"
	fi

	# Bucket commits by primary touched-path.
	lib_lines=""
	example_lines=""
	test_lines=""
	doc_lines=""
	build_lines=""

	while IFS= read -r commit; do
		[ -z "$commit" ] && continue
		sha="${commit%% *}"
		subject="${commit#* }"
		files=$(git show --name-only --format= "$sha")

		if echo "$files" | grep -q "^src/"; then
			lib_lines+="- ${sha} ${subject}"$'\n'
		elif echo "$files" | grep -q "^examples/"; then
			example_lines+="- ${sha} ${subject}"$'\n'
		elif echo "$files" | grep -q "^tests/"; then
			test_lines+="- ${sha} ${subject}"$'\n'
		elif echo "$files" | grep -q "^docs/"; then
			doc_lines+="- ${sha} ${subject}"$'\n'
		else
			build_lines+="- ${sha} ${subject}"$'\n'
		fi
	done < <(git log "$range" --format="%h %s")

	# Build the scaffold and prepend it above the topmost "## [" entry.
	scaffold=$(mktemp)
	trap 'rm -f "$scaffold"' EXIT

	emit_section() {
		local heading="$1"
		local lines="$2"
		if [ -n "$lines" ]; then
			echo "### ${heading}"
			echo
			printf "%s" "$lines"
			echo
		fi
	}

	{
		echo "## [${next}] — TODO: write title"
		echo
		echo "TODO: write narrative."
		echo
		emit_section "Library"        "$lib_lines"
		emit_section "Documentation"  "$doc_lines"
		emit_section "Examples"       "$example_lines"
		emit_section "Tests"          "$test_lines"
		emit_section "Build / config / CI" "$build_lines"
	} > "$scaffold"

	awk -v insert="$scaffold" '
		/^## \[/ && !done {
			while ((getline line < insert) > 0) print line
			done = 1
		}
		{ print }
	' CHANGELOG.md > CHANGELOG.md.tmp
	mv CHANGELOG.md.tmp CHANGELOG.md

	echo "CHANGELOG scaffold inserted for v${next}."

	if [ -z "${NO_EDIT:-}" ]; then
		echo "Opening editor..."
		${EDITOR:-vi} CHANGELOG.md
	fi

	echo
	echo "Next steps:"
	echo "  git add CHANGELOG.md && git commit -m \"Prepare ${next}\""
	echo "  just version {{BUMP}}"

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
