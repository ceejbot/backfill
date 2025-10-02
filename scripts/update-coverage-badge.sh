#!/bin/bash
# Update coverage badge in README.md
# Usage: ./scripts/update-coverage-badge.sh

set -euo pipefail

echo "Generating code coverage report..."
COVERAGE=$(cargo llvm-cov --all-targets --workspace --summary-only | grep -E "TOTAL.*[0-9]+\.[0-9]+%" | grep -oE "[0-9]+\.[0-9]+%" | head -1)

if [[ -z "$COVERAGE" ]]; then
    echo "Error: Could not extract coverage percentage"
    exit 1
fi

echo "Current coverage: $COVERAGE"

# Determine badge color based on coverage percentage
COVERAGE_NUM=${COVERAGE%\%}
if (( $(echo "$COVERAGE_NUM >= 80" | bc -l) )); then
    COLOR="brightgreen"
elif (( $(echo "$COVERAGE_NUM >= 60" | bc -l) )); then
    COLOR="yellow"
elif (( $(echo "$COVERAGE_NUM >= 40" | bc -l) )); then
    COLOR="orange"
else
    COLOR="red"
fi

echo "Badge color: $COLOR"

# Update README.md
if [[ -n $(which sd) ]]; then
	COVERAGE_ESCAPED=$(echo "$COVERAGE" | sd -s '%' '%25')
	sd "coverage-[0-9]*\.[0-9]*%25-[a-z]*" "coverage-${COVERAGE_ESCAPED}-${COLOR}" README.md
else
	COVERAGE_ESCAPED=$(echo "$COVERAGE" | sed 's/%/%25/g')
	sed -i.bak "s/coverage-[0-9]*\.[0-9]*%25-[a-z]*/coverage-${COVERAGE_ESCAPED}-${COLOR}/" README.md
	echo "Backup saved as README.md.bak"
fi


echo "Updated coverage badge in README.md to $COVERAGE ($COLOR)"
