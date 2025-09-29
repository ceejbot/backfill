# Code Coverage

This project uses `cargo-llvm-cov` for comprehensive code coverage analysis. Coverage is tracked for all tests and automatically reported in CI.

## Current Status

**Overall Coverage: 51.94%**
- Core Library (`src/lib.rs`): **73.49%** - Excellent ✅
- Error Handling (`src/errors.rs`): **74.07%** - Excellent ✅  
- Worker Binary (`src/bin/backfill-worker.rs`): **16.02%** - Expected low for runtime binary ⚠️

## Running Coverage Locally

### Basic Coverage Report
```bash
cargo llvm-cov
```

### Generate HTML Report
```bash
cargo llvm-cov --html
# Open target/llvm-cov/html/index.html in browser
```

### Summary Only
```bash
cargo llvm-cov --summary-only
```

### Generate LCOV for External Tools
```bash
cargo llvm-cov --lcov --output-path lcov.info
```

## Updating Coverage Badge

Use the provided script to update the README badge:
```bash
./scripts/update-coverage-badge.sh
```

This script:
1. Runs coverage analysis
2. Extracts the percentage  
3. Updates the README badge with appropriate color:
   - **≥80%**: Green (brightgreen)
   - **≥60%**: Yellow 
   - **≥40%**: Orange
   - **<40%**: Red

## CI Coverage Integration

Coverage is automatically generated in CI and uploaded to Codecov. The workflow:
1. Runs all tests with coverage instrumentation
2. Generates LCOV report
3. Uploads to Codecov (with token)
4. Displays coverage percentage in logs

## Coverage Targets

### Current Goals
- **Overall**: Maintain ≥50%, target 60%
- **Core Library**: Maintain ≥73%, target 85%
- **Error Handling**: Maintain ≥74%, target 80%
- **Worker Binary**: 16% is acceptable (runtime code)

### Improvement Areas
1. **Core Library Edge Cases**: Test boundary conditions, error paths
2. **Retry Logic**: Integration tests for exponential backoff behavior
3. **Error Paths**: Test more error classification scenarios

## Test Structure

Our **19 tests** cover:
- **10 Unit Tests** (`src/lib.rs`): Core functionality, retry policies, job specs
- **7 Integration Tests** (`tests/`): End-to-end database operations 
- **2 Worker Tests** (`src/bin/`): Configuration and error classification

## Understanding the Report

### HTML Report Analysis
The HTML report (`target/llvm-cov/html/index.html`) provides:
- Line-by-line coverage highlighting
- Function-level coverage statistics
- Branch coverage information
- Interactive filtering and navigation

### Key Metrics
- **Lines**: Percentage of executable lines hit by tests
- **Regions**: LLVM coverage regions (more granular than lines)
- **Functions**: Percentage of functions executed
- **Branches**: Conditional branch coverage (when available)

### Coverage Colors
- 🟢 **Green**: Well-covered code (≥80%)
- 🟡 **Yellow**: Moderately covered (60-79%)
- 🟠 **Orange**: Needs improvement (40-59%) 
- 🔴 **Red**: Poor coverage (<40%)

## Best Practices

1. **Run coverage before major changes** to establish baseline
2. **Review HTML reports** to identify specific gaps
3. **Focus on core library coverage** rather than binary coverage
4. **Test error paths and edge cases** not just happy paths
5. **Update badge regularly** with `./scripts/update-coverage-badge.sh`

## Tool Installation

If you don't have `cargo-llvm-cov`:
```bash
cargo install cargo-llvm-cov
```

The tool requires the `llvm-tools-preview` component:
```bash
rustup component add llvm-tools-preview
```
