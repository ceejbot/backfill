# Code Coverage

This project uses `cargo-llvm-cov` for comprehensive code coverage analysis. Coverage is tracked for all tests and automatically reported in CI.

## Current Status

**Overall Coverage: 74.62%** 🎉
- **Worker Module (`src/worker.rs`)**: **77.85%** - Excellent ✅
- **DLQ Module (`src/client/dlq.rs`)**: **70.02%** - Good ✅  
- **Retry Logic (`src/retries.rs`)**: **98.56%** - Excellent ✅
- **Priorities (`src/priorities.rs`)**: **100.00%** - Perfect ✅
- Error Handling (`src/errors.rs`): **80.95%** - Excellent ✅
- Core Library (`src/lib.rs`): **63.27%** - Good ✅

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

### Current Goals ✅ Achieved!
- **Overall**: ✅ **74.62%** (exceeded 60% target)
- **Worker Module**: ✅ **77.85%** (exceeded 70% target)
- **DLQ Module**: ✅ **70.02%** (met 70% target)
- **Retry Logic**: ✅ **98.56%** (excellent)
- **Priorities**: ✅ **100%** (perfect)

### Next Steps
1. **Core Library**: Improve from 63% to 70%+ with edge case testing
2. **Client/Enqueue**: Improve from 50% to 60%+ with more integration tests
3. **Admin API**: Add comprehensive tests when `axum` feature is enabled

## Test Structure

Our **46 tests** cover:
- **13 Unit Tests** (`src/lib.rs`): Core functionality, retry policies, job specs, worker config
- **7 Integration Tests** (`tests/integration_tests_clean.rs`): End-to-end database operations  
- **16 DLQ Tests** (`tests/dlq_tests.rs`): Dead letter queue functionality with filtering, stats, requeue
- **17 Worker Tests** (`tests/worker_tests.rs`): Worker runner, configuration, job handling

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
