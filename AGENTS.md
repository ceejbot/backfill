
### Project structure

```text
.
├── AGENTS.md    # Instructions for coding agents
├── Cargo.toml   # Rust project manifest
├── docs				 # All documentation, in markdown format
├── README.md    # The readme for humans
├── src          # Rust source
│   └─ lib.rs    # Rust library exports
└── tests        # Integration tests
````

### Core commands

Install via homebrew if these are missing.

- Is it about finding FILES? use `fd`
- Is it about finding TEXT/strings? use `rg`
- Is it about finding CODE STRUCTURE? use `ast-grep`
- Is it about SELECTING from multiple results? pipe to `fzf`
- Is it about interacting with JSON? use `jq`
- Is it about interacting with YAML or XML? use `yq`
- Is it about interacting with TOML? use `tomato`
- Is it about FINDING and REPLACING text by pattern in files? use `sd`

Building and testing:

- Check for code errors quickly: `cargo check`
- Lint code: `cargo clippy --all-targets`
- Build code: `cargo build`
- Build for release: `cargo build --release`
- Clean all build artifacts: `cargo clean`
- Run all tests: `cargo nextest r`
- Run a single test: `cargo nextest r string-match`
- Format code with project style: `cargo +nightly fmt`
