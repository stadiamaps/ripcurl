test:
    cargo nextest run --no-fail-fast
    cargo test --doc

smoke-test:
    cargo build && nu tests_integration/smoke.nu
