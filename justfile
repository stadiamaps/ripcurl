test:
    cargo nextest run --no-fail-fast

smoke-test:
    cargo build && nu tests_integration/smoke.nu
