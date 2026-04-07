# Adaptive Concurrency Dynamic Module

This repository ports the logic from `/Users/adamburan/git/adaptive-concurrency-wasm` into an Envoy HTTP dynamic module written with the official Rust SDK.

The module keeps **per-upstream-host** latency state, applies the **Gradient2** algorithm per host, marks degraded hosts as overloaded, and sheds a deterministic fraction of incoming requests proportional to the overloaded-host fraction.

## What it ports

- Per-host latency tracking keyed by upstream address
- Gradient2 limit recalculation
- Overload / recovery thresholds
- Deterministic request shedding
- Observability headers and Envoy stats
- Adaptive timeout calculation from healthy-host latency

## Important adaptation

The WASM version used a periodic root-context tick. The dynamic-module HTTP filter path does not expose the same hook, so housekeeping runs opportunistically during request and response processing:

- request headers: refresh state, decide whether to shed, optionally inject timeout header
- response headers: record latency sample, refresh state, tag overloaded-host responses

## Build

```bash
cargo build --release
cargo test
```

This crate uses the sibling Envoy checkout at `../envoy` for the Rust dynamic-module SDK so the module stays ABI-compatible with your local Envoy branch.

## Envoy usage

Set the module search path so Envoy can resolve `name: adaptive_concurrency_module` to `libadaptive_concurrency_module.so` (or the platform equivalent while developing locally):

```bash
export ENVOY_DYNAMIC_MODULES_SEARCH_PATH="$(pwd)/target/release"
```

Then reference the module from an HTTP filter:

```yaml
- name: dynamic_modules/adaptive_concurrency
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.http.dynamic_modules.v3.DynamicModuleFilter
    dynamic_module_config:
      name: adaptive_concurrency_module
    filter_name: adaptive_concurrency
    filter_config:
      "@type": type.googleapis.com/google.protobuf.StringValue
      value: |
        {
          "initial_concurrency_limit": 100,
          "sample_window_size": 250,
          "sample_percentile": 0.9,
          "overload_gradient_threshold": 0.7,
          "recovery_gradient_threshold": 0.95,
          "recovery_timeout_secs": 10
        }
```

See [examples/envoy.yaml](/Users/adamburan/git/envoy-dynamic-modules/examples/envoy.yaml) for a fuller example.

## Runtime behavior

- When overloaded hosts exist, the filter can inject `x-envoy-upstream-rq-per-try-timeout-ms` using the computed adaptive timeout.
- When a request is shed, the filter returns a local reply with:
  - `x-envoy-overloaded: true`
  - `x-adaptive-concurrency-shed: true`
- When a response comes from an overloaded host, the filter adds:
  - `x-adaptive-concurrency-limited: true`
  - `x-overloaded-host: <host>`

## Current limitations

- This matches the WASM repository’s behavior rather than Envoy’s in-tree adaptive concurrency controller exactly.
- Housekeeping is event-driven instead of timer-driven.
- The module computes the adaptive timeout from tracked healthy hosts and injects it as a request header; actual retry behavior still depends on the surrounding Envoy retry policy.
