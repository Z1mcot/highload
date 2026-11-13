# AGENTS.md — DDIA HW2: Resilience Strategies (Rust)

## 1. Purpose and source of truth

This repository contains a small educational Rust framework for resilience strategies when calling a model external service. Implement **all six** required items: default, retries, exponential backoff retries, round-robin, hedging, and round-robin with circuit breaking.

Before editing, read in this order:
1. `ITMO-DDIA-2026-MSE_MHS-HW-2.md` — original assignment; requirements take precedence.
2. `ARCHITECTURE.md` — chosen design and explicit interpretations of ambiguities.
3. `README.md` — how to run and understand the project.
4. Existing code and tests — preserve working behavior.

If a file is absent, say so. Never claim a requirement is implemented without a test demonstrating it.

## 2. Primary rule: human-readable Rust

The reader must be able to understand every line of code without knowledge of framework internals.

- Prefer the smallest straightforward implementation over a clever generalized one.
- Use descriptive names: `remaining_time`, `attempt_count`, `server_index`, `failure_rate`, not cryptic abbreviations.
- Keep functions focused; extract helpers for **meaningful** concepts, not one-line wrappers.
- Prefer `enum` and `struct` for domain concepts, and `trait` only for meaningful substitution points.
- Avoid unnecessary generics, macros, complex lifetime gymnastics, type erasure, `unsafe`, global mutable state, and deeply nested combinators.
- Avoid hidden side effects; make updates to counters and breaker state explicit.
- No `unwrap()`/`expect()` in production paths where failure is possible. In tests, they may be used with context.
- Handle edge cases directly; prefer `Result` and informative error variants over panics.
- Do not duplicate retry engines or copy/paste complete strategy implementations.
- Comments explain **why**, invariants, and non-obvious concurrency; they do not paraphrase syntax.
- Public API types and strategy invariants have short `///` documentation.
- Use English for Rust identifiers and comments, Russian for user-oriented explanations in README if convenient; be consistent.
- Write tests with names describing observable behavior, e.g. `retries_after_server_error_until_success`.
- If an abstraction makes the project harder to read, propose a simpler version.

## 3. Architecture boundaries

Use **one Rust library crate**, no workspace or multiple crates unless explicitly justified.

- `model`: outcomes, response payload, error types.
- `budget`: validated configuration and per-logical-request tracking.
- `transport`: `SubrequestSender` abstraction, independent of strategy or timeout settings.
- `client`: public `Client` and `MultiClient` strategy interfaces.
- `strategies`: default, retry, backoff, round-robin, hedging, circuit breaker.
- `tests`: controllable fake sender and observable integration tests.

Suggested files are described in `ARCHITECTURE.md`; keep the repository structure proportional to its size. A strategy must not know how a fake sender is implemented.

## 4. Semantics that must remain explicit

- An invocation is one logical request; each sender operation is a subrequest.
- `2xx` is success; `4xx`, `5xx`, and timeout are distinct outcomes. Preserve payload for success.
- Sender method accepts **no** client timeout, server timeout, or preselected response outcomes.
- Bound all work by one monotonic overall deadline. Per-attempt timeout is `min(subrequest_timeout, remaining_overall_time)`.
- Count attempts **when they start**; distinguish attempts started from completed outcomes.
- For this assignment, `max_attempts=1` means one call, matching the document's `max_retries=1` wording for default. Document this deliberate naming choice.
- Backoff applies after `5xx` or timeout, not after `4xx`. Include backoff sleeps in overall deadline.
- Round-robin advances on each subrequest, not only on successful outcomes.
- Hedging: first sender immediately; if **no response** has arrived by `hedging_delay`, launch all remaining senders concurrently at most once each. An early 4xx/5xx is a response and must not trigger fanout. Return first success, cancel/stop waiting for losers. If there is no success, wait until the overall deadline and return a deadline error as required by the assignment.
- Circuit breaker extends round-robin with per-sender state, isolation period, and a recovery mechanism. Define state scope and concurrency assumptions in `ARCHITECTURE.md`.
- Validate empty sender lists and invalid budgets without panicking.

**Important:** The assignment does not precisely define every budget edge case, retry-on-4xx policy, circuit-breaker state lifetime, or hedging early-error behavior. Use documented project decisions, not invented claims about the assignment. Raise design changes in your report.

## 5. Runtime and dependencies

Prefer stable Rust, edition 2021 or 2024 depending on the installed toolchain, `tokio` (timeouts, sleeps, asynchronous tests), and `async-trait` only if it materially improves the readability of object-safe interfaces. Consider `futures` only when required. Do not use a real HTTP client, database, `tower`, or `reqwest` unless a demonstrated requirement calls for them. Pin dependencies via `Cargo.lock` for reproducibility.

## 6. Testing requirements

For **each strategy**, write at least 3–5 readable behavior-oriented tests. Use a scripted fake sender that can return outcomes after controlled delays or never complete; avoid real network access.

Cover at minimum:
- success; failure and exhaustion; timeout and deadline;
- exactly one attempt for default;
- per-error budgets and total attempt budget for retry;
- no backoff after `4xx`, increasing delays after `5xx`/timeout, overall deadline during sleep;
- round-robin ordering and wraparound;
- hedging fanout timing, first-success-wins, cleanup of loser tasks;
- breaker exclusion and reentry after cooldown; all-open case;
- rejected invalid configuration.

Use `tokio::time::pause()` / `advance()` where suitable instead of flaky wall-clock timing. Never weaken a failing test to make it pass.

## 7. Commands and completion

The root `test.sh` **must** be executable and run the tests. Standard checks:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
./test.sh
```

If checks cannot run, state what was attempted and why. Do not claim success without command output.

## 8. Agent working procedure

1. Inspect the assignment, existing repository, documentation and tests.
2. Before major changes, summarize the current behavior, smallest change and affected files.
3. Implement one self-contained strategy at a time with tests.
4. Reuse common subrequest/deadline/attempt helpers; avoid overly generic policy engines.
5. Run formatting, linting and tests, then review readability and failure paths.
6. Update README and architecture only for actual design decisions.
7. Report changed files, tested scenarios, command results and any open questions.

Do not silently alter public semantics, reset state unexpectedly, hide failed checks or create an unrelated demo application. Do not overwrite prior homework in the same repository. Do not open a PR or push without user authorization.
