Nonolith Connect (Rust port)
============================

Async-first Rust port of [nonolith-connect](../connect): a background daemon
that owns USB communication with Nonolith CEE and Analog Devices ADALM1000
("M1K") source-measure units and exposes them to multiple simultaneous
clients over REST (`/rest/v1/...`) and WebSocket (`/ws/v0`) on
`localhost:9003`. The primary client is [Pixelpulse](../pixelpulse).

The behavioral reference is **`../connect/SPEC.md`**; the C++ test suite
(`../connect/tests/`) is the executable spec this port was written against.

Licensed under the GNU GPLv3+, like the original.

Architecture
------------

* **tokio** runtime; **axum** served through a hand-configured hyper `http1`
  connection (Title-Case headers and the 30 s initial-request timeout, for
  wire fidelity with the Boost.Beast original).
* **nusb** for USB: pure-Rust, async, with hotplug — no libusb and no
  dedicated USB event thread. Bulk streaming runs as per-device tokio tasks
  keeping N transfers in flight, mirroring the C++ transfer queues.
* The domain model (`StreamingDevice`: ring buffer, capture state machine,
  waveform sources, listeners/triggers) lives behind one mutex per device;
  broadcasts are sent under the lock, preserving the C++ single-threaded
  ordering guarantees.
* Handlers are transport-independent (`RestRequest`/`RestResponse`,
  `ClientHandle`), so the whole REST+WS stack is unit-tested in-process.

Differences from the C++ (deliberate, see SPEC.md §9)
-----------------------------------------------------

* **Q1 fixed** — triangle wave uses `rem_euclid`, so the first
  quarter-period is no longer out of range.
* **Q2 fixed** — `getDeviceById("")` returns 404 instead of crashing.
* **Q5 fixed** — bad `GET /input` query parameters return a 402 JSON error
  instead of an uncaught-exception 500.
* **Q8 fixed** — sample counters are `u64` (no 11-hour wrap).
* **Q10 fixed** — serials are NUL-trimmed server-side.
* Everything else — including the 402 error status (Q4), silent unknown WS
  commands (Q6), the non-continuous stale-read window (Q7), no origin check
  on WS upgrades (Q9) — is kept bug-for-bug for client compatibility.
* **Slow-client backpressure** (new) — binary data frames are dropped for a
  WebSocket client whose outgoing queue exceeds 8 MiB, instead of growing
  memory without bound (the C++ had the same unbounded queue via
  websocketpp). Frames are self-describing (`idx`/`sampleIndex`), so clients
  tolerate the gap; JSON protocol messages are never dropped.
* **Graceful shutdown** (new) — Ctrl-C pauses any running captures so
  devices stop streaming and USB interfaces are released before exit.

Build & test
------------

    cargo build --release           # binary: target/release/nonolith-connect
    cargo test                      # unit tests (ported from the C++ doctest suite)
    python3 tests/e2e.py target/release/nonolith-connect

`tests/e2e.py` is a verbatim copy of the C++ repository's e2e script
(vendored so CI can run it); `../connect/tests/e2e.py` also passes
unchanged. CI (`.github/workflows/ci.yml`) runs rustfmt, clippy
(`-D warnings`), the unit tests, and the e2e suite on every push.

The git revision is embedded at build time by `build.rs` (`git describe`);
set the `GITVERSION` environment variable to override it when building
without a git checkout.

Command line flags (same as the C++)
------------------------------------

**debug** — dump JSON communications to the console
**allow-remote** — listen on all interfaces instead of localhost
**allow-any-origin** — disable HTTP Origin checking
**port=N** — listen on port N (default 9003)
