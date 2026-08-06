<!-- Copyright 2026 Lalit Maganti -->
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Performance

## What is being measured?

Recording a trace takes some time away from the application doing the recording. These benchmarks
measure that added work by repeating a small tracing operation many times and calculating the
average cost of one operation.

The reported values mean:

- **Time** is how long the application spends recording one operation. Measured in nanoseconds,
  lower is better.
- **Allocations** is the number of new Rust heap allocations made while recording one operation.
  Zero means the operation reused memory that had already been prepared.
- **Trace bytes** is the operation's approximate contribution to the completed trace file. It is a
  storage-size measurement, not a measure of how much information every format retains.

The workloads use ordinary `tracing` instrumentation:

- An **event** is a single record, similar to a log message.
- A **structured event** is an event with named fields such as an index and state.
- A **span** measures an interval of work. The benchmark creates it, enters it, exits it, and closes
  it.
- A **span with event** records an event while inside the span.
- A **four-thread** workload has four threads recording the same operation concurrently. Its time
  is the total elapsed time divided by all operations completed by the four threads.

The comparison tables measure time spent recording while the application is running. They exclude
the final shutdown flush, but include output work triggered during recording.

These results were collected in release mode on a 6-core/12-thread Intel
Xeon Ice Lake virtual machine running Linux.

## Crate benchmark

Run:

```console
cargo bench --bench perf
```

A representative run produced:

| Workload                                   |   Time | Allocations | Trace bytes |
| ------------------------------------------ | -----: | ----------: | ----------: |
| Bare event                                 |  76 ns |           0 |        44 B |
| Event with counter                         |  87 ns |           0 |        60 B |
| Event with three annotations               |  91 ns |           0 |        70 B |
| Event with annotations and source location |  94 ns |           0 |        74 B |
| Span, `SpanTracks`                         | 240 ns |           0 |        80 B |
| Span, `ThreadTracks`                       | 295 ns |           0 |        69 B |
| Reused span enter/exit, `ThreadTracks`     | 149 ns |           0 |        68 B |
| Span, `Both`                               | 395 ns |           0 |       148 B |

Notes:

- Four-thread throughput was 30.2 million events/s and 7.9 million SpanTracks spans/s.
- Allocation counts include all Rust allocations during the measured operation after warmup; native allocations are not visible.

## Layer comparison

```console
cargo bench --manifest-path benchmarks/compare/Cargo.toml --bench compare
```

Time spent recording each operation, in nanoseconds:

| `tracing-subscriber` layer                                                       | Event | Structured event |  Span | Span + field | Span + event | 4-thread event | 4-thread span |
| -------------------------------------------------------------------------------- | ----: | ---------------: | ----: | -----------: | -----------: | -------------: | ------------: |
| `tracing-perfetto-file`                                                          |    74 |              101 |   239 |          252 |          407 |             39 |           125 |
| Connected [`tracing-tracy`](https://crates.io/crates/tracing-tracy)              |    77 |              115 |   310 |          400 |          399 |             53 |           141 |
| Official [`tracing-perfetto-sdk`](https://crates.io/crates/tracing-perfetto-sdk) |   574 |              895 |   769 |          866 |        1,376 |          1,203 |         1,337 |
| [`tracing-chrome`](https://crates.io/crates/tracing-chrome)                      |   353 |            1,549 | 1,123 |        1,838 |        1,536 |             98 |           235 |
| Modal [`SdkLayer`](https://crates.io/crates/tracing-perfetto-sdk-layer)          |   642 |              738 | 1,588 |        1,597 |        2,209 |            209 |           522 |
| [`tracing-perfetto`](https://crates.io/crates/tracing-perfetto)                  |   941 |            1,278 | 1,921 |        2,036 |        2,779 |            288 |           574 |
| Modal [`NativeLayer`](https://crates.io/crates/tracing-perfetto-sdk-layer)       | 1,133 |            1,408 | 2,787 |        2,970 |        4,195 |            333 |           816 |

Notes:

- This crate uses its default SpanTracks mode; `tracing-chrome` uses threaded mode and Modal `NativeLayer` uses async mode.
- The official SDK layer uses an active 128 MiB in-process session. Modal `SdkLayer` uses the C++ SDK; `NativeLayer` is Modal's Rust protobuf writer.
- Tracy uses an attached 0.13.1 capture process and the timer-fallback clock.
- Span workloads contain root spans. Similar-looking slices do not imply equivalent support for explicit parent tracks, cross-thread migration, or `follows_from` flows.
- Unsupported workloads are reported as unsupported by the harness rather than replaced with disabled or dropped work.

## Direct instrumentation APIs

Direct APIs avoid the `tracing` registry and callsite machinery. They provide lower-level context but do not implement the same interface or retain the same information.

| Workload          | This crate | Raw `perfetto-sdk` | `perfetto-recorder` |
| ----------------- | ---------: | -----------------: | ------------------: |
| Event             |      84 ns |             151 ns |         unsupported |
| Structured event  |      98 ns |             383 ns |         unsupported |
| Span              |     290 ns |             246 ns |      88 ns (667 ns) |
| Span with field   |     274 ns |             403 ns |      98 ns (771 ns) |
| Four-thread event |      38 ns |             272 ns |         unsupported |
| Four-thread span  |     132 ns |             422 ns |      23 ns (542 ns) |

Notes:

- This crate is included as a `tracing-subscriber` reference and uses its default SpanTracks mode.
- Raw `perfetto-sdk` uses an active 128 MiB in-process session and emits thread-track slices.
- `perfetto-recorder` records source locations, supports only the listed span workloads, and defers protobuf construction. Parenthesized values include deferred construction and encoding.
- `perfetto-recorder` stores each thread's events in a growing `Vec`, then builds the complete protobuf trace in memory before writing it; neither stage has a size limit.

## Running optional backends

```console
# Direct perfetto-recorder and raw Perfetto Rust SDK APIs
cargo bench --manifest-path benchmarks/compare/Cargo.toml \
  --features compare-direct --bench compare

# tracing-perfetto-sdk with an active native Perfetto SDK session
cargo bench --manifest-path benchmarks/compare/Cargo.toml \
  --features compare-native --bench compare

# Modal NativeLayer; requires protoc
cargo bench --manifest-path benchmarks/compare/Cargo.toml \
  --features compare-modal --bench compare

# Connected Tracy 0.13.1 capture
TRACY_CAPTURE=/path/to/tracy-capture \
  cargo bench --manifest-path benchmarks/compare/Cargo.toml \
  --features compare-tracy --bench compare

# Modal C++ SdkLayer; kept separate and requires protoc
cargo bench --manifest-path benchmarks/compare/Cargo.toml \
  --features compare-modal-sdk --bench compare
```

## Methodology

- Every backend/workload pair runs in a fresh process to isolate global state, thread locals, background writers, and allocator state.
- `hot ns` excludes final flushing. `drain ns` includes final flushing, writer shutdown, and capture completion.
- Allocation sampling runs outside the timed interval, avoiding measurement contention for allocation-heavy backends.
- Native Perfetto SDK measurements use an active in-process trace session; they do not measure the disabled category check.
- `perfetto-recorder` hot time covers event retention; drain time also covers deferred protobuf construction and encoding.
- Tracy measurements begin only after `tracy-capture` has connected.
- Output bytes measure the final artifact's storage cost. Perfetto uses protobuf, Chrome and fmt use text, and Tracy uses a compressed native container, so byte counts do not compare raw encoding efficiency.
- Backends differ in span semantics and retained metadata. The harness records those capabilities and does not substitute a no-op for unsupported behavior.

Set `COMPARE_ITERS` to override the default 100,000 measured operations. The exact workloads and backend configurations are in `benchmarks/compare/benches/compare.rs`.
