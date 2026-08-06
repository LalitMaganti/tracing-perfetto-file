// Copyright 2026 Lalit Maganti
// SPDX-License-Identifier: Apache-2.0

//! Throughput / allocation / size measurements. Run with:
//! `cargo bench --bench perf`
//!
//! Uses a custom harness (no external bench framework) and a counting
//! global allocator to report exact allocations per operation.

use std::alloc::{GlobalAlloc, Layout, System};
use std::io::Write;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use tracing_core::field::{Field, Visit};
use tracing_core::span::{Attributes, Id};
use tracing_core::{Event, Subscriber};
use tracing_perfetto_file::{PerfettoLayer, SpanMode};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

struct CountingAlloc;

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static A: CountingAlloc = CountingAlloc;

/// Discards writes but counts bytes, so we can report trace size.
#[derive(Clone, Default)]
struct NullWriter(std::sync::Arc<AtomicUsize>);

impl Write for NullWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.fetch_add(buf.len(), Ordering::Relaxed);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct Measurement {
    ns_per_op: f64,
    allocs_per_op: f64,
    bytes_per_op: f64,
}

#[derive(Default)]
struct InspectionLayer {
    next_flow_id: AtomicU64,
}

struct InspectionSpan(u64);

struct InspectionVisitor;

impl Visit for InspectionVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        std::hint::black_box(field);
        std::hint::black_box(value);
    }
}

impl<S> Layer<S> for InspectionLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, _attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let flow_id = self.next_flow_id.fetch_add(1, Ordering::Relaxed);
        span.extensions_mut().insert(InspectionSpan(flow_id));
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        std::hint::black_box(span.metadata());
        let extensions = span.extensions();
        if let Some(state) = extensions.get::<InspectionSpan>() {
            std::hint::black_box(state.0);
        }
    }

    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        std::hint::black_box(event.metadata());
        event.record(&mut InspectionVisitor);
    }
}

struct LongDebug;

impl std::fmt::Debug for LongDebug {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const VALUE: &str = concat!(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        );
        f.write_str(VALUE)
    }
}

fn measure(
    iters: u64,
    configure: impl Fn(
        tracing_perfetto_file::PerfettoLayerBuilder,
    ) -> tracing_perfetto_file::PerfettoLayerBuilder,
    op: impl Fn(u64),
) -> Measurement {
    let sink = NullWriter::default();
    let (layer, guard) = configure(PerfettoLayer::builder(sink.clone())).build();
    let subscriber = tracing_subscriber::registry().with(layer);
    let mut result = None;
    tracing::subscriber::with_default(subscriber, || {
        // Warmup: descriptors, interning, buffer growth.
        for i in 0..1000 {
            op(i);
        }
        let allocs_before = ALLOCS.load(Ordering::Relaxed);
        let start = Instant::now();
        for i in 0..iters {
            op(i);
        }
        let elapsed = start.elapsed();
        let allocs = ALLOCS.load(Ordering::Relaxed) - allocs_before;
        result = Some((elapsed, allocs));
    });
    guard.flush().unwrap();
    let (elapsed, allocs) = result.unwrap();
    let written = sink.0.load(Ordering::Relaxed);
    Measurement {
        ns_per_op: elapsed.as_nanos() as f64 / iters as f64,
        allocs_per_op: allocs as f64 / iters as f64,
        bytes_per_op: written as f64 / (iters + 1000) as f64,
    }
}

fn measure_reused_span(
    iters: u64,
    configure: impl Fn(
        tracing_perfetto_file::PerfettoLayerBuilder,
    ) -> tracing_perfetto_file::PerfettoLayerBuilder,
) -> Measurement {
    let sink = NullWriter::default();
    let (layer, guard) = configure(PerfettoLayer::builder(sink.clone())).build();
    let subscriber = tracing_subscriber::registry().with(layer);
    let mut result = None;
    tracing::subscriber::with_default(subscriber, || {
        let span = tracing::info_span!("work");
        for _ in 0..1000 {
            span.in_scope(|| {});
        }
        let allocs_before = ALLOCS.load(Ordering::Relaxed);
        let start = Instant::now();
        for _ in 0..iters {
            span.in_scope(|| {});
        }
        let elapsed = start.elapsed();
        let allocs = ALLOCS.load(Ordering::Relaxed) - allocs_before;
        result = Some((elapsed, allocs));
    });
    guard.flush().unwrap();
    let (elapsed, allocs) = result.unwrap();
    Measurement {
        ns_per_op: elapsed.as_nanos() as f64 / iters as f64,
        allocs_per_op: allocs as f64 / iters as f64,
        bytes_per_op: sink.0.load(Ordering::Relaxed) as f64 / (iters + 1000) as f64,
    }
}

fn measure_inspection(iters: u64, op: impl Fn()) -> Measurement {
    let subscriber = tracing_subscriber::registry().with(InspectionLayer::default());
    let mut result = None;
    tracing::subscriber::with_default(subscriber, || {
        for _ in 0..1000 {
            op();
        }
        let allocs_before = ALLOCS.load(Ordering::Relaxed);
        let start = Instant::now();
        for _ in 0..iters {
            op();
        }
        let elapsed = start.elapsed();
        let allocs = ALLOCS.load(Ordering::Relaxed) - allocs_before;
        result = Some((elapsed, allocs));
    });
    let (elapsed, allocs) = result.unwrap();
    Measurement {
        ns_per_op: elapsed.as_nanos() as f64 / iters as f64,
        allocs_per_op: allocs as f64 / iters as f64,
        bytes_per_op: 0.0,
    }
}

fn measure_inspection_reused_span(iters: u64) -> Measurement {
    let subscriber = tracing_subscriber::registry().with(InspectionLayer::default());
    let mut result = None;
    tracing::subscriber::with_default(subscriber, || {
        let span = tracing::info_span!("work");
        for _ in 0..1000 {
            span.in_scope(|| {});
        }
        let allocs_before = ALLOCS.load(Ordering::Relaxed);
        let start = Instant::now();
        for _ in 0..iters {
            span.in_scope(|| {});
        }
        let elapsed = start.elapsed();
        let allocs = ALLOCS.load(Ordering::Relaxed) - allocs_before;
        result = Some((elapsed, allocs));
    });
    let (elapsed, allocs) = result.unwrap();
    Measurement {
        ns_per_op: elapsed.as_nanos() as f64 / iters as f64,
        allocs_per_op: allocs as f64 / iters as f64,
        bytes_per_op: 0.0,
    }
}

fn report(name: &str, m: &Measurement) {
    println!(
        "{name:<44} {:>8.0} ns/op {:>7.2} allocs/op {:>8.1} bytes/op",
        m.ns_per_op, m.allocs_per_op, m.bytes_per_op
    );
}

fn peak_rss_kb() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status
        .lines()
        .find(|l| l.starts_with("VmHWM:"))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}

fn main() {
    const ITERS: u64 = 200_000;

    println!("== single thread ==");
    let m = measure(
        ITERS,
        |b| b.span_mode(SpanMode::ThreadTracks),
        |_| {
            tracing::info!("tick");
        },
    );
    report("event, bare", &m);
    let m = measure_inspection(ITERS, || tracing::info!("tick"));
    report("inspection floor, event", &m);

    let m = measure(
        ITERS,
        |b| b.span_mode(SpanMode::ThreadTracks).with_counters(),
        |i| tracing::info!(counter.ticks = i, "tick"),
    );
    report("event + counter sample", &m);

    let m = measure(
        ITERS,
        |b| b.span_mode(SpanMode::ThreadTracks).with_debug_annotations(),
        |i| {
            tracing::info!(index = i, state = "running", "tick");
        },
    );
    report("event, 3 annotations", &m);

    let m = measure(
        ITERS,
        |b| b.span_mode(SpanMode::ThreadTracks).with_debug_annotations(),
        |_| {
            tracing::info!(payload = ?LongDebug, "tick");
        },
    );
    report("event, 256-byte Debug annotation", &m);

    let m = measure(
        ITERS,
        |b| {
            b.span_mode(SpanMode::ThreadTracks)
                .with_debug_annotations()
                .with_source_locations()
        },
        |i| {
            tracing::info!(index = i, state = "running", "tick");
        },
    );
    report("event, annotations + source location", &m);

    let m = measure(
        ITERS,
        |b| b.span_mode(SpanMode::ThreadTracks),
        |_| {
            tracing::info_span!("work").in_scope(|| {});
        },
    );
    report("span enter+exit, ThreadTracks", &m);

    let m = measure(
        ITERS,
        |b| b.span_mode(SpanMode::ThreadTracks),
        |_| {
            let _span = tracing::info_span!("work");
        },
    );
    report("span create+drop, ThreadTracks", &m);

    let m = measure_inspection(ITERS, || {
        let _span = tracing::info_span!("work");
    });
    report("inspection floor, span create+drop", &m);

    let m = measure_reused_span(ITERS, |b| b.span_mode(SpanMode::ThreadTracks));
    report("reused span enter+exit, ThreadTracks", &m);
    let m = measure_inspection_reused_span(ITERS);
    report("inspection floor, reused enter+exit", &m);

    let m = measure(
        ITERS,
        |b| b.span_mode(SpanMode::ThreadTracks),
        |_| {
            let producer = tracing::info_span!("produce");
            let consumer = tracing::info_span!("consume");
            consumer.follows_from(producer.id());
            producer.in_scope(|| {});
            consumer.in_scope(|| {});
        },
    );
    report("two spans + follows_from, ThreadTracks", &m);

    let m = measure(
        ITERS,
        |b| b.span_mode(SpanMode::ThreadTracks).with_debug_annotations(),
        |i| {
            tracing::info_span!("work", index = i).in_scope(|| {});
        },
    );
    report("span enter+exit +1 field, ThreadTracks", &m);

    let m = measure(
        ITERS,
        |b| b.span_mode(SpanMode::Both),
        |_| {
            tracing::info_span!("work").in_scope(|| {});
        },
    );
    report("span enter+exit, Both", &m);

    let m = measure(
        ITERS,
        |b| b,
        |_| {
            tracing::info_span!("work").in_scope(|| {});
        },
    );
    report("span enter+exit, SpanTracks (no polls)", &m);

    println!("== 4 threads, contended ==");
    let sink = NullWriter::default();
    let (layer, guard) = PerfettoLayer::builder(sink.clone())
        .span_mode(SpanMode::ThreadTracks)
        .build();
    let subscriber = tracing_subscriber::registry().with(layer);
    let dispatch = tracing_core::Dispatch::new(subscriber);
    let start = Instant::now();
    let threads: Vec<_> = (0..4)
        .map(|_| {
            let dispatch = dispatch.clone();
            std::thread::spawn(move || {
                tracing::dispatcher::with_default(&dispatch, || {
                    for _ in 0..ITERS {
                        tracing::info!("tick");
                    }
                })
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    let elapsed = start.elapsed();
    guard.flush().unwrap();
    println!(
        "4x{ITERS} events: {:.0} ns/op per thread, {:.1}M events/s aggregate",
        elapsed.as_nanos() as f64 / ITERS as f64,
        (4 * ITERS) as f64 / elapsed.as_secs_f64() / 1e6
    );

    let sink = NullWriter::default();
    let (layer, guard) = PerfettoLayer::builder(sink).build();
    let subscriber = tracing_subscriber::registry().with(layer);
    let dispatch = tracing_core::Dispatch::new(subscriber);
    let start = Instant::now();
    let threads: Vec<_> = (0..4)
        .map(|_| {
            let dispatch = dispatch.clone();
            std::thread::spawn(move || {
                tracing::dispatcher::with_default(&dispatch, || {
                    for _ in 0..ITERS {
                        tracing::info_span!("work").in_scope(|| {});
                    }
                })
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    let elapsed = start.elapsed();
    guard.flush().unwrap();
    println!(
        "4x{ITERS} SpanTracks spans: {:.0} ns/op per thread, {:.1}M spans/s aggregate",
        elapsed.as_nanos() as f64 / ITERS as f64,
        (4 * ITERS) as f64 / elapsed.as_secs_f64() / 1e6
    );

    // Baseline: same loop with no subscriber at all.
    let start = Instant::now();
    for _ in 0..ITERS {
        tracing::info!("tick");
    }
    let disabled = start.elapsed();
    println!(
        "baseline disabled event (no subscriber): {:.1} ns/op",
        disabled.as_nanos() as f64 / ITERS as f64
    );

    if let Some(kb) = peak_rss_kb() {
        println!("peak RSS: {} KiB", kb);
    }
}
