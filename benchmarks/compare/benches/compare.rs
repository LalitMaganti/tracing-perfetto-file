// Copyright 2026 Lalit Maganti
// SPDX-License-Identifier: Apache-2.0

//! Cross-backend tracing-layer comparison.
//!
//! Run every supported combination:
//! `cargo bench --bench compare`
//!
//! Run one isolated combination:
//! `cargo bench --bench compare -- --child ours-span span`
//!
//! Include direct `perfetto-recorder` and raw Perfetto Rust SDK calls:
//! `cargo bench --features compare-direct --bench compare`
//!
//! Include the Perfetto SDK tracing layer (large vendored C++ build):
//! `cargo bench --features compare-native --bench compare`
//!
//! Include Modal's native writer (requires `protoc`):
//! `cargo bench --features compare-modal --bench compare`
//!
//! Include Tracy with a real capture process:
//! `TRACY_CAPTURE=/path/to/tracy-capture cargo bench --features compare-tracy --bench compare`
//!
//! Include Modal's C++ SDK layer in its own feature build (requires `protoc`):
//! `cargo bench --features compare-modal-sdk --bench compare`

use std::alloc::{GlobalAlloc, Layout, System};
use std::io::Write;
use std::process::Command;
#[cfg(feature = "compare-tracy")]
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use tracing_core::Dispatch;
use tracing_perfetto_file::{PerfettoLayer, SpanMode};
use tracing_subscriber::layer::SubscriberExt;

#[cfg(feature = "compare-raw-sdk")]
perfetto_sdk::track_event_categories! {
    pub mod raw_perfetto_categories {
        ("tracing", "Benchmark tracing category", []),
    }
}
#[cfg(feature = "compare-raw-sdk")]
use raw_perfetto_categories as perfetto_te_ns;

struct CountingAlloc;

static TRACK_ALLOCATIONS: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if TRACK_ALLOCATIONS.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if TRACK_ALLOCATIONS.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

#[derive(Clone, Debug, Default)]
struct CountingWriter(Arc<AtomicUsize>);

impl CountingWriter {
    fn bytes(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

impl Write for CountingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.fetch_add(buf.len(), Ordering::Relaxed);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SpanSemantics {
    EnterExit,
    Lifetime,
}

struct Backend {
    dispatch: Dispatch,
    finish: Option<Box<dyn FnOnce()>>,
    writer: CountingWriter,
    span_semantics: SpanSemantics,
    root_events: bool,
    nested_events: bool,
    fields: bool,
}

impl Backend {
    fn finish(mut self) -> usize {
        drop(self.dispatch);
        if let Some(finish) = self.finish.take() {
            finish();
        }
        self.writer.bytes()
    }
}

#[cfg(feature = "compare-tracy")]
fn setup_tracy_backend(writer: CountingWriter) -> Backend {
    let layer = tracing_tracy::TracyLayer::default();
    let dispatch = Dispatch::new(tracing_subscriber::registry().with(layer));
    let capture_executable = std::env::var_os("TRACY_CAPTURE")
        .expect("compare-tracy requires TRACY_CAPTURE=/path/to/tracy-capture");
    let capture_path = std::env::temp_dir().join(format!(
        "tracing-layer-comparison-{}.tracy",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&capture_path);
    let mut capture = Command::new(capture_executable)
        .args([
            "-o",
            capture_path.to_str().unwrap(),
            "-a",
            "127.0.0.1",
            "-f",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to start Tracy capture process");

    let deadline = Instant::now() + Duration::from_secs(10);
    while !tracing_tracy::client::Client::is_connected() {
        if let Some(status) = capture.try_wait().expect("failed to poll Tracy capture") {
            panic!("Tracy capture exited before connecting: {status}");
        }
        if Instant::now() >= deadline {
            let _ = capture.kill();
            panic!("Tracy capture did not connect within 10 seconds");
        }
        std::thread::sleep(Duration::from_millis(1));
    }

    let output = writer.clone();
    Backend {
        dispatch,
        finish: Some(Box::new(move || {
            // The dispatch and all worker threads are gone before this callback runs.
            unsafe {
                tracing_tracy::client::sys::___tracy_shutdown_profiler();
            }
            let status = capture.wait().expect("failed to wait for Tracy capture");
            assert!(status.success(), "Tracy capture failed: {status}");
            let bytes = std::fs::metadata(&capture_path).unwrap().len() as usize;
            output.0.store(bytes, Ordering::Relaxed);
            std::fs::remove_file(capture_path).unwrap();
        })),
        writer,
        span_semantics: SpanSemantics::EnterExit,
        root_events: true,
        nested_events: true,
        fields: true,
    }
}

#[cfg(any(feature = "compare-native", feature = "compare-raw-sdk"))]
fn native_trace_config() -> Vec<u8> {
    use perfetto_sdk::heap_buffer::HeapBuffer;
    use perfetto_sdk::pb_msg::{PbMsg, PbMsgWriter};
    use perfetto_sdk::protos::config::{
        data_source_config::DataSourceConfig,
        trace_config::{TraceConfig, TraceConfigBufferConfig, TraceConfigDataSource},
        track_event::track_event_config::TrackEventConfig,
    };

    let writer = PbMsgWriter::new();
    let heap_buffer = HeapBuffer::new(writer.stream_writer());
    let mut message = PbMsg::new(&writer).unwrap();
    {
        let mut config = TraceConfig { msg: &mut message };
        config.set_buffers(|buffer: &mut TraceConfigBufferConfig| {
            buffer.set_size_kb(128 * 1024);
        });
        config.set_data_sources(|sources: &mut TraceConfigDataSource| {
            sources.set_config(|source: &mut DataSourceConfig| {
                source.set_name("track_event");
                source.set_track_event_config(|events: &mut TrackEventConfig| {
                    events.set_enabled_categories("tracing");
                });
            });
        });
    }
    message.finalize();
    let mut bytes = vec![0; writer.stream_writer().get_written_size()];
    heap_buffer.copy_into(&mut bytes);
    bytes
}

#[cfg(feature = "compare-native")]
fn setup_native_backend(writer: CountingWriter, structured: bool) -> Backend {
    tracing_perfetto_sdk::init_in_process();
    let mut session = perfetto_sdk::tracing_session::TracingSession::in_process().unwrap();
    session.setup(&native_trace_config());
    session.start_blocking();

    let layer = if structured {
        tracing_perfetto_sdk::PerfettoLayer::new()
    } else {
        tracing_perfetto_sdk::PerfettoLayer::without_debug_annotations()
    };
    let dispatch = Dispatch::new(tracing_subscriber::registry().with(layer));
    let output = writer.clone();
    Backend {
        dispatch,
        finish: Some(Box::new(move || {
            session.flush_blocking(Duration::from_secs(5));
            session.stop_blocking();
            session.read_trace_blocking(move |data, _| {
                output.0.fetch_add(data.len(), Ordering::Relaxed);
            });
        })),
        writer,
        span_semantics: SpanSemantics::EnterExit,
        root_events: true,
        nested_events: true,
        fields: true,
    }
}

fn setup_backend(name: &str, structured: bool) -> Option<Backend> {
    let writer = CountingWriter::default();
    match name {
        "ours-thread" => {
            let mut builder =
                PerfettoLayer::builder(writer.clone()).span_mode(SpanMode::ThreadTracks);
            if structured {
                builder = builder.with_debug_annotations().with_source_locations();
            }
            let (layer, guard) = builder.build();
            let dispatch = Dispatch::new(tracing_subscriber::registry().with(layer));
            Some(Backend {
                dispatch,
                finish: Some(Box::new(move || guard.flush().unwrap())),
                writer,
                span_semantics: SpanSemantics::EnterExit,
                root_events: true,
                nested_events: true,
                fields: true,
            })
        }
        "ours-span" => {
            let mut builder = PerfettoLayer::builder(writer.clone());
            if structured {
                builder = builder.with_debug_annotations().with_source_locations();
            }
            let (layer, guard) = builder.build();
            let dispatch = Dispatch::new(tracing_subscriber::registry().with(layer));
            Some(Backend {
                dispatch,
                finish: Some(Box::new(move || guard.flush().unwrap())),
                writer,
                span_semantics: SpanSemantics::Lifetime,
                root_events: true,
                nested_events: true,
                fields: true,
            })
        }
        "fmt" => {
            let output = writer.clone();
            let layer = tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_file(structured)
                .with_line_number(structured)
                .with_span_events(
                    tracing_subscriber::fmt::format::FmtSpan::ENTER
                        | tracing_subscriber::fmt::format::FmtSpan::EXIT,
                )
                .with_writer(move || output.clone());
            let dispatch = Dispatch::new(tracing_subscriber::registry().with(layer));
            Some(Backend {
                dispatch,
                finish: None,
                writer,
                span_semantics: SpanSemantics::EnterExit,
                root_events: true,
                nested_events: true,
                fields: true,
            })
        }
        #[cfg(feature = "compare-tracy")]
        "tracy" => Some(setup_tracy_backend(writer)),
        "tracing-perfetto" => {
            let layer = tracing_perfetto::PerfettoLayer::new(std::sync::Mutex::new(writer.clone()))
                .with_debug_annotations(structured);
            let dispatch = Dispatch::new(tracing_subscriber::registry().with(layer));
            Some(Backend {
                dispatch,
                finish: None,
                writer,
                span_semantics: SpanSemantics::Lifetime,
                root_events: true,
                nested_events: true,
                fields: true,
            })
        }
        #[cfg(feature = "compare-modal")]
        "modal-native-sync" | "modal-native-async" => {
            let flavor = if name == "modal-native-sync" {
                tracing_perfetto_sdk_layer::Flavor::Sync
            } else {
                tracing_perfetto_sdk_layer::Flavor::Async
            };
            let output = writer.clone();
            let layer =
                tracing_perfetto_sdk_layer::NativeLayer::from_config_bytes(&[], move || {
                    output.clone()
                })
                .with_force_flavor(Some(flavor))
                .build()
                .unwrap();
            let flusher = layer.clone();
            let dispatch = Dispatch::new(tracing_subscriber::registry().with(layer));
            Some(Backend {
                dispatch,
                finish: Some(Box::new(move || {
                    flusher
                        .flush(Duration::from_secs(1), Duration::from_secs(1))
                        .unwrap();
                    flusher.stop().unwrap();
                })),
                writer,
                span_semantics: if flavor == tracing_perfetto_sdk_layer::Flavor::Sync {
                    SpanSemantics::EnterExit
                } else {
                    SpanSemantics::Lifetime
                },
                root_events: true,
                nested_events: true,
                fields: true,
            })
        }
        #[cfg(feature = "compare-modal-sdk")]
        "modal-sdk" => {
            // TraceConfig { buffers: [{ size_kb: 131072 }],
            //               data_sources: [{ config: { name: "rust_tracing" } }] }
            const CONFIG: &[u8] = b"\x0a\x04\x08\x80\x80\x08\x12\x10\x0a\x0e\x0a\x0crust_tracing";
            let output_path = std::env::temp_dir().join(format!(
                "modal-sdk-comparison-{}.pftrace",
                std::process::id()
            ));
            let _ = std::fs::remove_file(&output_path);
            let file = std::fs::File::create(&output_path).unwrap();
            let layer = tracing_perfetto_sdk_layer::SdkLayer::from_config_bytes(CONFIG, Some(file))
                .build()
                .unwrap();
            let flusher = layer.clone();
            let output = writer.clone();
            let dispatch = Dispatch::new(tracing_subscriber::registry().with(layer));
            Some(Backend {
                dispatch,
                finish: Some(Box::new(move || {
                    flusher.flush(Duration::from_secs(5)).unwrap();
                    flusher.stop().unwrap();
                    let bytes = std::fs::metadata(&output_path).unwrap().len() as usize;
                    output.0.store(bytes, Ordering::Relaxed);
                    drop(flusher);
                    std::fs::remove_file(output_path).unwrap();
                })),
                writer,
                span_semantics: SpanSemantics::Lifetime,
                root_events: true,
                nested_events: true,
                fields: true,
            })
        }
        "perfetto-writer" => {
            let layer = tracing_perfetto_writer::PerfettoLayer::new(writer.clone());
            let flusher = layer.clone();
            let dispatch = Dispatch::new(tracing_subscriber::registry().with(layer));
            Some(Backend {
                dispatch,
                finish: Some(Box::new(move || flusher.flush().unwrap())),
                writer,
                span_semantics: SpanSemantics::Lifetime,
                // This layer intentionally ignores events outside a span.
                root_events: false,
                nested_events: true,
                fields: true,
            })
        }
        "chrome-thread" => {
            let (layer, guard) = tracing_chrome::ChromeLayerBuilder::new()
                .include_args(structured)
                .include_locations(structured)
                .writer(writer.clone())
                .trace_style(tracing_chrome::TraceStyle::Threaded)
                .build();
            let dispatch = Dispatch::new(tracing_subscriber::registry().with(layer));
            Some(Backend {
                dispatch,
                finish: Some(Box::new(move || drop(guard))),
                writer,
                span_semantics: SpanSemantics::EnterExit,
                root_events: true,
                nested_events: true,
                fields: true,
            })
        }
        "chrome-async" => {
            let (layer, guard) = tracing_chrome::ChromeLayerBuilder::new()
                .include_args(structured)
                .include_locations(structured)
                .writer(writer.clone())
                .trace_style(tracing_chrome::TraceStyle::Async)
                .build();
            let dispatch = Dispatch::new(tracing_subscriber::registry().with(layer));
            Some(Backend {
                dispatch,
                finish: Some(Box::new(move || drop(guard))),
                writer,
                span_semantics: SpanSemantics::Lifetime,
                root_events: true,
                nested_events: true,
                fields: true,
            })
        }
        #[cfg(feature = "compare-native")]
        "tracing-perfetto-sdk" => Some(setup_native_backend(writer, structured)),
        "flame" => {
            let layer = tracing_flame::FlameLayer::new(writer.clone())
                .with_module_path(false)
                .with_file_and_line(false);
            let guard = layer.flush_on_drop();
            let dispatch = Dispatch::new(tracing_subscriber::registry().with(layer));
            Some(Backend {
                dispatch,
                finish: Some(Box::new(move || guard.flush().unwrap())),
                writer,
                span_semantics: SpanSemantics::EnterExit,
                root_events: false,
                nested_events: false,
                fields: false,
            })
        }
        _ => None,
    }
}

#[derive(Clone, Copy)]
enum Workload {
    Event,
    EventFields,
    Span,
    SpanFields,
    SpanEvent,
    ReusedSpan,
    MultiEvent,
    MultiSpan,
}

impl Workload {
    fn parse(name: &str) -> Option<Self> {
        match name {
            "event" => Some(Self::Event),
            "event-fields" => Some(Self::EventFields),
            "span" => Some(Self::Span),
            "span-fields" => Some(Self::SpanFields),
            "span-event" => Some(Self::SpanEvent),
            "reused-span" => Some(Self::ReusedSpan),
            "mt-event" => Some(Self::MultiEvent),
            "mt-span" => Some(Self::MultiSpan),
            _ => None,
        }
    }

    fn structured(self) -> bool {
        matches!(self, Self::EventFields | Self::SpanFields)
    }

    fn is_event(self) -> bool {
        matches!(self, Self::Event | Self::EventFields | Self::MultiEvent)
    }

    fn is_reused(self) -> bool {
        matches!(self, Self::ReusedSpan)
    }

    fn is_multi(self) -> bool {
        matches!(self, Self::MultiEvent | Self::MultiSpan)
    }
}

struct Measurement {
    hot: Duration,
    end_to_end: Duration,
    measured_ops: u64,
    total_ops: u64,
    allocation_ops: u64,
    allocs: u64,
    alloc_bytes: u64,
    output_bytes: usize,
}

fn event_ops(count: u64) {
    for _ in 0..count {
        tracing::info!("tick");
    }
}

fn event_field_ops(count: u64) {
    for i in 0..count {
        tracing::info!(index = i, state = "running", "tick");
    }
}

fn span_ops(count: u64) {
    for _ in 0..count {
        tracing::info_span!("work").in_scope(|| {});
    }
}

fn span_field_ops(count: u64) {
    for i in 0..count {
        tracing::info_span!("work", index = i).in_scope(|| {});
    }
}

fn span_event_ops(count: u64) {
    for _ in 0..count {
        tracing::info_span!("work").in_scope(|| tracing::info!("tick"));
    }
}

fn operation(workload: Workload) -> fn(u64) {
    match workload {
        Workload::Event | Workload::MultiEvent => event_ops,
        Workload::EventFields => event_field_ops,
        Workload::Span | Workload::MultiSpan => span_ops,
        Workload::SpanFields => span_field_ops,
        Workload::SpanEvent => span_event_ops,
        Workload::ReusedSpan => unreachable!(),
    }
}

fn measure_single(backend: Backend, workload: Workload, iters: u64) -> Measurement {
    const WARMUP: u64 = 5_000;
    let allocation_ops = iters.min(10_000);
    let mut measured_start = None;
    let mut hot = Duration::ZERO;
    let mut allocs = 0;
    let mut alloc_bytes = 0;

    tracing::dispatcher::with_default(&backend.dispatch, || {
        if workload.is_reused() {
            let span = tracing::info_span!("work");
            for _ in 0..WARMUP {
                span.in_scope(|| {});
            }
            let allocs_before = ALLOCS.load(Ordering::Relaxed);
            let alloc_bytes_before = ALLOC_BYTES.load(Ordering::Relaxed);
            TRACK_ALLOCATIONS.store(true, Ordering::Relaxed);
            for _ in 0..allocation_ops {
                span.in_scope(|| {});
            }
            TRACK_ALLOCATIONS.store(false, Ordering::Relaxed);
            allocs = ALLOCS.load(Ordering::Relaxed) - allocs_before;
            alloc_bytes = ALLOC_BYTES.load(Ordering::Relaxed) - alloc_bytes_before;

            let start = Instant::now();
            measured_start = Some(start);
            for _ in 0..iters {
                span.in_scope(|| {});
            }
            hot = start.elapsed();
        } else {
            let op = operation(workload);
            op(WARMUP);
            let allocs_before = ALLOCS.load(Ordering::Relaxed);
            let alloc_bytes_before = ALLOC_BYTES.load(Ordering::Relaxed);
            TRACK_ALLOCATIONS.store(true, Ordering::Relaxed);
            op(allocation_ops);
            TRACK_ALLOCATIONS.store(false, Ordering::Relaxed);
            allocs = ALLOCS.load(Ordering::Relaxed) - allocs_before;
            alloc_bytes = ALLOC_BYTES.load(Ordering::Relaxed) - alloc_bytes_before;

            let start = Instant::now();
            measured_start = Some(start);
            op(iters);
            hot = start.elapsed();
        }
    });

    let output_bytes = backend.finish();
    let end_to_end = measured_start.expect("measurement started").elapsed();
    Measurement {
        hot,
        end_to_end,
        measured_ops: iters,
        total_ops: iters + WARMUP + allocation_ops,
        allocation_ops,
        allocs,
        alloc_bytes,
        output_bytes,
    }
}

fn measure_multi(backend: Backend, workload: Workload, iters: u64) -> Measurement {
    const WARMUP: u64 = 2_000;
    const THREADS: u64 = 4;
    let allocation_ops = iters.min(2_000);
    let barrier = Arc::new(Barrier::new(THREADS as usize + 1));
    let op = operation(workload);
    let mut workers = Vec::with_capacity(THREADS as usize);

    for _ in 0..THREADS {
        let dispatch = backend.dispatch.clone();
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            tracing::dispatcher::with_default(&dispatch, || {
                op(WARMUP);
                barrier.wait();
                barrier.wait();
                op(allocation_ops);
                barrier.wait();
                barrier.wait();
                op(iters);
            });
        }));
    }

    // Wait until every worker has completed warmup before sampling allocations.
    barrier.wait();
    let allocs_before = ALLOCS.load(Ordering::Relaxed);
    let alloc_bytes_before = ALLOC_BYTES.load(Ordering::Relaxed);
    TRACK_ALLOCATIONS.store(true, Ordering::Relaxed);
    barrier.wait();
    barrier.wait();
    TRACK_ALLOCATIONS.store(false, Ordering::Relaxed);
    let allocs = ALLOCS.load(Ordering::Relaxed) - allocs_before;
    let alloc_bytes = ALLOC_BYTES.load(Ordering::Relaxed) - alloc_bytes_before;

    let start = Instant::now();
    barrier.wait();
    for worker in workers {
        worker.join().unwrap();
    }
    let hot = start.elapsed();
    let output_bytes = backend.finish();
    let end_to_end = start.elapsed();
    Measurement {
        hot,
        end_to_end,
        measured_ops: iters * THREADS,
        total_ops: (iters + WARMUP + allocation_ops) * THREADS,
        allocation_ops: allocation_ops * THREADS,
        allocs,
        alloc_bytes,
        output_bytes,
    }
}

fn supported(backend: &Backend, workload: Workload) -> bool {
    if workload.is_event() && !backend.root_events {
        return false;
    }
    if matches!(workload, Workload::SpanEvent) && !backend.nested_events {
        return false;
    }
    if workload.structured() && !backend.fields {
        return false;
    }
    if workload.is_reused() && backend.span_semantics != SpanSemantics::EnterExit {
        return false;
    }
    true
}

fn print_measurement(backend_name: &str, workload_name: &str, measurement: Measurement) {
    let hot_ns = measurement.hot.as_nanos() as f64 / measurement.measured_ops as f64;
    let end_to_end_ns = measurement.end_to_end.as_nanos() as f64 / measurement.measured_ops as f64;
    let allocs = measurement.allocs as f64 / measurement.allocation_ops as f64;
    let alloc_bytes = measurement.alloc_bytes as f64 / measurement.allocation_ops as f64;
    let output_bytes = measurement.output_bytes as f64 / measurement.total_ops as f64;
    println!(
        "{backend_name:<20} {workload_name:<14} {hot_ns:>9.1} {end_to_end_ns:>9.1} \
         {allocs:>9.2} {alloc_bytes:>11.1} {output_bytes:>10.1}",
    );
}

#[cfg(feature = "compare-recorder")]
fn recorder_ops(workload: Workload, count: u64) {
    match workload {
        Workload::Span | Workload::MultiSpan => {
            for _ in 0..count {
                perfetto_recorder::scope!("work");
            }
        }
        Workload::SpanFields => {
            for index in 0..count {
                perfetto_recorder::scope!("work", index = index);
            }
        }
        _ => unreachable!(),
    }
}

#[cfg(feature = "compare-recorder")]
fn recorder_supported(workload: Workload) -> bool {
    matches!(
        workload,
        Workload::Span | Workload::SpanFields | Workload::MultiSpan
    )
}

#[cfg(feature = "compare-recorder")]
fn measure_recorder_single(workload: Workload, iters: u64) -> Measurement {
    const WARMUP: u64 = 5_000;
    let allocation_ops = iters.min(10_000);
    let events_per_op =
        perfetto_recorder::EVENTS_PER_SPAN + usize::from(matches!(workload, Workload::SpanFields));
    perfetto_recorder::current_thread_reserve(
        (WARMUP + allocation_ops + iters) as usize * events_per_op,
    );

    recorder_ops(workload, WARMUP);
    let allocs_before = ALLOCS.load(Ordering::Relaxed);
    let alloc_bytes_before = ALLOC_BYTES.load(Ordering::Relaxed);
    TRACK_ALLOCATIONS.store(true, Ordering::Relaxed);
    recorder_ops(workload, allocation_ops);
    TRACK_ALLOCATIONS.store(false, Ordering::Relaxed);
    let allocs = ALLOCS.load(Ordering::Relaxed) - allocs_before;
    let alloc_bytes = ALLOC_BYTES.load(Ordering::Relaxed) - alloc_bytes_before;

    let start = Instant::now();
    recorder_ops(workload, iters);
    let hot = start.elapsed();
    let thread_data = perfetto_recorder::ThreadTraceData::take_current_thread();
    let mut trace = perfetto_recorder::TraceBuilder::new().unwrap();
    let output_bytes = trace
        .process_thread_data(&thread_data)
        .encode_to_vec()
        .len();
    let end_to_end = start.elapsed();

    Measurement {
        hot,
        end_to_end,
        measured_ops: iters,
        total_ops: WARMUP + allocation_ops + iters,
        allocation_ops,
        allocs,
        alloc_bytes,
        output_bytes,
    }
}

#[cfg(feature = "compare-recorder")]
fn measure_recorder_multi(workload: Workload, iters: u64) -> Measurement {
    const WARMUP: u64 = 2_000;
    const THREADS: u64 = 4;
    let allocation_ops = iters.min(2_000);
    let barrier = Arc::new(Barrier::new(THREADS as usize + 1));
    let mut workers = Vec::with_capacity(THREADS as usize);

    for _ in 0..THREADS {
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            perfetto_recorder::current_thread_reserve(
                (WARMUP + allocation_ops + iters) as usize * perfetto_recorder::EVENTS_PER_SPAN,
            );
            recorder_ops(workload, WARMUP);
            barrier.wait();
            barrier.wait();
            recorder_ops(workload, allocation_ops);
            barrier.wait();
            barrier.wait();
            recorder_ops(workload, iters);
            perfetto_recorder::ThreadTraceData::take_current_thread()
        }));
    }

    barrier.wait();
    let allocs_before = ALLOCS.load(Ordering::Relaxed);
    let alloc_bytes_before = ALLOC_BYTES.load(Ordering::Relaxed);
    TRACK_ALLOCATIONS.store(true, Ordering::Relaxed);
    barrier.wait();
    barrier.wait();
    TRACK_ALLOCATIONS.store(false, Ordering::Relaxed);
    let allocs = ALLOCS.load(Ordering::Relaxed) - allocs_before;
    let alloc_bytes = ALLOC_BYTES.load(Ordering::Relaxed) - alloc_bytes_before;

    let start = Instant::now();
    barrier.wait();
    let thread_data: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    let hot = start.elapsed();
    let mut trace = perfetto_recorder::TraceBuilder::new().unwrap();
    for data in &thread_data {
        trace.process_thread_data(data);
    }
    let output_bytes = trace.encode_to_vec().len();
    let end_to_end = start.elapsed();

    Measurement {
        hot,
        end_to_end,
        measured_ops: iters * THREADS,
        total_ops: (WARMUP + allocation_ops + iters) * THREADS,
        allocation_ops: allocation_ops * THREADS,
        allocs,
        alloc_bytes,
        output_bytes,
    }
}

#[cfg(feature = "compare-recorder")]
fn recorder_child(workload: Workload, workload_name: &str, iters: u64) {
    const NAME: &str = "perfetto-recorder";
    if !recorder_supported(workload) {
        println!("{NAME:<20} {workload_name:<14} unsupported");
        return;
    }
    perfetto_recorder::start().unwrap();
    let measurement = if workload.is_multi() {
        measure_recorder_multi(workload, iters)
    } else {
        measure_recorder_single(workload, iters)
    };
    print_measurement(NAME, workload_name, measurement);
}

#[cfg(feature = "compare-raw-sdk")]
fn raw_sdk_ops(workload: Workload, count: u64) {
    use perfetto_sdk::track_event::{EventContext, TrackEventDebugArg};
    use perfetto_sdk::{track_event_begin, track_event_end, track_event_instant};

    match workload {
        Workload::Event | Workload::MultiEvent => {
            for _ in 0..count {
                track_event_instant!("tracing", "tick");
            }
        }
        Workload::EventFields => {
            for index in 0..count {
                track_event_instant!("tracing", "tick", |ctx: &mut EventContext| {
                    ctx.add_debug_arg("index", TrackEventDebugArg::Uint64(index));
                    ctx.add_debug_arg("state", TrackEventDebugArg::String("running"));
                });
            }
        }
        Workload::Span | Workload::MultiSpan => {
            for _ in 0..count {
                track_event_begin!("tracing", "work");
                track_event_end!("tracing");
            }
        }
        Workload::SpanFields => {
            for index in 0..count {
                track_event_begin!("tracing", "work", |ctx: &mut EventContext| {
                    ctx.add_debug_arg("index", TrackEventDebugArg::Uint64(index));
                });
                track_event_end!("tracing");
            }
        }
        Workload::SpanEvent => {
            for _ in 0..count {
                track_event_begin!("tracing", "work");
                track_event_instant!("tracing", "tick");
                track_event_end!("tracing");
            }
        }
        Workload::ReusedSpan => unreachable!(),
    }
}

#[cfg(feature = "compare-raw-sdk")]
fn setup_raw_sdk_session() -> perfetto_sdk::tracing_session::TracingSession {
    use perfetto_sdk::producer::{Backends, Producer, ProducerInitArgsBuilder};
    use perfetto_sdk::track_event::TrackEvent;

    Producer::init(
        ProducerInitArgsBuilder::new()
            .backends(Backends::IN_PROCESS)
            .build(),
    );
    TrackEvent::init();
    perfetto_te_ns::register().unwrap();
    let mut session = perfetto_sdk::tracing_session::TracingSession::in_process().unwrap();
    session.setup(&native_trace_config());
    session.start_blocking();
    assert!(perfetto_te_ns::is_category_enabled(0));
    session
}

#[cfg(feature = "compare-raw-sdk")]
fn finish_raw_sdk_session(mut session: perfetto_sdk::tracing_session::TracingSession) -> usize {
    session.flush_blocking(Duration::from_secs(5));
    session.stop_blocking();
    let output_bytes = Arc::new(AtomicUsize::new(0));
    let output = Arc::clone(&output_bytes);
    session.read_trace_blocking(move |data, _| {
        output.fetch_add(data.len(), Ordering::Relaxed);
    });
    output_bytes.load(Ordering::Relaxed)
}

#[cfg(feature = "compare-raw-sdk")]
fn measure_raw_sdk_single(workload: Workload, iters: u64) -> Measurement {
    const WARMUP: u64 = 5_000;
    let allocation_ops = iters.min(10_000);
    let session = setup_raw_sdk_session();

    raw_sdk_ops(workload, WARMUP);
    let allocs_before = ALLOCS.load(Ordering::Relaxed);
    let alloc_bytes_before = ALLOC_BYTES.load(Ordering::Relaxed);
    TRACK_ALLOCATIONS.store(true, Ordering::Relaxed);
    raw_sdk_ops(workload, allocation_ops);
    TRACK_ALLOCATIONS.store(false, Ordering::Relaxed);
    let allocs = ALLOCS.load(Ordering::Relaxed) - allocs_before;
    let alloc_bytes = ALLOC_BYTES.load(Ordering::Relaxed) - alloc_bytes_before;

    let start = Instant::now();
    raw_sdk_ops(workload, iters);
    let hot = start.elapsed();
    let output_bytes = finish_raw_sdk_session(session);
    let end_to_end = start.elapsed();

    Measurement {
        hot,
        end_to_end,
        measured_ops: iters,
        total_ops: WARMUP + allocation_ops + iters,
        allocation_ops,
        allocs,
        alloc_bytes,
        output_bytes,
    }
}

#[cfg(feature = "compare-raw-sdk")]
fn measure_raw_sdk_multi(workload: Workload, iters: u64) -> Measurement {
    const WARMUP: u64 = 2_000;
    const THREADS: u64 = 4;
    let allocation_ops = iters.min(2_000);
    let session = setup_raw_sdk_session();
    let barrier = Arc::new(Barrier::new(THREADS as usize + 1));
    let mut workers = Vec::with_capacity(THREADS as usize);

    for _ in 0..THREADS {
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            raw_sdk_ops(workload, WARMUP);
            barrier.wait();
            barrier.wait();
            raw_sdk_ops(workload, allocation_ops);
            barrier.wait();
            barrier.wait();
            raw_sdk_ops(workload, iters);
        }));
    }

    barrier.wait();
    let allocs_before = ALLOCS.load(Ordering::Relaxed);
    let alloc_bytes_before = ALLOC_BYTES.load(Ordering::Relaxed);
    TRACK_ALLOCATIONS.store(true, Ordering::Relaxed);
    barrier.wait();
    barrier.wait();
    TRACK_ALLOCATIONS.store(false, Ordering::Relaxed);
    let allocs = ALLOCS.load(Ordering::Relaxed) - allocs_before;
    let alloc_bytes = ALLOC_BYTES.load(Ordering::Relaxed) - alloc_bytes_before;

    let start = Instant::now();
    barrier.wait();
    for worker in workers {
        worker.join().unwrap();
    }
    let hot = start.elapsed();
    let output_bytes = finish_raw_sdk_session(session);
    let end_to_end = start.elapsed();

    Measurement {
        hot,
        end_to_end,
        measured_ops: iters * THREADS,
        total_ops: (WARMUP + allocation_ops + iters) * THREADS,
        allocation_ops: allocation_ops * THREADS,
        allocs,
        alloc_bytes,
        output_bytes,
    }
}

#[cfg(feature = "compare-raw-sdk")]
fn raw_sdk_child(workload: Workload, workload_name: &str, iters: u64) {
    const NAME: &str = "raw-perfetto-sdk";
    if workload.is_reused() {
        println!("{NAME:<20} {workload_name:<14} unsupported");
        return;
    }
    let measurement = if workload.is_multi() {
        measure_raw_sdk_multi(workload, iters)
    } else {
        measure_raw_sdk_single(workload, iters)
    };
    print_measurement(NAME, workload_name, measurement);
}

fn child(backend_name: &str, workload_name: &str) {
    let workload = Workload::parse(workload_name).expect("unknown workload");
    let iters = std::env::var("COMPARE_ITERS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(100_000);
    #[cfg(feature = "compare-recorder")]
    if backend_name == "perfetto-recorder" {
        recorder_child(workload, workload_name, iters);
        return;
    }
    #[cfg(feature = "compare-raw-sdk")]
    if backend_name == "raw-perfetto-sdk" {
        raw_sdk_child(workload, workload_name, iters);
        return;
    }
    let Some(backend) = setup_backend(backend_name, workload.structured()) else {
        eprintln!("unknown backend: {backend_name}");
        std::process::exit(2);
    };
    if !supported(&backend, workload) {
        println!("{backend_name:<20} {workload_name:<14} unsupported");
        return;
    }

    let measurement = if workload.is_multi() {
        measure_multi(backend, workload, iters)
    } else {
        measure_single(backend, workload, iters)
    };
    print_measurement(backend_name, workload_name, measurement);
}

fn parent() {
    #[allow(unused_mut)]
    let mut backends = vec![
        "ours-thread",
        "ours-span",
        "fmt",
        "tracing-perfetto",
        "perfetto-writer",
        "chrome-thread",
        "chrome-async",
        "flame",
    ];
    #[cfg(feature = "compare-recorder")]
    backends.push("perfetto-recorder");
    #[cfg(feature = "compare-raw-sdk")]
    backends.push("raw-perfetto-sdk");
    #[cfg(feature = "compare-modal")]
    backends.extend(["modal-native-sync", "modal-native-async"]);
    #[cfg(feature = "compare-native")]
    backends.push("tracing-perfetto-sdk");
    #[cfg(feature = "compare-tracy")]
    backends.push("tracy");
    #[cfg(feature = "compare-modal-sdk")]
    backends.push("modal-sdk");
    const WORKLOADS: &[&str] = &[
        "event",
        "event-fields",
        "span",
        "span-fields",
        "span-event",
        "reused-span",
        "mt-event",
        "mt-span",
    ];

    println!(
        "{:<20} {:<14} {:>9} {:>9} {:>9} {:>11} {:>10}",
        "backend", "workload", "hot ns", "drain ns", "allocs", "alloc B", "output B"
    );
    println!("hot ns excludes final flushing; drain ns includes it");
    println!("allocations are sampled separately and do not perturb the timed interval");
    println!("output bytes include warmup and allocation sampling, normalized per operation");
    println!("output bytes measure complete artifacts; formats and compression differ");
    #[cfg(any(feature = "compare-native", feature = "compare-raw-sdk"))]
    println!("Perfetto SDK allocation counts cover Rust allocations only, not C++ allocations");
    #[cfg(feature = "compare-recorder")]
    println!("perfetto-recorder drain time includes deferred protobuf construction and encoding");
    #[cfg(feature = "compare-tracy")]
    println!("tracy is measured only after tracy-capture has connected (timer fallback clock)");
    #[cfg(feature = "compare-tracy")]
    println!("tracy allocation counts cover Rust allocations only, not C++ allocations");
    #[cfg(feature = "compare-modal-sdk")]
    println!("modal-sdk allocation counts cover Rust allocations only, not C++ allocations");
    let executable = std::env::current_exe().unwrap();
    for workload in WORKLOADS {
        println!("\n== {workload} ==");
        for backend in &backends {
            let status = Command::new(&executable)
                .args(["--child", backend, workload])
                .status()
                .unwrap();
            if !status.success() {
                std::process::exit(status.code().unwrap_or(1));
            }
        }
    }
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("--child") {
        let backend = args.get(2).expect("missing backend");
        let workload = args.get(3).expect("missing workload");
        child(backend, workload);
    } else {
        parent();
    }
}
