// Copyright 2026 Lalit Maganti
// SPDX-License-Identifier: Apache-2.0

#![doc = include_str!("../README.md")]
#![deny(missing_docs)]

mod emit;
mod platform;
mod proto;
mod runtime;
mod sequence;
mod span_state;
mod subscriber;
mod thread;
mod visitor;

use std::io::Write;
use std::sync::Arc;

use crate::runtime::{Config, Inner};

/// A [`tracing_subscriber::Layer`] that writes a Perfetto trace stream.
///
/// Construct with [`PerfettoLayer::builder`]; see the crate docs for usage.
#[must_use = "the layer must be installed on a tracing subscriber"]
pub struct PerfettoLayer {
    inner: Arc<Inner>,
}

impl PerfettoLayer {
    /// Starts building a layer that writes the trace to `writer`.
    ///
    /// The writer may be any owned [`Write`] sink, not just a file. For
    /// example, a TCP connection can stream trace data to a collector:
    ///
    /// ```no_run
    /// # use tracing_perfetto_file::PerfettoLayer;
    /// let stream = std::net::TcpStream::connect("127.0.0.1:9001")?;
    /// let (layer, guard) = PerfettoLayer::builder(stream).build();
    /// # let _ = (layer, guard);
    /// # Ok::<(), std::io::Error>(())
    /// ```
    ///
    /// Slices, instants, flows, poll linking, levels, and targets are always
    /// on. Additional data is opt-in through the builder's `with_*` methods.
    pub fn builder(writer: impl Write + Send + 'static) -> PerfettoLayerBuilder {
        PerfettoLayerBuilder {
            writer: Box::new(writer),
            config: Config {
                span_mode: SpanMode::default(),
                debug_annotations: false,
                poll_slices: false,
                source_locations: false,
                counters: false,
            },
        }
    }
}

/// Configures and builds a [`PerfettoLayer`].
#[must_use = "the builder does nothing until build is called"]
pub struct PerfettoLayerBuilder {
    writer: Box<dyn Write + Send>,
    config: Config,
}

impl PerfettoLayerBuilder {
    /// Selects how spans map onto tracks. Default: [`SpanMode::SpanTracks`].
    pub fn span_mode(mut self, mode: SpanMode) -> Self {
        self.config.span_mode = mode;
        self
    }

    /// Records span and event fields as Perfetto debug annotations, visible
    /// when selecting a slice in the UI.
    pub fn with_debug_annotations(mut self) -> Self {
        self.config.debug_annotations = true;
        self
    }

    /// SpanTracks only: emits a nested `poll` slice per enter->exit interval
    /// inside each span's lifetime slice.
    pub fn with_poll_slices(mut self) -> Self {
        self.config.poll_slices = true;
        self
    }

    /// Records each span's and event's file and line as a Perfetto source
    /// location.
    pub fn with_source_locations(mut self) -> Self {
        self.config.source_locations = true;
        self
    }

    /// Maps numeric event fields named `counter.*` to Perfetto counter
    /// tracks instead of debug annotations, e.g.
    /// `tracing::info!(counter.queue_depth = 42)` plots `queue_depth` as a
    /// per-process counter.
    pub fn with_counters(mut self) -> Self {
        self.config.counters = true;
        self
    }

    /// Builds the layer. Keep the returned [`FlushGuard`] alive for the
    /// duration of tracing; dropping it flushes all buffered data.
    pub fn build(self) -> (PerfettoLayer, FlushGuard) {
        let inner = Arc::new(Inner::new(self.writer, self.config));
        (
            PerfettoLayer {
                inner: Arc::clone(&inner),
            },
            FlushGuard { inner },
        )
    }
}

/// How spans are mapped onto Perfetto tracks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum SpanMode {
    /// Both views at once. Every span gets its own track with a slice covering
    /// the span's whole lifetime and each enter -> exit interval additionally
    /// becomes a slice on the thread's track.
    Both,
    /// Only the thread-track view: each enter -> exit interval becomes a slice
    /// on the entering thread's track. Cheapest; async spans show one slice
    /// per poll.
    ThreadTracks,
    /// The default: a lifetime slice per span, parented on the enclosing
    /// span's track, optionally with nested `poll` slices per enter -> exit
    /// interval (see [`PerfettoLayerBuilder::with_poll_slices`]).
    #[default]
    SpanTracks,
}

impl SpanMode {
    /// Whether enter -> exit intervals are emitted on the thread tracks.
    fn emit_thread_slices(self) -> bool {
        matches!(self, SpanMode::ThreadTracks | SpanMode::Both)
    }

    /// Whether spans get their own track with a lifetime slice.
    fn emit_span_slices(self) -> bool {
        matches!(self, SpanMode::SpanTracks | SpanMode::Both)
    }
}

/// Flushes buffered trace data when dropped.
///
/// Trace bytes accumulate in per-thread queues and only reach the output
/// writer when a queue fills, a thread exits, or the trace is flushed. Keep
/// this guard alive for the whole tracing session and drop it (or call
/// [`FlushGuard::flush`]) before consuming the completed trace output.
#[must_use = "keep the guard alive for the tracing session so buffered data is flushed"]
pub struct FlushGuard {
    inner: Arc<Inner>,
}

impl FlushGuard {
    /// Drains all threads' queues to the writer and flushes it.
    ///
    /// Also surfaces the first write error hit by background emission, if
    /// any; once a write error occurs, further trace data is discarded.
    pub fn flush(&self) -> std::io::Result<()> {
        self.inner.flush_all()
    }
}

impl Drop for FlushGuard {
    fn drop(&mut self) {
        let _ = self.inner.flush_all();
    }
}
