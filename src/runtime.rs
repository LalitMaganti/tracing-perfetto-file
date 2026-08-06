// Copyright 2026 Lalit Maganti
// SPDX-License-Identifier: Apache-2.0

//! Layer-wide runtime state, output queues, and shared value types.

use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::io::Write;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Instant;

use crate::SpanMode;
use crate::platform::boottime_ns;
use crate::span_state::SpanStore;
use crate::thread::acquire;

/// Resolved builder options.
pub(crate) struct Config {
    pub span_mode: SpanMode,
    pub debug_annotations: bool,
    pub poll_slices: bool,
    pub source_locations: bool,
    pub counters: bool,
}

/// An owned span field value, stored until the span's slices are emitted.
pub(crate) enum FieldValue {
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    Str(String),
}

/// One end of a `follows_from` flow, waiting for the span's next event.
pub(crate) enum PendingFlow {
    Start(u64),
    Terminate(u64),
}

/// Mutable data retained only by spans that need later fields or flows.
#[derive(Default)]
pub(crate) struct RetainedSpanData {
    pub fields: Vec<(&'static str, FieldValue)>,
    pub pending_flows: Vec<PendingFlow>,
    pub poll_flow_id: Option<NonZeroU64>,
}

impl RetainedSpanData {
    pub(crate) fn clear(&mut self) {
        self.fields.clear();
        self.pending_flows.clear();
        self.poll_flow_id = None;
    }
}

/// Cheap multiplicative hasher for pointer-sized keys.
#[derive(Default)]
pub(crate) struct PtrHasher(u64);

impl std::hash::Hasher for PtrHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.0 = (self.0 ^ u64::from(byte)).wrapping_mul(0x100_0000_01b3);
        }
    }

    fn write_usize(&mut self, value: usize) {
        let mut mixed = (value as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        self.0 = mixed ^ (mixed >> 31);
    }
}

pub(crate) type PtrMap<V> = HashMap<usize, V, std::hash::BuildHasherDefault<PtrHasher>>;

/// Consumer half of a thread's lock-free packet queue. The producer remains
/// thread-local; this mutex only serializes cold-path drainers.
#[repr(align(64))]
pub(crate) struct PerThreadBuf {
    consumer: Mutex<rtrb::Consumer<u8>>,
}

impl PerThreadBuf {
    pub(crate) fn new(consumer: rtrb::Consumer<u8>) -> Self {
        PerThreadBuf {
            consumer: Mutex::new(consumer),
        }
    }
}

/// State shared by the layer, flush guard, and every thread context.
pub(crate) struct Inner {
    id: u64,
    writer: Mutex<Box<dyn Write + Send>>,
    error: Mutex<Option<std::io::Error>>,
    has_error: AtomicBool,
    start: Instant,
    counter_tracks: Mutex<PtrMap<u64>>,
    next_sequence_id: AtomicU32,
    next_uuid: AtomicU64,
    next_flow_id: AtomicU64,
    /// Permanently selects flow-aware span callback paths after the first
    /// `follows_from` relationship.
    has_follows_from: AtomicBool,
    next_tid_offset: AtomicU64,
    process_track: OnceLock<u64>,
    process_desc_emitted: AtomicBool,
    threads: Mutex<Vec<Arc<PerThreadBuf>>>,
    pub(crate) span_store: SpanStore,
    pub(crate) config: Config,
}

impl Inner {
    pub(crate) fn new(writer: Box<dyn Write + Send>, config: Config) -> Self {
        let uuid_base = RandomState::new().hash_one(std::process::id());
        let flow_id_base = RandomState::new().hash_one(uuid_base);
        static NEXT_LAYER_ID: AtomicU64 = AtomicU64::new(1);
        Inner {
            id: NEXT_LAYER_ID.fetch_add(1, Ordering::Relaxed),
            writer: Mutex::new(writer),
            error: Mutex::new(None),
            has_error: AtomicBool::new(false),
            start: Instant::now(),
            counter_tracks: Mutex::new(PtrMap::default()),
            next_sequence_id: AtomicU32::new(
                (RandomState::new().hash_one(0u64) as u32).max(0x0001_0000),
            ),
            next_uuid: AtomicU64::new(uuid_base),
            next_flow_id: AtomicU64::new(flow_id_base),
            has_follows_from: AtomicBool::new(false),
            next_tid_offset: AtomicU64::new(0),
            process_track: OnceLock::new(),
            process_desc_emitted: AtomicBool::new(false),
            threads: Mutex::new(Vec::new()),
            span_store: SpanStore::new(),
            config,
        }
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    #[inline(always)]
    pub(crate) fn has_error(&self) -> bool {
        self.has_error.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub(crate) fn raw_clock_ns(&self) -> u64 {
        boottime_ns().unwrap_or_else(|| self.start.elapsed().as_nanos() as u64)
    }

    pub(crate) fn alloc_uuid(&self) -> u64 {
        loop {
            let uuid = self.next_uuid.fetch_add(1, Ordering::Relaxed);
            if uuid != 0 {
                return uuid;
            }
        }
    }

    pub(crate) fn alloc_flow_id(&self) -> u64 {
        loop {
            let id = self.next_flow_id.fetch_add(1, Ordering::Relaxed);
            if id != 0 {
                return id;
            }
        }
    }

    pub(crate) fn note_follows_from(&self) {
        self.has_follows_from.store(true, Ordering::Release);
    }

    pub(crate) fn has_follows_from(&self) -> bool {
        self.has_follows_from.load(Ordering::Acquire)
    }

    pub(crate) fn alloc_sequence_id(&self) -> u32 {
        self.next_sequence_id.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn alloc_tid(&self) -> u64 {
        u64::from(std::process::id()) + self.next_tid_offset.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn process_track_uuid(&self) -> u64 {
        *self.process_track.get_or_init(|| self.alloc_uuid())
    }

    pub(crate) fn claim_process_descriptor(&self) -> bool {
        !self.process_desc_emitted.swap(true, Ordering::Relaxed)
    }

    pub(crate) fn counter_track_uuid(&self, key: usize) -> u64 {
        let mut tracks = self
            .counter_tracks
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *tracks.entry(key).or_insert_with(|| self.alloc_uuid())
    }

    pub(crate) fn register_thread(&self, buffer: Arc<PerThreadBuf>) {
        self.threads
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(buffer);
    }

    pub(crate) fn take_retained_span(self: &Arc<Self>) -> Box<RetainedSpanData> {
        let Some(mut thread) = acquire(self) else {
            return Box::new(RetainedSpanData::default());
        };
        thread.take_retained_span()
    }

    pub(crate) fn recycle_retained_span(self: &Arc<Self>, retained: Box<RetainedSpanData>) {
        if let Some(mut thread) = acquire(self) {
            thread.return_retained_span(retained);
        }
    }

    /// Drains one thread's published queue and then writes `trailing` while
    /// holding the same consumer lock, preserving packet order on overflow.
    #[cold]
    #[inline(never)]
    pub(crate) fn drain_thread_buffer(&self, buffer: &PerThreadBuf, trailing: &[u8]) {
        let mut consumer = buffer
            .consumer
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut writer = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let mut error = None;
        let available = consumer.slots();
        if available != 0 {
            let chunk = consumer
                .read_chunk(available)
                .expect("the reported SPSC slots must remain readable");
            if !self.has_error() {
                let (first, second) = chunk.as_slices();
                if let Err(write_error) = writer.write_all(first) {
                    error = Some(write_error);
                } else if let Err(write_error) = writer.write_all(second) {
                    error = Some(write_error);
                }
            }
            chunk.commit_all();
        }
        if error.is_none()
            && !self.has_error()
            && !trailing.is_empty()
            && let Err(write_error) = writer.write_all(trailing)
        {
            error = Some(write_error);
        }
        drop(writer);
        drop(consumer);
        if let Some(error) = error {
            self.record_error(error);
        }
    }

    #[cold]
    #[inline(never)]
    fn record_error(&self, error: std::io::Error) {
        let mut recorded = self.error.lock().unwrap_or_else(PoisonError::into_inner);
        if recorded.is_none() {
            *recorded = Some(error);
        }
        self.has_error.store(true, Ordering::Relaxed);
    }

    pub(crate) fn flush_all(&self) -> std::io::Result<()> {
        let buffers = self
            .threads
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        for buffer in buffers {
            self.drain_thread_buffer(&buffer, &[]);
        }
        {
            let mut writer = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
            if let Err(error) = writer.flush() {
                drop(writer);
                self.record_error(error);
            }
        }
        if !self.has_error() {
            return Ok(());
        }
        let recorded = self.error.lock().unwrap_or_else(PoisonError::into_inner);
        Err(match recorded.as_ref() {
            Some(error) => std::io::Error::new(error.kind(), error.to_string()),
            None => std::io::Error::other("tracing-perfetto-file: write error"),
        })
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        let _ = self.flush_all();
    }
}
