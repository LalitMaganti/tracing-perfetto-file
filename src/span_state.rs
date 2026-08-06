// Copyright 2026 Lalit Maganti
// SPDX-License-Identifier: Apache-2.0

//! Unified concurrent storage for active span state and reusable track UUIDs.

use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::num::NonZeroU64;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_queue::ArrayQueue;

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use dashmap::mapref::one::{Ref, RefMut};
use parking_lot::{Mutex, MutexGuard};

use crate::runtime::RetainedSpanData;

const MAX_REUSABLE_KEYS: usize = 8192;

#[derive(Clone, Copy, Debug)]
pub(crate) struct TrackKey {
    pub(crate) name: &'static str,
    pub(crate) parent_uuid: u64,
}

impl PartialEq for TrackKey {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        self.name.as_ptr() == other.name.as_ptr()
            && self.name.len() == other.name.len()
            && self.parent_uuid == other.parent_uuid
    }
}

impl Eq for TrackKey {}

impl Hash for TrackKey {
    #[inline(always)]
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_usize(self.name.as_ptr() as usize);
        state.write_usize(self.name.len());
        state.write_u64(self.parent_uuid);
    }
}

pub(crate) struct SpanData {
    pub(crate) track_pool: Option<Arc<TrackPool>>,
    pub(crate) track_uuid: Option<NonZeroU64>,
    pub(crate) retained: Option<Box<RetainedSpanData>>,
}

impl SpanData {
    #[inline(always)]
    pub(crate) fn new(retained: Option<Box<RetainedSpanData>>) -> Self {
        SpanData {
            track_pool: None,
            track_uuid: None,
            retained,
        }
    }
}

pub(crate) struct TrackPool {
    key: TrackKey,
    tracks: ArrayQueue<u64>,
}

impl TrackPool {
    #[inline(always)]
    fn new(key: TrackKey) -> Self {
        Self {
            key,
            tracks: ArrayQueue::new(4),
        }
    }

    #[inline(always)]
    pub(crate) fn key(&self) -> TrackKey {
        self.key
    }

    #[inline(always)]
    pub(crate) fn claim(&self) -> Option<u64> {
        self.tracks.pop()
    }

    #[inline(always)]
    pub(crate) fn release(&self, uuid: u64) {
        let _ = self.tracks.push(uuid);
    }
}

#[derive(Default)]
struct StoreHasher(u64);

impl Hasher for StoreHasher {
    #[inline(always)]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline(always)]
    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.write_u64(u64::from(byte));
        }
    }

    #[inline(always)]
    fn write_u8(&mut self, value: u8) {
        self.write_u64(u64::from(value));
    }

    #[inline(always)]
    fn write_usize(&mut self, value: usize) {
        self.write_u64(value as u64);
    }

    #[inline(always)]
    fn write_u64(&mut self, value: u64) {
        let mut mixed = (self.0 ^ value).wrapping_add(0x9e37_79b9_7f4a_7c15);
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        self.0 = mixed ^ (mixed >> 31);
    }
}

const FAST_SHARDS: usize = 8;
const FAST_SLOTS: usize = 4;

type OverflowMap = DashMap<u64, SpanData, BuildHasherDefault<StoreHasher>>;
type PoolMap = DashMap<TrackKey, Arc<TrackPool>, BuildHasherDefault<StoreHasher>>;

struct ActiveEntry {
    span_id: NonZeroU64,
    data: SpanData,
}

struct ActiveSlots {
    entries: [Option<ActiveEntry>; FAST_SLOTS],
}

impl ActiveSlots {
    #[inline(always)]
    fn new() -> Self {
        Self {
            entries: std::array::from_fn(|_| None),
        }
    }

    #[inline(always)]
    fn insert(&mut self, span_id: u64, data: SpanData) -> Result<(), SpanData> {
        for slot in &mut self.entries {
            if slot.is_none() {
                *slot = Some(ActiveEntry {
                    span_id: NonZeroU64::new(span_id).expect("tracing span IDs are nonzero"),
                    data,
                });
                return Ok(());
            }
        }
        Err(data)
    }

    #[inline(always)]
    fn index(&self, span_id: u64) -> Option<usize> {
        for index in 0..FAST_SLOTS {
            if let Some(entry) = &self.entries[index]
                && entry.span_id.get() == span_id
            {
                return Some(index);
            }
        }
        None
    }

    #[inline(always)]
    fn take(&mut self, span_id: u64) -> Option<SpanData> {
        let index = self.index(span_id)?;
        let entry = self.entries[index].take()?;
        Some(entry.data)
    }
}

pub(crate) struct SpanRef<'a> {
    inner: SpanRefInner<'a>,
}

enum SpanRefInner<'a> {
    Fast {
        slots: MutexGuard<'a, ActiveSlots>,
        index: usize,
    },
    Overflow(Ref<'a, u64, SpanData>),
}

impl Deref for SpanRef<'_> {
    type Target = SpanData;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        match &self.inner {
            SpanRefInner::Fast { slots, index } => {
                &slots.entries[*index]
                    .as_ref()
                    .expect("occupied span slot")
                    .data
            }
            SpanRefInner::Overflow(data) => data.value(),
        }
    }
}

pub(crate) struct SpanRefMut<'a> {
    inner: SpanRefMutInner<'a>,
}

enum SpanRefMutInner<'a> {
    Fast {
        slots: MutexGuard<'a, ActiveSlots>,
        index: usize,
    },
    Overflow(RefMut<'a, u64, SpanData>),
}

impl Deref for SpanRefMut<'_> {
    type Target = SpanData;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        match &self.inner {
            SpanRefMutInner::Fast { slots, index } => {
                &slots.entries[*index]
                    .as_ref()
                    .expect("occupied span slot")
                    .data
            }
            SpanRefMutInner::Overflow(data) => data.value(),
        }
    }
}

impl DerefMut for SpanRefMut<'_> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        match &mut self.inner {
            SpanRefMutInner::Fast { slots, index } => {
                &mut slots.entries[*index]
                    .as_mut()
                    .expect("occupied span slot")
                    .data
            }
            SpanRefMutInner::Overflow(data) => data.value_mut(),
        }
    }
}

#[repr(C)]
struct ActiveShard {
    slots: Mutex<ActiveSlots>,
    // Avoid the lock-convoy behavior measured with the naturally packed stride.
    _stride_padding: [u8; 8],
}

/// Concurrent active-span state with an inline bounded fast tier.
pub(crate) struct SpanStore {
    active: [ActiveShard; FAST_SHARDS],
    overflow: OverflowMap,
    pools: PoolMap,
    reusable_keys: AtomicUsize,
}

impl SpanStore {
    pub(crate) fn new() -> Self {
        SpanStore {
            active: std::array::from_fn(|_| ActiveShard {
                slots: Mutex::new(ActiveSlots::new()),
                _stride_padding: [0; 8],
            }),
            overflow: OverflowMap::with_capacity_and_hasher(16, BuildHasherDefault::default()),
            pools: PoolMap::with_capacity_and_hasher(16, BuildHasherDefault::default()),
            reusable_keys: AtomicUsize::new(0),
        }
    }

    #[inline(always)]
    fn active_shard(span_id: u64) -> usize {
        let mut mixed = span_id.wrapping_add(0x9e37_79b9_7f4a_7c15);
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        (mixed ^ (mixed >> 27)) as usize & (FAST_SHARDS - 1)
    }

    #[inline(always)]
    pub(crate) fn insert(&self, span_id: u64, data: SpanData) {
        let shard = Self::active_shard(span_id);
        let data = match self.active[shard].slots.lock().insert(span_id, data) {
            Ok(()) => return,
            Err(data) => data,
        };
        let replaced = self.overflow.insert(span_id, data);
        debug_assert!(replaced.is_none(), "span state inserted once");
    }

    #[inline(always)]
    pub(crate) fn get(&self, span_id: u64) -> Option<SpanRef<'_>> {
        let shard = Self::active_shard(span_id);
        let slots = self.active[shard].slots.lock();
        if let Some(index) = slots.index(span_id) {
            return Some(SpanRef {
                inner: SpanRefInner::Fast { slots, index },
            });
        }
        drop(slots);
        let data = self.overflow.get(&span_id)?;
        Some(SpanRef {
            inner: SpanRefInner::Overflow(data),
        })
    }

    #[inline(always)]
    pub(crate) fn get_mut(&self, span_id: u64) -> Option<SpanRefMut<'_>> {
        let shard = Self::active_shard(span_id);
        let slots = self.active[shard].slots.lock();
        if let Some(index) = slots.index(span_id) {
            return Some(SpanRefMut {
                inner: SpanRefMutInner::Fast { slots, index },
            });
        }
        drop(slots);
        let data = self.overflow.get_mut(&span_id)?;
        Some(SpanRefMut {
            inner: SpanRefMutInner::Overflow(data),
        })
    }

    #[inline(always)]
    pub(crate) fn take(&self, span_id: u64) -> Option<SpanData> {
        let shard = Self::active_shard(span_id);
        if let Some(data) = self.active[shard].slots.lock().take(span_id) {
            return Some(data);
        }
        let (_, data) = self.overflow.remove(&span_id)?;
        Some(data)
    }

    #[inline(always)]
    pub(crate) fn track_pool(&self, key: TrackKey) -> Option<Arc<TrackPool>> {
        if let Some(pool) = self.pools.get(&key) {
            return Some(Arc::clone(&pool));
        }
        self.create_track_pool(key)
    }

    #[cold]
    #[inline(never)]
    fn create_track_pool(&self, key: TrackKey) -> Option<Arc<TrackPool>> {
        match self.pools.entry(key) {
            Entry::Occupied(occupied) => Some(Arc::clone(occupied.get())),
            Entry::Vacant(vacant) => {
                self.reusable_keys
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                        (count < MAX_REUSABLE_KEYS).then_some(count + 1)
                    })
                    .ok()?;
                let pool = Arc::new(TrackPool::new(key));
                vacant.insert(Arc::clone(&pool));
                Some(pool)
            }
        }
    }
}
