// Copyright 2026 Lalit Maganti
// SPDX-License-Identifier: Apache-2.0

use std::cell::RefCell;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::{Arc, Weak};

use crate::runtime::{Inner, PerThreadBuf, RetainedSpanData};
use crate::sequence::SequenceState;
use crate::span_state::{TrackKey, TrackPool};

struct CachedTrackPool {
    key: TrackKey,
    pool: Arc<TrackPool>,
    local_uuid: Option<u64>,
}

pub(crate) struct ClaimedTrack {
    pub(crate) pool: Arc<TrackPool>,
    pub(crate) uuid: Option<u64>,
}

/// Per-thread, per-layer-instance state.
pub(crate) struct ThreadCtx {
    inner: Weak<Inner>,
    borrowed: bool,
    pub(crate) sequence: SequenceState,
    pub(crate) producer: rtrb::Producer<u8>,
    pub(crate) buffer: Arc<PerThreadBuf>,
    track_pools: [Option<CachedTrackPool>; 8],
    next_track_pool: usize,
    retained_spans: [Option<Box<RetainedSpanData>>; 4],
}

impl ThreadCtx {
    #[cold]
    #[inline(never)]
    fn new(inner: &Arc<Inner>) -> Self {
        let (producer, consumer) = rtrb::RingBuffer::new(32 * 1024);
        let buffer = Arc::new(PerThreadBuf::new(consumer));
        inner.register_thread(Arc::clone(&buffer));
        ThreadCtx {
            inner: Arc::downgrade(inner),
            borrowed: false,
            sequence: SequenceState::new(
                inner.alloc_sequence_id(),
                inner.alloc_uuid(),
                inner.raw_clock_ns(),
            ),
            producer,
            buffer,
            track_pools: std::array::from_fn(|_| None),
            next_track_pool: 0,
            retained_spans: std::array::from_fn(|_| None),
        }
    }

    pub(crate) fn thread_track_uuid(&self) -> u64 {
        self.sequence.thread_track_uuid()
    }

    #[inline(always)]
    pub(crate) fn claim_track(&mut self, inner: &Inner, key: TrackKey) -> Option<ClaimedTrack> {
        for slot in &mut self.track_pools {
            let Some(cached) = slot else { continue };
            if cached.key != key {
                continue;
            }
            let uuid = match cached.local_uuid.take() {
                Some(uuid) => Some(uuid),
                None => cached.pool.claim(),
            };
            return Some(ClaimedTrack {
                pool: Arc::clone(&cached.pool),
                uuid,
            });
        }
        let pool = inner.span_store.track_pool(key)?;
        let uuid = pool.claim();
        self.replace_track_pool(key, Arc::clone(&pool), None);
        Some(ClaimedTrack { pool, uuid })
    }

    #[inline(always)]
    pub(crate) fn release_track(&mut self, pool: &Arc<TrackPool>, uuid: u64) {
        for slot in &mut self.track_pools {
            let Some(cached) = slot else { continue };
            if !Arc::ptr_eq(&cached.pool, pool) {
                continue;
            }
            if cached.local_uuid.is_none() {
                cached.local_uuid = Some(uuid);
            } else {
                pool.release(uuid);
            }
            return;
        }
        self.replace_track_pool(pool.key(), Arc::clone(pool), Some(uuid));
    }

    pub(crate) fn take_retained_span(&mut self) -> Box<RetainedSpanData> {
        for slot in &mut self.retained_spans {
            if let Some(retained) = slot.take() {
                return retained;
            }
        }
        Box::new(RetainedSpanData::default())
    }

    pub(crate) fn return_retained_span(&mut self, mut retained: Box<RetainedSpanData>) {
        retained.clear();
        for slot in &mut self.retained_spans {
            if slot.is_none() {
                *slot = Some(retained);
                return;
            }
        }
    }

    #[inline(always)]
    fn replace_track_pool(&mut self, key: TrackKey, pool: Arc<TrackPool>, local: Option<u64>) {
        let slot = self.next_track_pool;
        self.next_track_pool = (slot + 1) % self.track_pools.len();
        if let Some(old) = self.track_pools[slot].take()
            && let Some(uuid) = old.local_uuid
        {
            old.pool.release(uuid);
        }
        self.track_pools[slot] = Some(CachedTrackPool {
            key,
            pool,
            local_uuid: local,
        });
    }
}

impl Drop for ThreadCtx {
    fn drop(&mut self) {
        for slot in &mut self.track_pools {
            let Some(cached) = slot else { continue };
            if let Some(uuid) = cached.local_uuid.take() {
                cached.pool.release(uuid);
            }
        }
        if let Some(inner) = self.inner.upgrade() {
            inner.drain_thread_buffer(&self.buffer, &[]);
        }
    }
}

thread_local! {
    /// Per-thread contexts keyed by layer instance. Boxing keeps a context at a
    /// stable address while contexts for other layer instances are inserted.
    static THREAD_CTXS: RefCell<Vec<(u64, Box<ThreadCtx>)>> = const { RefCell::new(Vec::new()) };
}

#[cold]
#[inline(never)]
fn insert_thread_context(
    contexts: &mut Vec<(u64, Box<ThreadCtx>)>,
    key: u64,
    inner: &Arc<Inner>,
) -> usize {
    contexts.retain(|(_, context)| context.inner.strong_count() > 0);
    contexts.push((key, Box::new(ThreadCtx::new(inner))));
    contexts.len() - 1
}

/// Exclusive access to one layer's context on the calling thread.
///
/// This guard cannot leave the thread. Its stable context remains owned by TLS,
/// so dropping the thread still drops and drains every context.
pub(crate) struct ThreadCtxGuard<'a> {
    context: NonNull<ThreadCtx>,
    _scope: PhantomData<(&'a Arc<Inner>, Rc<()>)>,
}

impl Deref for ThreadCtxGuard<'_> {
    type Target = ThreadCtx;

    fn deref(&self) -> &Self::Target {
        // SAFETY: `context` points into a stable Box owned by this thread's TLS.
        unsafe { self.context.as_ref() }
    }
}

impl DerefMut for ThreadCtxGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: acquisition sets `borrowed`, preventing another guard from
        // being created until this guard resets it in Drop.
        unsafe { self.context.as_mut() }
    }
}

impl Drop for ThreadCtxGuard<'_> {
    fn drop(&mut self) {
        // SAFETY: the guard cannot leave this thread, and the borrowed Arc
        // keeps this context from being removed by dead-layer cleanup.
        unsafe { self.context.as_mut().borrowed = false };
    }
}

/// Acquires this layer's context on the calling thread. Reentrant access from
/// user formatting implementations is silently dropped.
#[inline]
pub(crate) fn acquire(inner: &Arc<Inner>) -> Option<ThreadCtxGuard<'_>> {
    if inner.has_error() {
        return None;
    }
    let key = inner.id();
    THREAD_CTXS
        .try_with(|contexts| {
            let Ok(mut contexts) = contexts.try_borrow_mut() else {
                return None;
            };
            let mut found = None;
            for index in 0..contexts.len() {
                if contexts[index].0 == key {
                    found = Some(index);
                    break;
                }
            }
            let index = match found {
                Some(index) => index,
                None => insert_thread_context(&mut contexts, key, inner),
            };
            let context = &mut *contexts[index].1;
            if context.borrowed {
                return None;
            }
            context.borrowed = true;
            Some(ThreadCtxGuard {
                context: NonNull::from(context),
                _scope: PhantomData,
            })
        })
        .ok()
        .flatten()
}
