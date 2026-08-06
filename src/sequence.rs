// Copyright 2026 Lalit Maganti
// SPDX-License-Identifier: Apache-2.0

//! Sequence state, packet lifecycle, and callback-scoped output infrastructure.
//!
//! This module owns sequence encoding state and packet-level schema writes.

use std::ops::{Deref, DerefMut};

use crate::emit::schema::*;
use crate::platform::{ClockDomain, monotonic_ns, os_tid, process_name, trace_clock_domain};
use crate::proto::{MessageToken, ProtoBuffer};
use crate::runtime::{Inner, PerThreadBuf, PtrMap};
use crate::thread::ThreadCtx;

const LEVEL_ANNOTATION_NAME_IID: u64 = 1;

fn realtime_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0)
}

#[derive(Default)]
struct Interner {
    map: PtrMap<u64>,
    next: u64,
    last_key: usize,
    last_iid: u64,
}

impl Interner {
    fn after_reserved(next: u64) -> Self {
        Interner {
            map: PtrMap::default(),
            next,
            last_key: 0,
            last_iid: 0,
        }
    }

    #[inline(always)]
    fn intern(&mut self, value: &'static str, pending: &mut Vec<(u64, &'static str)>) -> u64 {
        let key = value.as_ptr() as usize;
        if self.last_key == key {
            return self.last_iid;
        }
        match self.map.get(&key) {
            Some(&iid) => {
                self.last_key = key;
                self.last_iid = iid;
                iid
            }
            None => self.intern_miss(value, pending),
        }
    }

    #[cold]
    #[inline(never)]
    fn intern_miss(&mut self, value: &'static str, pending: &mut Vec<(u64, &'static str)>) -> u64 {
        self.next += 1;
        self.last_key = value.as_ptr() as usize;
        self.last_iid = self.next;
        self.map.insert(self.last_key, self.next);
        pending.push((self.next, value));
        self.next
    }
}

/// Encoding state for one packet sequence. It lives inside `ThreadCtx`, but
/// packet lifecycle access is mediated by `SequenceWriter`.
pub(crate) struct SequenceState {
    sequence_id: u32,
    thread_track_uuid: u64,
    clock_anchor_ns: u64,
    last_timestamp_ns: u64,
    packet_prefix: Option<(usize, MessageToken)>,
    initialized: bool,
    first_packet_done: bool,
    event_names: Interner,
    annotation_names: Interner,
    level_name_seen: bool,
    level_values_seen: u8,
    categories: Interner,
    source_locations: PtrMap<u64>,
    next_source_location_iid: u64,
    new_event_names: Vec<(u64, &'static str)>,
    new_annotation_names: Vec<(u64, &'static str)>,
    new_annotation_values: Vec<(u64, &'static str)>,
    new_categories: Vec<(u64, &'static str)>,
    new_source_locations: Vec<(u64, &'static str, u32)>,
    counter_track_cache: PtrMap<u64>,
    scratch: ProtoBuffer,
}

impl SequenceState {
    pub(crate) fn new(sequence_id: u32, thread_track_uuid: u64, clock_anchor_ns: u64) -> Self {
        SequenceState {
            sequence_id,
            thread_track_uuid,
            clock_anchor_ns,
            last_timestamp_ns: clock_anchor_ns,
            packet_prefix: None,
            initialized: false,
            first_packet_done: false,
            event_names: Interner::default(),
            annotation_names: Interner::after_reserved(LEVEL_ANNOTATION_NAME_IID),
            level_name_seen: false,
            level_values_seen: 0,
            categories: Interner::default(),
            source_locations: PtrMap::default(),
            next_source_location_iid: 0,
            new_event_names: Vec::new(),
            new_annotation_names: Vec::new(),
            new_annotation_values: Vec::new(),
            new_categories: Vec::new(),
            new_source_locations: Vec::new(),
            counter_track_cache: PtrMap::default(),
            scratch: ProtoBuffer::new(),
        }
    }

    #[inline(always)]
    pub(crate) fn thread_track_uuid(&self) -> u64 {
        self.thread_track_uuid
    }

    #[inline(always)]
    pub(crate) fn intern_event_name(&mut self, name: &'static str) -> u64 {
        self.event_names.intern(name, &mut self.new_event_names)
    }

    #[inline(always)]
    pub(crate) fn intern_annotation_name(&mut self, name: &'static str) -> u64 {
        self.annotation_names
            .intern(name, &mut self.new_annotation_names)
    }

    #[inline(always)]
    pub(crate) fn counter_track_uuid(&self, name: &'static str) -> Option<u64> {
        self.counter_track_cache
            .get(&(name.as_ptr() as usize))
            .copied()
    }

    #[inline(always)]
    pub(crate) fn cache_counter_track(&mut self, name: &'static str, uuid: u64) {
        self.counter_track_cache
            .insert(name.as_ptr() as usize, uuid);
    }

    #[inline(always)]
    pub(crate) fn intern_category(&mut self, name: &'static str) -> u64 {
        self.categories.intern(name, &mut self.new_categories)
    }

    pub(crate) fn intern_source_location(
        &mut self,
        callsite: usize,
        file: &'static str,
        line: u32,
    ) -> u64 {
        *self.source_locations.entry(callsite).or_insert_with(|| {
            self.next_source_location_iid += 1;
            self.new_source_locations
                .push((self.next_source_location_iid, file, line));
            self.next_source_location_iid
        })
    }

    #[inline(always)]
    fn emit_interned_data(&mut self, packet: &mut ProtoBuffer) {
        if !self.new_event_names.is_empty()
            || !self.new_annotation_names.is_empty()
            || !self.new_annotation_values.is_empty()
            || !self.new_categories.is_empty()
            || !self.new_source_locations.is_empty()
        {
            self.emit_pending_interned_data(packet);
        }
    }

    #[cold]
    #[inline(never)]
    fn emit_pending_interned_data(&mut self, packet: &mut ProtoBuffer) {
        {
            let mut interned = packet.message(trace_packet::INTERNED_DATA);
            for (iid, name) in &self.new_categories {
                let mut entry = interned.message(interned_data::EVENT_CATEGORIES);
                entry.varint_field(event_name::IID, *iid);
                entry.string_field(event_name::NAME, name);
            }
            for (iid, name) in &self.new_event_names {
                let mut entry = interned.message(interned_data::EVENT_NAMES);
                entry.varint_field(event_name::IID, *iid);
                entry.string_field(event_name::NAME, name);
            }
            for (iid, name) in &self.new_annotation_names {
                let mut entry = interned.message(interned_data::DEBUG_ANNOTATION_NAMES);
                entry.varint_field(debug_annotation_name::IID, *iid);
                entry.string_field(debug_annotation_name::NAME, name);
            }
            for (iid, value) in &self.new_annotation_values {
                let mut entry = interned.message(interned_data::DEBUG_ANNOTATION_STRING_VALUES);
                entry.varint_field(interned_string::IID, *iid);
                entry.string_field(interned_string::STR, value);
            }
            for (iid, file, line) in &self.new_source_locations {
                let mut entry = interned.message(interned_data::SOURCE_LOCATIONS);
                entry.varint_field(source_location::IID, *iid);
                entry.string_field(source_location::FILE_NAME, file);
                entry.varint_field(source_location::LINE_NUMBER, u64::from(*line));
            }
        }
        self.new_event_names.clear();
        self.new_annotation_names.clear();
        self.new_annotation_values.clear();
        self.new_categories.clear();
        self.new_source_locations.clear();
    }

    #[inline(always)]
    pub(crate) fn level_name_seen(&self) -> bool {
        self.level_name_seen
    }

    #[inline(always)]
    pub(crate) fn level_value_seen(&self, bit: u8) -> bool {
        self.level_values_seen & bit != 0
    }

    #[cold]
    #[inline(never)]
    pub(crate) fn register_level_name(&mut self) {
        self.level_name_seen = true;
        self.new_annotation_names
            .push((LEVEL_ANNOTATION_NAME_IID, "level"));
    }

    #[cold]
    #[inline(never)]
    pub(crate) fn register_level_value(&mut self, bit: u8, iid: u64, value: &'static str) {
        self.level_values_seen |= bit;
        self.new_annotation_values.push((iid, value));
    }
}

pub(crate) struct Packet<'a> {
    inner: &'a Inner,
    pub(crate) sequence: &'a mut SequenceState,
    producer: &'a mut rtrb::Producer<u8>,
    buffer: &'a PerThreadBuf,
    pub(crate) proto: ProtoBuffer,
    root: MessageToken,
}

impl Deref for Packet<'_> {
    type Target = ProtoBuffer;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.proto
    }
}

impl DerefMut for Packet<'_> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.proto
    }
}

impl Drop for Packet<'_> {
    #[inline(always)]
    fn drop(&mut self) {
        self.sequence.emit_interned_data(&mut self.proto);
        self.proto.finish_message(self.root);
        if self
            .producer
            .push_entire_slice(self.proto.as_bytes())
            .is_err()
        {
            self.inner
                .drain_thread_buffer(self.buffer, self.proto.as_bytes());
        }
        self.sequence.scratch = std::mem::take(&mut self.proto);
    }
}

/// Callback-scoped access to packet-sequence infrastructure.
pub(crate) struct SequenceWriter<'a> {
    inner: &'a Inner,
    pub(crate) sequence: &'a mut SequenceState,
    producer: &'a mut rtrb::Producer<u8>,
    buffer: &'a PerThreadBuf,
    /// All packets produced by one tracing callback share its timestamp.
    timestamp_ns: Option<u64>,
}

impl<'a> SequenceWriter<'a> {
    #[inline(always)]
    pub(crate) fn new(inner: &'a Inner, thread: &'a mut ThreadCtx) -> Self {
        SequenceWriter {
            inner,
            sequence: &mut thread.sequence,
            producer: &mut thread.producer,
            buffer: &thread.buffer,
            timestamp_ns: None,
        }
    }

    #[inline(always)]
    pub(crate) fn sequence(&mut self) -> &mut SequenceState {
        self.sequence
    }

    #[inline(always)]
    pub(crate) fn packet(&mut self) -> Packet<'_> {
        if !self.sequence.initialized {
            self.initialize();
        }
        self.packet_without_initialization()
    }

    #[inline(always)]
    fn packet_without_initialization(&mut self) -> Packet<'_> {
        let mut proto = std::mem::take(&mut self.sequence.scratch);
        let (root, reused_prefix) = match self.sequence.packet_prefix {
            Some((prefix_len, root)) => {
                proto.truncate(prefix_len);
                (root, true)
            }
            None => {
                proto.clear();
                (proto.begin_message(trace::PACKET), false)
            }
        };
        let raw_timestamp = self.timestamp();
        let mut packet = Packet {
            inner: self.inner,
            sequence: self.sequence,
            producer: self.producer,
            buffer: self.buffer,
            proto,
            root,
        };
        if packet.sequence.first_packet_done {
            let delta = raw_timestamp.saturating_sub(packet.sequence.last_timestamp_ns);
            packet.sequence.last_timestamp_ns = raw_timestamp;
            if !reused_prefix {
                Self::initialize_packet_prefix(&mut packet);
            }
            packet.varint_field(trace_packet::TIMESTAMP, delta);
        } else {
            let (timestamp, clock_id) = match trace_clock_domain() {
                ClockDomain::Builtin(clock_id) => (raw_timestamp, clock_id),
                ClockDomain::SequenceLocal => {
                    (realtime_ns(), clock_snapshot::BUILTIN_CLOCK_REALTIME)
                }
            };
            Self::initialize_first_packet(&mut packet, timestamp, clock_id);
        }
        packet
    }

    #[cold]
    #[inline(never)]
    fn initialize_packet_prefix(packet: &mut Packet<'_>) {
        let sequence_id = packet.sequence.sequence_id;
        packet.varint_field(
            trace_packet::TRUSTED_PACKET_SEQUENCE_ID,
            u64::from(sequence_id),
        );
        packet.varint_field(
            trace_packet::SEQUENCE_FLAGS,
            trace_packet::SEQ_NEEDS_INCREMENTAL_STATE,
        );
        packet.sequence.packet_prefix = Some((packet.proto.as_bytes().len(), packet.root));
    }

    #[cold]
    #[inline(never)]
    fn initialize_first_packet(packet: &mut Packet<'_>, timestamp: u64, timestamp_clock_id: u64) {
        packet.varint_field(trace_packet::TIMESTAMP, timestamp);
        let sequence_id = packet.sequence.sequence_id;
        packet.varint_field(
            trace_packet::TRUSTED_PACKET_SEQUENCE_ID,
            u64::from(sequence_id),
        );
        packet.sequence.first_packet_done = true;
        packet.varint_field(
            trace_packet::SEQUENCE_FLAGS,
            trace_packet::SEQ_INCREMENTAL_STATE_CLEARED | trace_packet::SEQ_NEEDS_INCREMENTAL_STATE,
        );
        packet.varint_field(trace_packet::FIRST_PACKET_ON_SEQUENCE, 1);
        packet.varint_field(trace_packet::TIMESTAMP_CLOCK_ID, timestamp_clock_id);
        let thread_track_uuid = packet.sequence.thread_track_uuid;
        let mut defaults = packet.message(trace_packet::TRACE_PACKET_DEFAULTS);
        defaults.varint_field(
            trace_packet_defaults::TIMESTAMP_CLOCK_ID,
            clock_snapshot::CUSTOM_CLOCK_ID,
        );
        let mut track_defaults = defaults.message(trace_packet_defaults::TRACK_EVENT_DEFAULTS);
        track_defaults.redundant_varint_field(track_event_defaults::TRACK_UUID, thread_track_uuid);
    }

    #[inline(always)]
    fn timestamp(&mut self) -> u64 {
        match self.timestamp_ns {
            Some(timestamp) => timestamp,
            None => {
                let timestamp = self.inner.raw_clock_ns();
                self.timestamp_ns = Some(timestamp);
                timestamp
            }
        }
    }

    #[cold]
    #[inline(never)]
    fn initialize(&mut self) {
        self.sequence.initialized = true;
        self.clock_snapshot();
        self.process_descriptor();
        self.thread_descriptor();
    }

    fn clock_snapshot(&mut self) {
        let raw_timestamp = self.timestamp();
        self.sequence.last_timestamp_ns = raw_timestamp;
        let trace_clock_domain = trace_clock_domain();
        let primary_clock_id = match trace_clock_domain {
            ClockDomain::Builtin(clock_id) => clock_id,
            ClockDomain::SequenceLocal => clock_snapshot::BUILTIN_CLOCK_REALTIME,
        };
        let mut clocks = [(0, 0); 4];
        let mut count = 0;
        clocks[count] = (
            clock_snapshot::CUSTOM_CLOCK_ID,
            raw_timestamp.saturating_sub(self.sequence.clock_anchor_ns),
        );
        count += 1;
        if let ClockDomain::Builtin(clock_id) = trace_clock_domain {
            clocks[count] = (clock_id, raw_timestamp);
            count += 1;
        }
        clocks[count] = (clock_snapshot::BUILTIN_CLOCK_REALTIME, realtime_ns());
        count += 1;
        if trace_clock_domain != ClockDomain::Builtin(clock_snapshot::BUILTIN_CLOCK_MONOTONIC)
            && let Some(timestamp) = monotonic_ns()
        {
            clocks[count] = (clock_snapshot::BUILTIN_CLOCK_MONOTONIC, timestamp);
            count += 1;
        }
        let mut packet = self.packet_without_initialization();
        {
            let mut snapshot = packet.message(trace_packet::CLOCK_SNAPSHOT);
            snapshot.varint_field(clock_snapshot::PRIMARY_TRACE_CLOCK, primary_clock_id);
            for (clock_id, timestamp) in &clocks[..count] {
                let mut clock = snapshot.message(clock_snapshot::CLOCKS);
                clock.varint_field(clock_snapshot::CLOCK_ID, *clock_id);
                clock.varint_field(clock_snapshot::CLOCK_TIMESTAMP, *timestamp);
                if *clock_id == clock_snapshot::CUSTOM_CLOCK_ID {
                    clock.varint_field(clock_snapshot::CLOCK_IS_INCREMENTAL, 1);
                }
            }
        }
    }

    fn process_descriptor(&mut self) {
        if !self.inner.claim_process_descriptor() {
            return;
        }
        let process_uuid = self.inner.process_track_uuid();
        let pid = u64::from(std::process::id());
        let name = process_name();
        let mut packet = self.packet_without_initialization();
        {
            let mut descriptor = packet.message(trace_packet::TRACK_DESCRIPTOR);
            descriptor.redundant_varint_field(track_descriptor::UUID, process_uuid);
            let mut process = descriptor.message(track_descriptor::PROCESS);
            process.varint_field(process_descriptor::PID, pid);
            for argument in std::env::args() {
                process.string_field(process_descriptor::CMDLINE, &argument);
            }
            process.string_field(process_descriptor::PROCESS_NAME, &name);
        }
    }

    fn thread_descriptor(&mut self) {
        let pid = u64::from(std::process::id());
        let tid = os_tid().unwrap_or_else(|| self.inner.alloc_tid());
        let thread_name = std::thread::current()
            .name()
            .map(str::to_owned)
            .unwrap_or_else(|| format!("thread-{tid}"));
        let thread_uuid = self.sequence.thread_track_uuid;
        let mut packet = self.packet_without_initialization();
        {
            let mut descriptor = packet.message(trace_packet::TRACK_DESCRIPTOR);
            descriptor.redundant_varint_field(track_descriptor::UUID, thread_uuid);
            let mut thread = descriptor.message(track_descriptor::THREAD);
            thread.varint_field(thread_descriptor::PID, pid);
            thread.varint_field(thread_descriptor::TID, tid);
            thread.string_field(thread_descriptor::THREAD_NAME, &thread_name);
        }
    }
}
