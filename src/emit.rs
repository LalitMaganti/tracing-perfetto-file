// Copyright 2026 Lalit Maganti
// SPDX-License-Identifier: Apache-2.0

//! Straight-line Perfetto protobuf wire writers.

use std::fmt;

use crate::proto::ProtoBuffer;
use schema::*;

pub(crate) const MAX_EXTRA_COUNTERS: usize = 8;

/// Straight-line writer for one `TrackEvent` protobuf message.
pub(crate) struct TrackEventWriter<'a> {
    buffer: &'a mut ProtoBuffer,
}

impl<'a> TrackEventWriter<'a> {
    #[inline(always)]
    pub(crate) fn new(buffer: &'a mut ProtoBuffer) -> Self {
        Self { buffer }
    }

    #[inline(always)]
    pub(crate) fn slice_begin(&mut self) {
        self.buffer
            .varint_field(track_event::TYPE, track_event::TYPE_SLICE_BEGIN);
    }

    #[inline(always)]
    pub(crate) fn slice_end(&mut self) {
        self.buffer
            .varint_field(track_event::TYPE, track_event::TYPE_SLICE_END);
    }

    #[inline(always)]
    pub(crate) fn instant(&mut self) {
        self.buffer
            .varint_field(track_event::TYPE, track_event::TYPE_INSTANT);
    }

    #[inline(always)]
    pub(crate) fn track_uuid(&mut self, uuid: u64) {
        self.buffer
            .redundant_varint_field(track_event::TRACK_UUID, uuid);
    }

    #[inline(always)]
    pub(crate) fn name_iid(&mut self, iid: u64) {
        self.buffer.varint_field(track_event::NAME_IID, iid);
    }

    #[inline(always)]
    pub(crate) fn flow_start(&mut self, id: u64) {
        self.buffer.fixed64_field(track_event::FLOW_IDS, id);
    }

    #[inline(always)]
    pub(crate) fn flow_terminate(&mut self, id: u64) {
        self.buffer
            .fixed64_field(track_event::TERMINATING_FLOW_IDS, id);
    }

    #[inline(always)]
    pub(crate) fn source_location_iid(&mut self, iid: u64) {
        self.buffer
            .varint_field(track_event::SOURCE_LOCATION_IID, iid);
    }

    #[inline(always)]
    pub(crate) fn category_iid(&mut self, iid: u64) {
        self.buffer.varint_field(track_event::CATEGORY_IIDS, iid);
    }

    #[inline(always)]
    pub(crate) fn level_annotation(&mut self, name_iid: u64, value_iid: u64) {
        let mut annotation = self.buffer.message(track_event::DEBUG_ANNOTATIONS);
        annotation.varint_field(debug_annotation::NAME_IID, name_iid);
        annotation.varint_field(debug_annotation::STRING_VALUE_IID, value_iid);
    }

    #[inline(always)]
    pub(crate) fn annotation_bool(&mut self, name_iid: u64, value: bool) {
        let mut annotation = self.buffer.message(track_event::DEBUG_ANNOTATIONS);
        annotation.varint_field(debug_annotation::NAME_IID, name_iid);
        annotation.varint_field(debug_annotation::BOOL_VALUE, u64::from(value));
    }

    #[inline(always)]
    pub(crate) fn annotation_i64(&mut self, name_iid: u64, value: i64) {
        let mut annotation = self.buffer.message(track_event::DEBUG_ANNOTATIONS);
        annotation.varint_field(debug_annotation::NAME_IID, name_iid);
        annotation.varint_field(debug_annotation::INT_VALUE, value as u64);
    }

    #[inline(always)]
    pub(crate) fn annotation_u64(&mut self, name_iid: u64, value: u64) {
        let mut annotation = self.buffer.message(track_event::DEBUG_ANNOTATIONS);
        annotation.varint_field(debug_annotation::NAME_IID, name_iid);
        annotation.varint_field(debug_annotation::UINT_VALUE, value);
    }

    #[inline(always)]
    pub(crate) fn annotation_f64(&mut self, name_iid: u64, value: f64) {
        let mut annotation = self.buffer.message(track_event::DEBUG_ANNOTATIONS);
        annotation.varint_field(debug_annotation::NAME_IID, name_iid);
        annotation.double_field(debug_annotation::DOUBLE_VALUE, value);
    }

    #[inline(always)]
    pub(crate) fn annotation_str(&mut self, name_iid: u64, value: &str) {
        let mut annotation = self.buffer.message(track_event::DEBUG_ANNOTATIONS);
        annotation.varint_field(debug_annotation::NAME_IID, name_iid);
        annotation.string_field(debug_annotation::STRING_VALUE, value);
    }

    #[inline(always)]
    pub(crate) fn annotation_debug(&mut self, name_iid: u64, value: &dyn fmt::Debug) {
        let mut annotation = self.buffer.message(track_event::DEBUG_ANNOTATIONS);
        annotation.varint_field(debug_annotation::NAME_IID, name_iid);
        annotation.debug_field(debug_annotation::STRING_VALUE, &value);
    }

    #[inline(always)]
    pub(crate) fn annotation_display(&mut self, name_iid: u64, value: &dyn fmt::Display) {
        let mut annotation = self.buffer.message(track_event::DEBUG_ANNOTATIONS);
        annotation.varint_field(debug_annotation::NAME_IID, name_iid);
        annotation.display_field(debug_annotation::STRING_VALUE, &value);
    }

    #[inline(always)]
    pub(crate) fn counter_i64(&mut self, track_uuid: u64, value: i64) {
        self.buffer
            .redundant_varint_field(track_event::EXTRA_COUNTER_TRACK_UUIDS, track_uuid);
        self.buffer
            .varint_field(track_event::EXTRA_COUNTER_VALUES, value as u64);
    }

    #[inline(always)]
    pub(crate) fn counter_f64(&mut self, track_uuid: u64, value: f64) {
        self.buffer
            .redundant_varint_field(track_event::EXTRA_DOUBLE_COUNTER_TRACK_UUIDS, track_uuid);
        self.buffer
            .double_field(track_event::EXTRA_DOUBLE_COUNTER_VALUES, value);
    }

    #[inline(always)]
    pub(crate) fn event_name_str(&mut self, value: &str) {
        self.buffer.string_field(track_event::NAME, value);
    }

    #[inline(always)]
    pub(crate) fn event_name_debug(&mut self, value: &dyn fmt::Debug) {
        self.buffer.debug_field(track_event::NAME, &value);
    }
}

#[inline(always)]
pub(crate) fn track_descriptor(
    buffer: &mut ProtoBuffer,
    uuid: u64,
    name: &'static str,
    parent_uuid: u64,
) {
    let mut descriptor = buffer.message(trace_packet::TRACK_DESCRIPTOR);
    descriptor.redundant_varint_field(track_descriptor::UUID, uuid);
    descriptor.string_field(track_descriptor::NAME, name);
    descriptor.redundant_varint_field(track_descriptor::PARENT_UUID, parent_uuid);
}

#[inline(always)]
pub(crate) fn counter_descriptor(
    buffer: &mut ProtoBuffer,
    uuid: u64,
    name: &'static str,
    parent_uuid: u64,
) {
    let mut descriptor = buffer.message(trace_packet::TRACK_DESCRIPTOR);
    descriptor.redundant_varint_field(track_descriptor::UUID, uuid);
    descriptor.string_field(track_descriptor::NAME, name);
    descriptor.redundant_varint_field(track_descriptor::PARENT_UUID, parent_uuid);
    let _counter = descriptor.message(track_descriptor::COUNTER);
}

pub(crate) mod schema {
    //! Hand-transcribed field numbers from the Perfetto trace protos.
    //!
    //! Sources are the `.proto` files under `protos/perfetto/trace/` in the
    //! Perfetto repository. Proto field numbers are stable by contract, so
    //! these constants never change for existing fields; re-check the protos
    //! only when adding support for new fields.

    /// `perfetto.protos.Trace` (trace.proto).
    pub(crate) mod trace {
        /// `repeated TracePacket packet = 1;`
        pub const PACKET: u32 = 1;
    }

    /// `perfetto.protos.TracePacket` (trace_packet.proto).
    pub(crate) mod trace_packet {
        /// `ClockSnapshot clock_snapshot = 6;`
        pub const CLOCK_SNAPSHOT: u32 = 6;
        /// `optional uint64 timestamp = 8;`
        pub const TIMESTAMP: u32 = 8;
        /// `uint32 trusted_packet_sequence_id = 10;`
        pub const TRUSTED_PACKET_SEQUENCE_ID: u32 = 10;
        /// `TrackEvent track_event = 11;`
        pub const TRACK_EVENT: u32 = 11;
        /// `optional InternedData interned_data = 12;`
        pub const INTERNED_DATA: u32 = 12;
        /// `optional uint32 sequence_flags = 13;`
        pub const SEQUENCE_FLAGS: u32 = 13;
        /// `optional uint32 timestamp_clock_id = 58;`
        pub const TIMESTAMP_CLOCK_ID: u32 = 58;
        /// `TracePacketDefaults trace_packet_defaults = 59;`
        pub const TRACE_PACKET_DEFAULTS: u32 = 59;
        /// `TrackDescriptor track_descriptor = 60;`
        pub const TRACK_DESCRIPTOR: u32 = 60;
        /// `optional bool first_packet_on_sequence = 87;`
        pub const FIRST_PACKET_ON_SEQUENCE: u32 = 87;

        /// `SequenceFlags.SEQ_INCREMENTAL_STATE_CLEARED`.
        pub const SEQ_INCREMENTAL_STATE_CLEARED: u64 = 1;
        /// `SequenceFlags.SEQ_NEEDS_INCREMENTAL_STATE`.
        pub const SEQ_NEEDS_INCREMENTAL_STATE: u64 = 2;
    }

    /// `perfetto.protos.TracePacketDefaults` (trace_packet_defaults.proto).
    pub(crate) mod trace_packet_defaults {
        /// `optional TrackEventDefaults track_event_defaults = 11;`
        pub const TRACK_EVENT_DEFAULTS: u32 = 11;
        /// `optional uint32 timestamp_clock_id = 58;`
        pub const TIMESTAMP_CLOCK_ID: u32 = 58;
    }

    /// `perfetto.protos.TrackEventDefaults` (track_event/track_event.proto).
    pub(crate) mod track_event_defaults {
        /// `optional uint64 track_uuid = 11;`
        pub const TRACK_UUID: u32 = 11;
    }

    /// `perfetto.protos.ClockSnapshot` / `Clock` (clock_snapshot.proto).
    pub(crate) mod clock_snapshot {
        /// `repeated Clock clocks = 1;`
        pub const CLOCKS: u32 = 1;
        /// `Clock.clock_id = 1;`
        pub const CLOCK_ID: u32 = 1;
        /// `Clock.timestamp = 2;`
        pub const CLOCK_TIMESTAMP: u32 = 2;
        /// `Clock.is_incremental = 3;`
        pub const CLOCK_IS_INCREMENTAL: u32 = 3;

        /// `BuiltinClock.BUILTIN_CLOCK_REALTIME` (common/builtin_clock.proto).
        pub const BUILTIN_CLOCK_REALTIME: u64 = 1;
        /// `BuiltinClock.BUILTIN_CLOCK_MONOTONIC`.
        pub const BUILTIN_CLOCK_MONOTONIC: u64 = 3;
        /// `BuiltinClock.BUILTIN_CLOCK_BOOTTIME`.
        pub const BUILTIN_CLOCK_BOOTTIME: u64 = 6;
        /// First sequence-scoped custom clock id (trace_packet.proto: ids
        /// 64-127 are scoped to the packet sequence that defines them).
        pub const CUSTOM_CLOCK_ID: u64 = 64;
    }

    /// `perfetto.protos.TrackEvent` (track_event/track_event.proto).
    pub(crate) mod track_event {
        /// `repeated uint64 category_iids = 3;`
        pub const CATEGORY_IIDS: u32 = 3;
        /// `repeated DebugAnnotation debug_annotations = 4;`
        pub const DEBUG_ANNOTATIONS: u32 = 4;
        /// `optional Type type = 9;`
        pub const TYPE: u32 = 9;
        /// `uint64 name_iid = 10;`
        pub const NAME_IID: u32 = 10;
        /// `optional uint64 track_uuid = 11;`
        pub const TRACK_UUID: u32 = 11;
        /// `repeated int64 extra_counter_values = 12;`
        pub const EXTRA_COUNTER_VALUES: u32 = 12;
        /// `string name = 23;`
        pub const NAME: u32 = 23;
        /// `repeated uint64 extra_counter_track_uuids = 31;`
        pub const EXTRA_COUNTER_TRACK_UUIDS: u32 = 31;
        /// `uint64 source_location_iid = 34;`
        pub const SOURCE_LOCATION_IID: u32 = 34;
        /// `repeated uint64 extra_double_counter_track_uuids = 45;`
        pub const EXTRA_DOUBLE_COUNTER_TRACK_UUIDS: u32 = 45;
        /// `repeated double extra_double_counter_values = 46;`
        pub const EXTRA_DOUBLE_COUNTER_VALUES: u32 = 46;
        /// `repeated fixed64 flow_ids = 47;`
        pub const FLOW_IDS: u32 = 47;
        /// `repeated fixed64 terminating_flow_ids = 48;`
        pub const TERMINATING_FLOW_IDS: u32 = 48;

        /// `Type.TYPE_SLICE_BEGIN`.
        pub const TYPE_SLICE_BEGIN: u64 = 1;
        /// `Type.TYPE_SLICE_END`.
        pub const TYPE_SLICE_END: u64 = 2;
        /// `Type.TYPE_INSTANT`.
        pub const TYPE_INSTANT: u64 = 3;
    }

    /// `perfetto.protos.EventName` and `EventCategory`
    /// (track_event/track_event.proto; both are iid/name pairs).
    pub(crate) mod event_name {
        /// `optional uint64 iid = 1;`
        pub const IID: u32 = 1;
        /// `optional string name = 2;`
        pub const NAME: u32 = 2;
    }

    /// `perfetto.protos.SourceLocation` (track_event/source_location.proto).
    pub(crate) mod source_location {
        /// `optional uint64 iid = 1;`
        pub const IID: u32 = 1;
        /// `optional string file_name = 2;`
        pub const FILE_NAME: u32 = 2;
        /// `optional uint32 line_number = 4;`
        pub const LINE_NUMBER: u32 = 4;
    }

    /// `perfetto.protos.TrackDescriptor` (track_event/track_descriptor.proto).
    pub(crate) mod track_descriptor {
        /// `optional uint64 uuid = 1;`
        pub const UUID: u32 = 1;
        /// `string name = 2;`
        pub const NAME: u32 = 2;
        /// `optional ProcessDescriptor process = 3;`
        pub const PROCESS: u32 = 3;
        /// `optional ThreadDescriptor thread = 4;`
        pub const THREAD: u32 = 4;
        /// `optional uint64 parent_uuid = 5;`
        pub const PARENT_UUID: u32 = 5;
        /// `optional CounterDescriptor counter = 8;`
        pub const COUNTER: u32 = 8;
    }

    /// `perfetto.protos.ProcessDescriptor` (track_event/process_descriptor.proto).
    pub(crate) mod process_descriptor {
        /// `optional int32 pid = 1;`
        pub const PID: u32 = 1;
        /// `repeated string cmdline = 2;`
        pub const CMDLINE: u32 = 2;
        /// `optional string process_name = 6;`
        pub const PROCESS_NAME: u32 = 6;
    }

    /// `perfetto.protos.ThreadDescriptor` (track_event/thread_descriptor.proto).
    pub(crate) mod thread_descriptor {
        /// `optional int32 pid = 1;`
        pub const PID: u32 = 1;
        /// `optional int64 tid = 2;`
        pub const TID: u32 = 2;
        /// `optional string thread_name = 5;`
        pub const THREAD_NAME: u32 = 5;
    }

    /// `perfetto.protos.InternedData` (interned_data/interned_data.proto).
    pub(crate) mod interned_data {
        /// `repeated EventCategory event_categories = 1;`
        pub const EVENT_CATEGORIES: u32 = 1;
        /// `repeated EventName event_names = 2;`
        pub const EVENT_NAMES: u32 = 2;
        /// `repeated DebugAnnotationName debug_annotation_names = 3;`
        pub const DEBUG_ANNOTATION_NAMES: u32 = 3;
        /// `repeated InternedString debug_annotation_string_values = 29;`
        pub const DEBUG_ANNOTATION_STRING_VALUES: u32 = 29;
        /// `repeated SourceLocation source_locations = 4;`
        pub const SOURCE_LOCATIONS: u32 = 4;
    }

    /// `perfetto.protos.DebugAnnotation` (track_event/debug_annotation.proto).
    pub(crate) mod debug_annotation {
        /// `uint64 name_iid = 1;`
        pub const NAME_IID: u32 = 1;
        /// `bool bool_value = 2;`
        pub const BOOL_VALUE: u32 = 2;
        /// `uint64 uint_value = 3;`
        pub const UINT_VALUE: u32 = 3;
        /// `int64 int_value = 4;`
        pub const INT_VALUE: u32 = 4;
        /// `double double_value = 5;`
        pub const DOUBLE_VALUE: u32 = 5;
        /// `string string_value = 6;`
        pub const STRING_VALUE: u32 = 6;
        /// `uint64 string_value_iid = 17;`
        pub const STRING_VALUE_IID: u32 = 17;
    }

    /// `perfetto.protos.InternedString` (profiling/profile_common.proto).
    pub(crate) mod interned_string {
        /// `optional uint64 iid = 1;`
        pub const IID: u32 = 1;
        /// `optional bytes str = 2;`
        pub const STR: u32 = 2;
    }

    /// `perfetto.protos.DebugAnnotationName` (track_event/debug_annotation.proto).
    pub(crate) mod debug_annotation_name {
        /// `optional uint64 iid = 1;`
        pub const IID: u32 = 1;
        /// `optional string name = 2;`
        pub const NAME: u32 = 2;
    }
}
