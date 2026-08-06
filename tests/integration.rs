// Copyright 2026 Lalit Maganti
// SPDX-License-Identifier: Apache-2.0

//! End-to-end tests: emit a trace through the layer, then decode the wire
//! format with a minimal protobuf reader and check the packet structure.

use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex};

use tracing_perfetto_file::{PerfettoLayer, SpanMode};
use tracing_subscriber::layer::SubscriberExt;

/// Minimal protobuf wire decoder, the inverse of the crate's encoder.
mod pb {
    #[derive(Debug, Clone)]
    pub enum Value {
        Varint(u64),
        Fixed64(u64),
        Bytes(Vec<u8>),
    }

    #[derive(Debug, Clone, Default)]
    pub struct Msg {
        pub fields: Vec<(u32, Value)>,
    }

    impl Msg {
        pub fn parse(mut data: &[u8]) -> Msg {
            let mut fields = Vec::new();
            while !data.is_empty() {
                let (tag, rest) = varint(data);
                data = rest;
                let field_id = (tag >> 3) as u32;
                match tag & 7 {
                    0 => {
                        let (v, rest) = varint(data);
                        data = rest;
                        fields.push((field_id, Value::Varint(v)));
                    }
                    1 => {
                        let (bytes, rest) = data.split_at(8);
                        data = rest;
                        fields.push((
                            field_id,
                            Value::Fixed64(u64::from_le_bytes(bytes.try_into().unwrap())),
                        ));
                    }
                    2 => {
                        let (len, rest) = varint(data);
                        let (bytes, rest) = rest.split_at(len as usize);
                        data = rest;
                        fields.push((field_id, Value::Bytes(bytes.to_vec())));
                    }
                    wire_type => panic!("unexpected wire type {wire_type}"),
                }
            }
            Msg { fields }
        }

        pub fn varint(&self, field_id: u32) -> Option<u64> {
            self.fields.iter().find_map(|(id, v)| match v {
                Value::Varint(v) if *id == field_id => Some(*v),
                _ => None,
            })
        }

        pub fn varints(&self, field_id: u32) -> Vec<u64> {
            self.fields
                .iter()
                .filter_map(|(id, v)| match v {
                    Value::Varint(v) if *id == field_id => Some(*v),
                    _ => None,
                })
                .collect()
        }

        pub fn fixed64s(&self, field_id: u32) -> Vec<u64> {
            self.fields
                .iter()
                .filter_map(|(id, v)| match v {
                    Value::Fixed64(v) if *id == field_id => Some(*v),
                    _ => None,
                })
                .collect()
        }

        pub fn bytes(&self, field_id: u32) -> Option<&[u8]> {
            self.fields.iter().find_map(|(id, v)| match v {
                Value::Bytes(b) if *id == field_id => Some(b.as_slice()),
                _ => None,
            })
        }

        pub fn msg(&self, field_id: u32) -> Option<Msg> {
            self.bytes(field_id).map(Msg::parse)
        }

        pub fn msgs(&self, field_id: u32) -> Vec<Msg> {
            self.fields
                .iter()
                .filter_map(|(id, v)| match v {
                    Value::Bytes(b) if *id == field_id => Some(Msg::parse(b)),
                    _ => None,
                })
                .collect()
        }

        pub fn string(&self, field_id: u32) -> Option<String> {
            self.bytes(field_id)
                .map(|b| String::from_utf8(b.to_vec()).unwrap())
        }
    }

    pub fn varint(data: &[u8]) -> (u64, &[u8]) {
        let mut value = 0u64;
        let mut shift = 0;
        for (i, byte) in data.iter().enumerate() {
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return (value, &data[i + 1..]);
            }
            shift += 7;
        }
        panic!("truncated varint");
    }
}

// TracePacket field numbers, mirrored from the crate's schema.
const CLOCK_SNAPSHOT: u32 = 6;
const TIMESTAMP: u32 = 8;
const TIMESTAMP_CLOCK_ID: u32 = 58;
const SEQUENCE_ID: u32 = 10;
const TRACK_EVENT: u32 = 11;
const INTERNED_DATA: u32 = 12;
const SEQUENCE_FLAGS: u32 = 13;
const TRACE_PACKET_DEFAULTS: u32 = 59;
const TRACK_DESCRIPTOR: u32 = 60;
const FIRST_PACKET_ON_SEQUENCE: u32 = 87;
// ClockSnapshot / Clock.
const CLOCKS: u32 = 1;
const CLOCK_ID: u32 = 1;
const CLOCK_TIMESTAMP: u32 = 2;
const CLOCK_IS_INCREMENTAL: u32 = 3;
const CUSTOM_CLOCK_ID: u64 = 64;
// TracePacketDefaults / TrackEventDefaults.
const TRACK_EVENT_DEFAULTS: u32 = 11;
const TED_TRACK_UUID: u32 = 11;
// TrackEvent.
const TE_DEBUG_ANNOTATIONS: u32 = 4;
const TE_TYPE: u32 = 9;
const TE_NAME_IID: u32 = 10;
const TE_TRACK_UUID: u32 = 11;
const TE_NAME: u32 = 23;
const TE_FLOW_IDS: u32 = 47;
const TE_TERMINATING_FLOW_IDS: u32 = 48;
const TYPE_SLICE_BEGIN: u64 = 1;
const TYPE_SLICE_END: u64 = 2;
const TYPE_INSTANT: u64 = 3;
// TrackDescriptor.
const TD_UUID: u32 = 1;
const TD_NAME: u32 = 2;
const TD_PROCESS: u32 = 3;
const TD_THREAD: u32 = 4;
const TD_PARENT_UUID: u32 = 5;
// InternedData.
const EVENT_NAMES: u32 = 2;
const DEBUG_ANNOTATION_STRING_VALUES: u32 = 29;
// DebugAnnotation.
const DA_STRING_VALUE_IID: u32 = 17;

#[derive(Clone, Default)]
struct SharedBuf(Arc<Mutex<Vec<u8>>>);

impl Write for SharedBuf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Parses the raw trace stream into TracePacket messages.
fn parse_trace(data: &[u8]) -> Vec<pb::Msg> {
    let trace = pb::Msg::parse(data);
    assert!(
        trace.fields.iter().all(|(id, _)| *id == 1),
        "top level must contain only Trace.packet fields"
    );
    trace.msgs(1)
}

/// Resolves each track event's name through the per-sequence interning
/// tables, walking packets in order.
fn resolve_event_names(packets: &[pb::Msg]) -> Vec<(u64, String)> {
    let mut interned: HashMap<u64, HashMap<u64, String>> = HashMap::new();
    let mut out = Vec::new();
    for packet in packets {
        let seq = packet.varint(SEQUENCE_ID).unwrap();
        if let Some(data) = packet.msg(INTERNED_DATA) {
            let table = interned.entry(seq).or_default();
            for entry in data.msgs(EVENT_NAMES) {
                table.insert(entry.varint(1).unwrap(), entry.string(2).unwrap());
            }
        }
        if let Some(event) = packet.msg(TRACK_EVENT) {
            if let Some(iid) = event.varint(TE_NAME_IID) {
                let name = interned
                    .get(&seq)
                    .and_then(|t| t.get(&iid))
                    .unwrap_or_else(|| panic!("unresolved name iid {iid} on sequence {seq}"));
                out.push((event.varint(TE_TYPE).unwrap(), name.clone()));
            } else if let Some(name) = event.string(TE_NAME) {
                out.push((event.varint(TE_TYPE).unwrap(), name));
            }
        }
    }
    out
}

/// Per-sequence default track uuids from `TracePacketDefaults`.
fn default_tracks(packets: &[pb::Msg]) -> HashMap<u64, u64> {
    let mut defaults = HashMap::new();
    for packet in packets {
        if let Some(d) = packet.msg(TRACE_PACKET_DEFAULTS)
            && let Some(t) = d.msg(TRACK_EVENT_DEFAULTS)
            && let Some(uuid) = t.varint(TED_TRACK_UUID)
        {
            defaults.insert(packet.varint(SEQUENCE_ID).unwrap(), uuid);
        }
    }
    defaults
}

/// The track uuid a track event targets, honoring the sequence default.
fn event_track(packet: &pb::Msg, event: &pb::Msg, defaults: &HashMap<u64, u64>) -> u64 {
    event
        .varint(TE_TRACK_UUID)
        .or_else(|| defaults.get(&packet.varint(SEQUENCE_ID).unwrap()).copied())
        .expect("event needs an explicit or default track uuid")
}

fn packet_structure_checks(packets: &[pb::Msg]) {
    let mut first_seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
    let mut incremental_clocks = std::collections::HashSet::new();
    let mut last_ts: HashMap<u64, u64> = HashMap::new();
    for packet in packets {
        let seq = packet
            .varint(SEQUENCE_ID)
            .expect("every packet has a sequence id");
        assert_ne!(seq, 0);
        let ts = packet
            .varint(TIMESTAMP)
            .expect("every packet has a timestamp");
        if let Some(snapshot) = packet.msg(CLOCK_SNAPSHOT) {
            let clock = snapshot
                .msgs(CLOCKS)
                .into_iter()
                .find(|clock| clock.varint(CLOCK_ID) == Some(CUSTOM_CLOCK_ID))
                .expect("each sequence defines its custom clock");
            assert_eq!(clock.varint(CLOCK_IS_INCREMENTAL), Some(1));
            last_ts.insert(
                seq,
                clock
                    .varint(CLOCK_TIMESTAMP)
                    .expect("custom clock has a timestamp"),
            );
            incremental_clocks.insert(seq);
        }
        // Packets with an explicit clock id are in a different domain than
        // the sequence default. Otherwise accumulate incremental timestamps.
        if packet.varint(TIMESTAMP_CLOCK_ID).is_none() {
            if incremental_clocks.contains(&seq) {
                let absolute = last_ts.entry(seq).or_default();
                *absolute = absolute.checked_add(ts).expect("timestamp overflow");
            } else {
                let prev = last_ts.insert(seq, ts).unwrap_or(0);
                assert!(ts >= prev, "timestamps must be non-decreasing per sequence");
            }
        }
        let flags = packet.varint(SEQUENCE_FLAGS);
        if first_seen.insert(seq) {
            assert_eq!(
                flags,
                Some(3),
                "first packet must clear + need incremental state"
            );
            assert_eq!(packet.varint(FIRST_PACKET_ON_SEQUENCE), Some(1));
        } else if packet.msg(TRACK_EVENT).is_some() {
            assert_eq!(flags, Some(2), "track events need incremental state");
        }
    }
    // Begin/end balance per track.
    let defaults = default_tracks(packets);
    let mut depth: HashMap<u64, i64> = HashMap::new();
    for packet in packets {
        if let Some(event) = packet.msg(TRACK_EVENT) {
            let track = event_track(packet, &event, &defaults);
            match event.varint(TE_TYPE).unwrap() {
                TYPE_SLICE_BEGIN => *depth.entry(track).or_default() += 1,
                TYPE_SLICE_END => {
                    let d = depth.entry(track).or_default();
                    *d -= 1;
                    assert!(*d >= 0, "slice end without begin on track {track}");
                }
                _ => {}
            }
        }
    }
    assert!(
        depth.values().all(|d| *d == 0),
        "unbalanced slices: {depth:?}"
    );
}

fn descriptors(packets: &[pb::Msg]) -> Vec<pb::Msg> {
    packets
        .iter()
        .filter_map(|p| p.msg(TRACK_DESCRIPTOR))
        .collect()
}

#[test]
fn thread_tracks_basic() {
    let buf = SharedBuf::default();
    let (layer, guard) = PerfettoLayer::builder(buf.clone())
        .span_mode(SpanMode::ThreadTracks)
        .with_debug_annotations()
        .build();
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        for i in 0..2u64 {
            let span = tracing::info_span!("load", index = i, kind = "disk");
            span.in_scope(|| {
                tracing::info!(bytes = 128u64, "loaded");
            });
        }
    });
    guard.flush().unwrap();

    let data = buf.0.lock().unwrap().clone();
    let packets = parse_trace(&data);
    packet_structure_checks(&packets);

    let descs = descriptors(&packets);
    assert_eq!(
        descs.iter().filter(|d| d.msg(TD_PROCESS).is_some()).count(),
        1
    );
    assert_eq!(
        descs.iter().filter(|d| d.msg(TD_THREAD).is_some()).count(),
        1
    );

    let names = resolve_event_names(&packets);
    assert_eq!(
        names
            .iter()
            .filter(|(t, n)| *t == TYPE_SLICE_BEGIN && n == "load")
            .count(),
        2
    );
    assert_eq!(names.iter().filter(|(t, _)| *t == TYPE_INSTANT).count(), 2);

    // "load" used twice must be interned exactly once.
    let mut load_interned = 0;
    for packet in &packets {
        if let Some(data) = packet.msg(INTERNED_DATA) {
            for entry in data.msgs(EVENT_NAMES) {
                if entry.string(2).unwrap() == "load" {
                    load_interned += 1;
                }
            }
        }
    }
    assert_eq!(load_interned, 1);

    // Debug annotations present on the begin events.
    let begins: Vec<_> = packets
        .iter()
        .filter_map(|p| p.msg(TRACK_EVENT))
        .filter(|e| e.varint(TE_TYPE) == Some(TYPE_SLICE_BEGIN))
        .collect();
    assert!(
        begins
            .iter()
            .all(|e| e.msgs(TE_DEBUG_ANNOTATIONS).len() == 3)
    );
}

#[test]
fn span_tracks_cross_thread() {
    let buf = SharedBuf::default();
    let (layer, guard) = PerfettoLayer::builder(buf.clone())
        .span_mode(SpanMode::SpanTracks)
        .with_poll_slices()
        .build();
    let subscriber = tracing_subscriber::registry().with(layer);
    let dispatch = tracing_core::Dispatch::new(subscriber);
    tracing::dispatcher::with_default(&dispatch, || {
        let span = tracing::info_span!("request", id = 7u64);
        // Poll the span from two other threads, like an async runtime.
        for _ in 0..2 {
            let span = span.clone();
            let dispatch = dispatch.clone();
            std::thread::spawn(move || {
                tracing::dispatcher::with_default(&dispatch, || {
                    let _entered = span.enter();
                })
            })
            .join()
            .unwrap();
        }
        drop(span);
    });
    guard.flush().unwrap();

    let data = buf.0.lock().unwrap().clone();
    let packets = parse_trace(&data);
    packet_structure_checks(&packets);

    let descs = descriptors(&packets);
    let process_uuid = descs
        .iter()
        .find(|d| d.msg(TD_PROCESS).is_some())
        .and_then(|d| d.varint(TD_UUID))
        .unwrap();
    let request_track = descs
        .iter()
        .find(|d| d.string(TD_NAME).as_deref() == Some("request"))
        .expect("span track descriptor");
    assert_eq!(request_track.varint(TD_PARENT_UUID), Some(process_uuid));
    let track_uuid = request_track.varint(TD_UUID).unwrap();

    // Lifetime slice plus two poll slices, all on the span's own track, with
    // the polls emitted from different sequences than the lifetime begin.
    let events: Vec<_> = packets
        .iter()
        .filter(|p| p.msg(TRACK_EVENT).and_then(|e| e.varint(TE_TRACK_UUID)) == Some(track_uuid))
        .collect();
    let begins = events
        .iter()
        .filter(|p| p.msg(TRACK_EVENT).unwrap().varint(TE_TYPE) == Some(1))
        .count();
    let ends = events
        .iter()
        .filter(|p| p.msg(TRACK_EVENT).unwrap().varint(TE_TYPE) == Some(2))
        .count();
    assert_eq!(begins, 3);
    assert_eq!(ends, 3);
    let sequences: std::collections::HashSet<u64> = events
        .iter()
        .map(|p| p.varint(SEQUENCE_ID).unwrap())
        .collect();
    assert!(
        sequences.len() >= 2,
        "polls should come from other threads' sequences"
    );
}

#[test]
fn follows_from_flows() {
    let buf = SharedBuf::default();
    let (layer, guard) = PerfettoLayer::builder(buf.clone())
        .span_mode(SpanMode::Both)
        .build();
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        let first = tracing::info_span!("produce");
        let second = tracing::info_span!("consume");
        second.follows_from(first.id());
        first.in_scope(|| {});
        second.in_scope(|| {});
    });
    guard.flush().unwrap();

    let data = buf.0.lock().unwrap().clone();
    let packets = parse_trace(&data);
    packet_structure_checks(&packets);

    let names = resolve_event_names(&packets);
    assert!(names.contains(&(TYPE_SLICE_BEGIN, "produce".to_owned())));
    assert!(names.contains(&(TYPE_SLICE_BEGIN, "consume".to_owned())));

    // The producer starts the flow and the consumer explicitly terminates it.
    let mut starts = Vec::new();
    let mut ends = Vec::new();
    for packet in &packets {
        if let Some(event) = packet.msg(TRACK_EVENT) {
            starts.extend(event.fixed64s(TE_FLOW_IDS));
            ends.extend(event.fixed64s(TE_TERMINATING_FLOW_IDS));
        }
    }
    assert_eq!(ends.len(), 1, "one consumer dependency endpoint");
    assert_eq!(starts.len(), 3, "one dependency plus two poll flows");
    assert_eq!(
        starts.iter().filter(|id| **id == ends[0]).count(),
        1,
        "the producer starts the dependency terminated by the consumer"
    );
}

#[test]
fn span_tracks_follows_from_migrates_external_state() {
    let buf = SharedBuf::default();
    let (layer, guard) = PerfettoLayer::builder(buf.clone())
        .span_mode(SpanMode::SpanTracks)
        .build();
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        let first = tracing::info_span!("produce");
        let second = tracing::info_span!("consume");
        second.follows_from(first.id());
        drop(first);
        drop(second);
    });
    guard.flush().unwrap();

    let data = buf.0.lock().unwrap().clone();
    let packets = parse_trace(&data);
    packet_structure_checks(&packets);
    let mut starts = Vec::new();
    let mut ends = Vec::new();
    for packet in &packets {
        if let Some(event) = packet.msg(TRACK_EVENT) {
            starts.extend(event.fixed64s(TE_FLOW_IDS));
            ends.extend(event.fixed64s(TE_TERMINATING_FLOW_IDS));
        }
    }
    assert_eq!(starts.len(), 1);
    assert_eq!(ends.len(), 1);
    assert_eq!(starts, ends);
}

#[test]
fn poll_slices_are_linked_by_default() {
    let buf = SharedBuf::default();
    let (layer, guard) = PerfettoLayer::builder(buf.clone())
        .span_mode(SpanMode::ThreadTracks)
        .build();
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        let span = tracing::info_span!("request");
        span.in_scope(|| {});
        span.in_scope(|| {});
    });
    guard.flush().unwrap();

    let data = buf.0.lock().unwrap().clone();
    let packets = parse_trace(&data);
    let flow_ids: Vec<_> = packets
        .iter()
        .filter_map(|packet| packet.msg(TRACK_EVENT))
        .filter(|event| event.varint(TE_TYPE) == Some(TYPE_SLICE_BEGIN))
        .flat_map(|event| event.fixed64s(TE_FLOW_IDS))
        .collect();
    assert_eq!(flow_ids.len(), 2);
    assert_eq!(flow_ids[0], flow_ids[1]);
}

#[test]
fn follows_from_fanout_uses_distinct_flows() {
    let buf = SharedBuf::default();
    let (layer, guard) = PerfettoLayer::builder(buf.clone())
        .span_mode(SpanMode::Both)
        .build();
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        let producer = tracing::info_span!("produce");
        let first = tracing::info_span!("consume_first");
        let second = tracing::info_span!("consume_second");
        first.follows_from(producer.id());
        second.follows_from(producer.id());
        producer.in_scope(|| {});
        first.in_scope(|| {});
        second.in_scope(|| {});
    });
    guard.flush().unwrap();

    let data = buf.0.lock().unwrap().clone();
    let packets = parse_trace(&data);
    let mut starts = Vec::new();
    let mut ends = Vec::new();
    for packet in &packets {
        if let Some(event) = packet.msg(TRACK_EVENT) {
            starts.extend(event.fixed64s(TE_FLOW_IDS));
            ends.extend(event.fixed64s(TE_TERMINATING_FLOW_IDS));
        }
    }
    ends.sort_unstable();
    assert_eq!(ends.len(), 2);
    assert_ne!(ends[0], ends[1], "each dependency is a separate edge");
    assert_eq!(starts.len(), 5, "two dependencies plus three poll flows");
    assert!(ends.iter().all(|id| starts.contains(id)));
}

#[test]
fn flush_drains_an_idle_live_thread() {
    let buf = SharedBuf::default();
    let (layer, guard) = PerfettoLayer::builder(buf.clone()).build();
    let subscriber = tracing_subscriber::registry().with(layer);
    let dispatch = tracing_core::Dispatch::new(subscriber);
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        tracing::dispatcher::with_default(&dispatch, || {
            tracing::info!("from idle worker");
            ready_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
    });

    ready_rx.recv().unwrap();
    guard.flush().unwrap();
    let data = buf.0.lock().unwrap().clone();
    let packets = parse_trace(&data);
    packet_structure_checks(&packets);
    assert_eq!(
        packets
            .iter()
            .filter_map(|packet| packet.msg(TRACK_EVENT))
            .filter(|event| event.varint(TE_TYPE) == Some(TYPE_INSTANT))
            .count(),
        1
    );

    release_tx.send(()).unwrap();
    worker.join().unwrap();
}

#[test]
fn multi_thread_stress() {
    const TE_EXTRA_COUNTER_VALUES: u32 = 12;
    const TD_COUNTER: u32 = 8;
    let buf = SharedBuf::default();
    let (layer, guard) = PerfettoLayer::builder(buf.clone()).with_counters().build();
    let subscriber = tracing_subscriber::registry().with(layer);
    let dispatch = tracing_core::Dispatch::new(subscriber);
    let threads: Vec<_> = (0..4)
        .map(|t| {
            let dispatch = dispatch.clone();
            std::thread::spawn(move || {
                tracing::dispatcher::with_default(&dispatch, || {
                    for i in 0..500u64 {
                        tracing::info_span!("work", thread = t, i).in_scope(|| {
                            tracing::info!(counter.ticks = i as i64, "tick");
                        });
                    }
                })
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    guard.flush().unwrap();

    let data = buf.0.lock().unwrap().clone();
    let packets = parse_trace(&data);
    packet_structure_checks(&packets);
    let descs = descriptors(&packets);
    assert_eq!(
        descs.iter().filter(|d| d.msg(TD_PROCESS).is_some()).count(),
        1
    );
    assert_eq!(
        descs.iter().filter(|d| d.msg(TD_THREAD).is_some()).count(),
        4
    );
    let instants = packets
        .iter()
        .filter_map(|p| p.msg(TRACK_EVENT))
        .filter(|e| e.varint(TE_TYPE) == Some(TYPE_INSTANT))
        .count();
    assert_eq!(instants, 2000);
    // All threads sample one shared counter track, and each sequence
    // re-emits its descriptor so samples never precede it in timestamp
    // order.
    let counter_descs: Vec<_> = descs
        .iter()
        .filter(|d| d.msg(TD_COUNTER).is_some())
        .collect();
    assert_eq!(counter_descs.len(), 4);
    let uuids: std::collections::HashSet<u64> = counter_descs
        .iter()
        .map(|d| d.varint(TD_UUID).unwrap())
        .collect();
    assert_eq!(uuids.len(), 1, "one counter track shared by all threads");
    let counter_samples = packets
        .iter()
        .filter_map(|p| p.msg(TRACK_EVENT))
        .map(|event| event.varints(TE_EXTRA_COUNTER_VALUES).len())
        .sum::<usize>();
    assert_eq!(counter_samples, 2000);
}

#[test]
fn both_mode_with_metadata() {
    const TE_SOURCE_LOCATION_IID: u32 = 34;
    const TE_CATEGORY_IIDS: u32 = 3;
    const SOURCE_LOCATIONS: u32 = 4;
    let buf = SharedBuf::default();
    let (layer, guard) = PerfettoLayer::builder(buf.clone())
        .span_mode(SpanMode::Both)
        .with_debug_annotations()
        .with_source_locations()
        .build();
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        let span = tracing::info_span!("outer", answer = 42u64);
        span.record("answer", 43u64);
        span.in_scope(|| {
            tracing::info!("hi");
        });
    });
    guard.flush().unwrap();

    let data = buf.0.lock().unwrap().clone();
    let packets = parse_trace(&data);
    packet_structure_checks(&packets);

    // Both mode: the span begins twice, once on its own track (lifetime)
    // and once on the thread track (the poll), plus one instant.
    let descs = descriptors(&packets);
    let span_track = descs
        .iter()
        .find(|d| d.string(TD_NAME).as_deref() == Some("outer"))
        .expect("span track exists in Both mode");
    let span_uuid = span_track.varint(TD_UUID).unwrap();
    let thread_uuid = descs
        .iter()
        .find(|d| d.msg(TD_THREAD).is_some())
        .and_then(|d| d.varint(TD_UUID))
        .unwrap();
    let defaults = default_tracks(&packets);
    let events: Vec<_> = packets
        .iter()
        .filter_map(|p| p.msg(TRACK_EVENT).map(|e| (p, e)))
        .collect();
    let begins_on = |uuid| {
        events
            .iter()
            .filter(|(p, e)| {
                e.varint(TE_TYPE) == Some(TYPE_SLICE_BEGIN) && event_track(p, e, &defaults) == uuid
            })
            .count()
    };
    assert_eq!(begins_on(span_uuid), 1, "lifetime slice on the span track");
    assert_eq!(begins_on(thread_uuid), 1, "poll slice on the thread track");

    let annotations_on = |uuid| {
        events
            .iter()
            .find(|(p, e)| {
                e.varint(TE_TYPE) == Some(TYPE_SLICE_BEGIN) && event_track(p, e, &defaults) == uuid
            })
            .unwrap()
            .1
            .msgs(TE_DEBUG_ANNOTATIONS)
            .len()
    };
    assert_eq!(annotations_on(span_uuid), 2, "field and level on lifetime");
    assert_eq!(annotations_on(thread_uuid), 1, "only level on thread poll");
    let lifetime_end = events
        .iter()
        .find(|(p, e)| {
            e.varint(TE_TYPE) == Some(TYPE_SLICE_END) && event_track(p, e, &defaults) == span_uuid
        })
        .unwrap();
    assert_eq!(
        lifetime_end.1.msgs(TE_DEBUG_ANNOTATIONS).len(),
        1,
        "record update on lifetime end"
    );

    let interned_levels: Vec<_> = packets
        .iter()
        .filter_map(|packet| packet.msg(INTERNED_DATA))
        .flat_map(|data| data.msgs(DEBUG_ANNOTATION_STRING_VALUES))
        .map(|entry| entry.string(2).unwrap())
        .collect();
    assert_eq!(interned_levels, ["INFO"]);
    assert!(events.iter().any(|(_, event)| {
        event
            .msgs(TE_DEBUG_ANNOTATIONS)
            .iter()
            .any(|annotation| annotation.varint(DA_STRING_VALUE_IID).is_some())
    }));

    // Source locations are interned per callsite and referenced by iid on
    // begin and instant events; level/target ride along as annotations.
    let mut locations: HashMap<(u64, u64), (String, u64)> = HashMap::new();
    for packet in &packets {
        let seq = packet.varint(SEQUENCE_ID).unwrap();
        if let Some(data) = packet.msg(INTERNED_DATA) {
            for entry in data.msgs(SOURCE_LOCATIONS) {
                locations.insert(
                    (seq, entry.varint(1).unwrap()),
                    (entry.string(2).unwrap(), entry.varint(4).unwrap()),
                );
            }
        }
    }
    for packet in &packets {
        let seq = packet.varint(SEQUENCE_ID).unwrap();
        let Some(event) = packet.msg(TRACK_EVENT) else {
            continue;
        };
        let ty = event.varint(TE_TYPE).unwrap();
        if ty == TYPE_SLICE_BEGIN || ty == TYPE_INSTANT {
            let iid = event
                .varint(TE_SOURCE_LOCATION_IID)
                .expect("source location iid");
            let (file, line) = &locations[&(seq, iid)];
            assert!(file.ends_with("integration.rs"));
            assert!(*line > 0);
            assert!(
                !event.msgs(TE_DEBUG_ANNOTATIONS).is_empty(),
                "level annotation"
            );
            assert!(
                event.varint(TE_CATEGORY_IIDS).is_some(),
                "target as interned category"
            );
        }
    }
}

#[test]
fn span_track_reuse_and_message_names() {
    let buf = SharedBuf::default();
    let (layer, guard) = PerfettoLayer::builder(buf.clone()).build();
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        // Three distinct callsites with the same descriptor name and parent.
        tracing::info_span!("work").in_scope(|| tracing::info!("did a thing"));
        tracing::info_span!("work").in_scope(|| tracing::info!("did a thing"));
        tracing::info_span!("work").in_scope(|| tracing::info!("did a thing"));
    });
    guard.flush().unwrap();

    let data = buf.0.lock().unwrap().clone();
    let packets = parse_trace(&data);
    packet_structure_checks(&packets);

    // Sequential same-name spans reuse one pooled track: exactly one
    // descriptor despite coming from three callsites.
    let work_descriptors = descriptors(&packets)
        .iter()
        .filter(|d| d.string(TD_NAME).as_deref() == Some("work"))
        .count();
    assert_eq!(work_descriptors, 1);

    // Unnamed events take their message as the display name.
    let names = resolve_event_names(&packets);
    assert_eq!(
        names
            .iter()
            .filter(|(t, n)| *t == TYPE_INSTANT && n == "did a thing")
            .count(),
        3
    );
}

#[test]
fn counter_tracks() {
    const TE_EXTRA_COUNTER_VALUES: u32 = 12;
    const TE_EXTRA_COUNTER_TRACK_UUIDS: u32 = 31;
    const TD_COUNTER: u32 = 8;
    let buf = SharedBuf::default();
    let (layer, guard) = PerfettoLayer::builder(buf.clone())
        .with_debug_annotations()
        .with_counters()
        .build();
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        for i in 0..5i64 {
            tracing::info!(counter.queue_depth = i, other = "kept", "tick");
        }
    });
    guard.flush().unwrap();

    let data = buf.0.lock().unwrap().clone();
    let packets = parse_trace(&data);
    packet_structure_checks(&packets);

    // One counter track named after the field minus the prefix.
    let counter_descs: Vec<_> = descriptors(&packets)
        .into_iter()
        .filter(|d| d.msg(TD_COUNTER).is_some())
        .collect();
    assert_eq!(counter_descs.len(), 1);
    assert_eq!(
        counter_descs[0].string(TD_NAME).as_deref(),
        Some("queue_depth")
    );
    let counter_uuid = counter_descs[0].varint(TD_UUID).unwrap();

    // Five samples are attached directly to their instant events. The counter
    // field is not also emitted as an annotation, while other fields are.
    let mut values = Vec::new();
    for packet in &packets {
        let Some(event) = packet.msg(TRACK_EVENT) else {
            continue;
        };
        if event.varint(TE_TYPE) == Some(TYPE_INSTANT) {
            assert_eq!(event.varints(TE_EXTRA_COUNTER_TRACK_UUIDS), [counter_uuid],);
            values.extend(event.varints(TE_EXTRA_COUNTER_VALUES));
            assert_eq!(event.msgs(TE_DEBUG_ANNOTATIONS).len(), 2);
        }
    }
    assert_eq!(values, [0, 1, 2, 3, 4]);
}

#[test]
fn counters_above_the_extra_counter_limit_become_annotations() {
    const DA_INT_VALUE: u32 = 4;
    const TE_EXTRA_COUNTER_VALUES: u32 = 12;
    const TE_EXTRA_COUNTER_TRACK_UUIDS: u32 = 31;
    const TD_COUNTER: u32 = 8;
    let buf = SharedBuf::default();
    let (layer, guard) = PerfettoLayer::builder(buf.clone()).with_counters().build();
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        tracing::info!(
            counter.c0 = 0i64,
            counter.c1 = 1i64,
            counter.c2 = 2i64,
            counter.c3 = 3i64,
            counter.c4 = 4i64,
            counter.c5 = 5i64,
            counter.c6 = 6i64,
            counter.c7 = 7i64,
            counter.c8 = 8i64,
            counter.c9 = 9i64,
            "tick",
        );
    });
    guard.flush().unwrap();

    let data = buf.0.lock().unwrap().clone();
    let packets = parse_trace(&data);
    packet_structure_checks(&packets);
    assert_eq!(
        descriptors(&packets)
            .into_iter()
            .filter(|descriptor| descriptor.msg(TD_COUNTER).is_some())
            .count(),
        8,
    );
    let event = packets
        .iter()
        .find_map(|packet| packet.msg(TRACK_EVENT))
        .filter(|event| event.varint(TE_TYPE) == Some(TYPE_INSTANT))
        .expect("instant event");
    assert_eq!(event.varints(TE_EXTRA_COUNTER_TRACK_UUIDS).len(), 8);
    assert_eq!(
        event.varints(TE_EXTRA_COUNTER_VALUES),
        [0, 1, 2, 3, 4, 5, 6, 7]
    );
    let overflow: Vec<_> = event
        .msgs(TE_DEBUG_ANNOTATIONS)
        .into_iter()
        .filter_map(|annotation| annotation.varint(DA_INT_VALUE))
        .collect();
    assert_eq!(overflow, [8, 9]);
}

#[test]
fn write_error_is_sticky_and_surfaced() {
    struct FailingWriter;
    impl Write for FailingWriter {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("disk full"))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let (layer, guard) = PerfettoLayer::builder(FailingWriter).build();
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        for _ in 0..100 {
            tracing::info_span!("s").in_scope(|| {});
        }
    });
    let err = guard.flush().unwrap_err();
    assert!(err.to_string().contains("disk full"));
}
