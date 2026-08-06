// Copyright 2026 Lalit Maganti
// SPDX-License-Identifier: Apache-2.0

//! Private tracing callback implementation.

use std::num::NonZeroU64;
use std::sync::Arc;

use tracing_core::span::{Attributes, Id, Record};
use tracing_core::{Event, Level, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

use crate::emit;
use crate::emit::TrackEventWriter;
use crate::emit::schema::trace_packet;
use crate::runtime::{FieldValue, Inner, PendingFlow};
use crate::sequence::{SequenceState, SequenceWriter};
use crate::span_state::{SpanData, TrackKey};
use crate::thread::acquire;
use crate::visitor::{AttributeVisitor, EventVisitor, StoreVisitor};
use crate::{PerfettoLayer, SpanMode};

const LEVEL_NAME_IID: u64 = 1;
const TRACE_VALUE_IID: u64 = 1;
const DEBUG_VALUE_IID: u64 = 2;
const INFO_VALUE_IID: u64 = 3;
const WARN_VALUE_IID: u64 = 4;
const ERROR_VALUE_IID: u64 = 5;

impl SpanData {
    #[inline(always)]
    fn fields(&self) -> &[(&'static str, FieldValue)] {
        match &self.retained {
            Some(retained) => retained.fields.as_slice(),
            None => &[],
        }
    }

    #[inline(always)]
    fn pending_flows(&self) -> &[PendingFlow] {
        match &self.retained {
            Some(retained) => retained.pending_flows.as_slice(),
            None => &[],
        }
    }

    #[inline(always)]
    fn clear_pending_flows(&mut self) {
        if let Some(retained) = &mut self.retained {
            retained.pending_flows.clear();
        }
    }

    #[inline(always)]
    fn queue_flow(&mut self, inner: &Arc<Inner>, flow: PendingFlow) {
        if self.retained.is_none() {
            self.retained = Some(inner.take_retained_span());
        }
        self.retained
            .as_mut()
            .expect("retained span data initialized")
            .pending_flows
            .push(flow);
    }
}

#[cold]
#[inline(never)]
fn create_span_track(inner: &Inner, writer: &mut SequenceWriter<'_>, key: TrackKey) -> u64 {
    let uuid = inner.alloc_uuid();
    emit::track_descriptor(&mut writer.packet(), uuid, key.name, key.parent_uuid);
    uuid
}

#[cold]
#[inline(never)]
fn create_counter_track(inner: &Inner, writer: &mut SequenceWriter<'_>, name: &'static str) {
    let uuid = inner.counter_track_uuid(name.as_ptr() as usize);
    emit::counter_descriptor(
        &mut writer.packet(),
        uuid,
        name.strip_prefix("counter.").unwrap_or(name),
        inner.process_track_uuid(),
    );
    writer.sequence().cache_counter_track(name, uuid);
}

#[inline(always)]
fn write_metadata(
    inner: &Inner,
    metadata: &'static Metadata<'static>,
    sequence: &mut SequenceState,
    event: &mut TrackEventWriter<'_>,
) {
    // Source locations are interned by callsite and emitted once per sequence.
    if inner.config.source_locations
        && let (Some(file), Some(line)) = (metadata.file(), metadata.line())
    {
        event.source_location_iid(sequence.intern_source_location(
            std::ptr::from_ref(metadata) as usize,
            file,
            line,
        ));
    }

    // Levels use reserved interned IDs so every sequence encodes them identically.
    let (value_iid, value, bit) = match *metadata.level() {
        Level::TRACE => (TRACE_VALUE_IID, "TRACE", 1 << 0),
        Level::DEBUG => (DEBUG_VALUE_IID, "DEBUG", 1 << 1),
        Level::INFO => (INFO_VALUE_IID, "INFO", 1 << 2),
        Level::WARN => (WARN_VALUE_IID, "WARN", 1 << 3),
        Level::ERROR => (ERROR_VALUE_IID, "ERROR", 1 << 4),
    };
    if !sequence.level_name_seen() {
        sequence.register_level_name();
    }
    if !sequence.level_value_seen(bit) {
        sequence.register_level_value(bit, value_iid, value);
    }
    event.level_annotation(LEVEL_NAME_IID, value_iid);

    // The tracing target becomes the Perfetto event category.
    event.category_iid(sequence.intern_category(metadata.target()));
}

#[inline(always)]
fn write_pending_flows(event: &mut TrackEventWriter<'_>, flows: &[PendingFlow]) {
    for flow in flows {
        match *flow {
            PendingFlow::Start(id) => event.flow_start(id),
            PendingFlow::Terminate(id) => event.flow_terminate(id),
        }
    }
}

#[inline(always)]
fn write_stored_fields(
    event: &mut TrackEventWriter<'_>,
    sequence: &mut SequenceState,
    fields: &[(&'static str, FieldValue)],
) {
    for (name, value) in fields {
        let name_iid = sequence.intern_annotation_name(name);
        match value {
            FieldValue::Bool(value) => event.annotation_bool(name_iid, *value),
            FieldValue::I64(value) => event.annotation_i64(name_iid, *value),
            FieldValue::U64(value) => event.annotation_u64(name_iid, *value),
            FieldValue::F64(value) => event.annotation_f64(name_iid, *value),
            FieldValue::Str(value) => event.annotation_str(name_iid, value),
        }
    }
}

// Encoding scopes intentionally bind writer, packet, message, and event in
// sequence. Each child borrows its parent, and dropping the parent finalizes
// packet output or nested-message framing.
impl<S> Layer<S> for PerfettoLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    #[inline(always)]
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let inner = &self.inner;
        if inner.has_error() {
            return;
        }
        let Some(span) = ctx.span(id) else { return };

        // Retain state only for opt-in thread slices that need fields or a poll flow.
        let retain_fields =
            inner.config.debug_annotations && inner.config.span_mode == SpanMode::ThreadTracks;
        let thread_slices = inner.config.span_mode.emit_thread_slices();
        let retained = if retain_fields || thread_slices {
            Some(inner.take_retained_span())
        } else {
            None
        };
        let mut data = SpanData::new(retained);
        if let Some(retained) = &mut data.retained {
            if retain_fields {
                attrs.record(&mut StoreVisitor {
                    fields: &mut retained.fields,
                });
            }
            if thread_slices {
                retained.poll_flow_id = NonZeroU64::new(inner.alloc_flow_id());
            }
        }

        // Thread-only mode needs state but does not emit a lifetime slice.
        if !inner.config.span_mode.emit_span_slices() {
            inner.span_store.insert(id.into_u64(), data);
            return;
        }

        // Lifetime tracks mirror the span hierarchy, rooted at the process track.
        let metadata = span.metadata();
        let key = TrackKey {
            name: metadata.name(),
            parent_uuid: span
                .parent()
                .and_then(|parent| {
                    inner
                        .span_store
                        .get(parent.id().into_u64())
                        .and_then(|data| data.track_uuid.map(NonZeroU64::get))
                })
                .unwrap_or_else(|| inner.process_track_uuid()),
        };

        // Claim a descriptor-compatible track, creating one on a cache miss.
        let Some(mut thread) = acquire(inner) else {
            inner.span_store.insert(id.into_u64(), data);
            return;
        };
        let (track_pool, reusable_uuid) = match thread.claim_track(inner, key) {
            Some(claim) => (Some(claim.pool), claim.uuid),
            None => (None, None),
        };
        let mut writer = SequenceWriter::new(inner, &mut thread);
        let track_uuid =
            reusable_uuid.unwrap_or_else(|| create_span_track(inner, &mut writer, key));

        // Emit the lifetime slice begin and its initial attributes.
        {
            let mut packet = writer.packet();
            let sequence = &mut *packet.sequence;
            let mut message = packet.proto.message(trace_packet::TRACK_EVENT);
            let mut event = TrackEventWriter::new(&mut message);
            event.slice_begin();
            event.track_uuid(track_uuid);
            event.name_iid(sequence.intern_event_name(metadata.name()));
            write_metadata(inner, metadata, sequence, &mut event);
            if inner.config.debug_annotations {
                attrs.record(&mut AttributeVisitor::new(&mut event, sequence));
            }
        }

        // Publish state only after the lifetime track is ready for later callbacks.
        data.track_pool = track_pool;
        data.track_uuid = NonZeroU64::new(track_uuid);
        inner.span_store.insert(id.into_u64(), data);
    }

    #[inline(always)]
    fn on_record(&self, id: &Id, values: &Record<'_>, _ctx: Context<'_, S>) {
        let inner = &self.inner;
        if !inner.config.debug_annotations || inner.has_error() {
            return;
        }
        let Some(mut data) = inner.span_store.get_mut(id.into_u64()) else {
            return;
        };
        // Records after creation are retained for the lifetime slice end.
        if data.retained.is_none() {
            data.retained = Some(inner.take_retained_span());
        }
        let retained = data
            .retained
            .as_mut()
            .expect("retained span data initialized");
        values.record(&mut StoreVisitor {
            fields: &mut retained.fields,
        });
    }

    #[inline(always)]
    fn on_follows_from(&self, id: &Id, follows: &Id, ctx: Context<'_, S>) {
        let inner = &self.inner;
        if inner.has_error() {
            return;
        }
        if ctx.span(id).is_none() || ctx.span(follows).is_none() {
            return;
        }

        // Queue each end of the relationship on the callback that will emit it.
        inner.note_follows_from();
        let flow = inner.alloc_flow_id();

        // The predecessor starts the flow at its next emitted boundary.
        {
            let Some(mut data) = inner.span_store.get_mut(follows.into_u64()) else {
                return;
            };
            data.queue_flow(inner, PendingFlow::Start(flow));
        }

        // The dependent span terminates the same flow when it next runs.
        let Some(mut data) = inner.span_store.get_mut(id.into_u64()) else {
            return;
        };
        data.queue_flow(inner, PendingFlow::Terminate(flow));
    }

    #[inline(always)]
    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        let inner = &self.inner;

        // Decide whether this enter produces a thread slice or a poll slice.
        let thread_slices = inner.config.span_mode.emit_thread_slices();
        if (!thread_slices && !inner.config.poll_slices) || inner.has_error() {
            return;
        }
        let Some(span) = ctx.span(id) else { return };
        let Some(mut thread) = acquire(inner) else {
            return;
        };
        let Some(mut data) = inner.span_store.get_mut(id.into_u64()) else {
            return;
        };

        // Poll slices require the lifetime track created by `on_new_span`.
        let poll_track_uuid = if thread_slices {
            None
        } else {
            let Some(uuid) = data.track_uuid else { return };
            Some(uuid.get())
        };
        let has_flows = inner.has_follows_from();

        // Encode the slice before consuming its queued flow relationships.
        {
            let mut writer = SequenceWriter::new(inner, &mut thread);
            let mut packet = writer.packet();
            let sequence = &mut *packet.sequence;
            let mut message = packet.proto.message(trace_packet::TRACK_EVENT);
            let mut event = TrackEventWriter::new(&mut message);
            event.slice_begin();

            // Thread slices carry span metadata; poll slices use the lifetime track.
            if thread_slices {
                let metadata = span.metadata();
                event.name_iid(sequence.intern_event_name(metadata.name()));
                write_metadata(inner, metadata, sequence, &mut event);
                if inner.config.debug_annotations
                    && inner.config.span_mode == SpanMode::ThreadTracks
                {
                    write_stored_fields(&mut event, sequence, data.fields());
                }
            } else if let Some(uuid) = poll_track_uuid {
                event.track_uuid(uuid);
                event.name_iid(sequence.intern_event_name("poll"));
            }

            // Poll flows link repeated enters; queued flows represent `follows_from`.
            if let Some(retained) = &data.retained
                && let Some(flow) = retained.poll_flow_id
            {
                event.flow_start(flow.get());
            }
            if has_flows {
                write_pending_flows(&mut event, data.pending_flows());
            }
        }
        if has_flows {
            data.clear_pending_flows();
        }
    }

    #[inline(always)]
    fn on_exit(&self, id: &Id, _ctx: Context<'_, S>) {
        let inner = &self.inner;

        // Mirror the slice kind selected by `on_enter`.
        let thread_slices = inner.config.span_mode.emit_thread_slices();
        if (!thread_slices && !inner.config.poll_slices) || inner.has_error() {
            return;
        }
        let Some(mut thread) = acquire(inner) else {
            return;
        };
        let Some(mut data) = inner.span_store.get_mut(id.into_u64()) else {
            return;
        };
        // Poll slice ends must target the same lifetime track as their begin.
        let poll_track_uuid = if thread_slices {
            None
        } else {
            let Some(uuid) = data.track_uuid else { return };
            Some(uuid.get())
        };
        let include_flows = thread_slices && inner.has_follows_from();

        // Emit the end before removing any flow relationships it carries.
        {
            let mut writer = SequenceWriter::new(inner, &mut thread);
            let mut packet = writer.packet();
            let mut message = packet.proto.message(trace_packet::TRACK_EVENT);
            let mut event = TrackEventWriter::new(&mut message);
            event.slice_end();
            if let Some(uuid) = poll_track_uuid {
                event.track_uuid(uuid);
            }
            if include_flows {
                write_pending_flows(&mut event, data.pending_flows());
            }
        }
        if include_flows {
            data.clear_pending_flows();
        }
    }

    #[inline(always)]
    fn on_close(&self, id: Id, _ctx: Context<'_, S>) {
        let inner = &self.inner;

        // Remove the state first so no later callback can observe a closed span.
        let Some(mut data) = inner.span_store.take(id.into_u64()) else {
            return;
        };
        let track_pool = data.track_pool.take();
        let track_uuid = data.track_uuid.map(NonZeroU64::get);
        let mut thread = if track_uuid.is_some() && !inner.has_error() {
            acquire(inner)
        } else {
            None
        };

        // Finish the lifetime slice while its fields and flows are still available.
        if let Some(uuid) = track_uuid
            && let Some(thread) = thread.as_mut()
        {
            let mut writer = SequenceWriter::new(inner, thread);
            let mut packet = writer.packet();
            let mut message = packet.proto.message(trace_packet::TRACK_EVENT);
            let mut event = TrackEventWriter::new(&mut message);
            event.slice_end();
            event.track_uuid(uuid);
            write_pending_flows(&mut event, data.pending_flows());
            if inner.config.debug_annotations {
                write_stored_fields(&mut event, &mut *packet.sequence, data.fields());
            }
        }

        // Return reusable tracks locally when possible.
        if let Some(pool) = track_pool
            && let Some(uuid) = track_uuid
        {
            if let Some(thread) = thread.as_mut() {
                thread.release_track(&pool, uuid);
            } else {
                pool.release(uuid);
            }
        }
        if let Some(retained) = data.retained.take() {
            match thread.as_mut() {
                Some(thread) => thread.return_retained_span(retained),
                None => inner.recycle_retained_span(retained),
            }
        }
    }

    #[inline(always)]
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let inner = &self.inner;
        if inner.has_error() {
            return;
        }

        let metadata = event.metadata();
        let Some(mut thread) = acquire(inner) else {
            return;
        };

        // Pure span-track mode places instants on the enclosing lifetime track.
        let thread_track_uuid = thread.thread_track_uuid();
        let track_uuid = if inner.config.span_mode == SpanMode::SpanTracks {
            ctx.event_span(event)
                .and_then(|span| {
                    inner
                        .span_store
                        .get(span.id().into_u64())
                        .and_then(|data| data.track_uuid.map(NonZeroU64::get))
                })
                .unwrap_or(thread_track_uuid)
        } else {
            thread_track_uuid
        };
        let mut writer = SequenceWriter::new(inner, &mut thread);

        // Counter descriptors must precede the event that references them.
        if inner.config.counters {
            for field in metadata
                .fields()
                .into_iter()
                .filter(|field| field.name().starts_with("counter."))
                .take(emit::MAX_EXTRA_COUNTERS)
            {
                if writer.sequence().counter_track_uuid(field.name()).is_none() {
                    create_counter_track(inner, &mut writer, field.name());
                }
            }
        }

        // Encode the instant and let the visitor append fields and counter values.
        let mut packet = writer.packet();
        let sequence = &mut *packet.sequence;
        let mut message = packet.proto.message(trace_packet::TRACK_EVENT);
        let mut track_event = TrackEventWriter::new(&mut message);
        track_event.instant();
        if track_uuid != thread_track_uuid {
            track_event.track_uuid(track_uuid);
        }
        write_metadata(inner, metadata, sequence, &mut track_event);
        // Unnamed tracing events use `message`; explicitly named events keep metadata.
        let wrote_name = {
            let mut visitor = EventVisitor::new(
                &mut track_event,
                sequence,
                inner.config.debug_annotations,
                inner.config.counters,
                metadata.name().starts_with("event "),
            );
            event.record(&mut visitor);
            visitor.wrote_name()
        };
        if !wrote_name {
            track_event.name_iid(sequence.intern_event_name(metadata.name()));
        }
    }
}
