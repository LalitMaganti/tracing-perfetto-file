// Copyright 2026 Lalit Maganti
// SPDX-License-Identifier: Apache-2.0

//! Tracing field visitors for inline emission and retained span fields.

use std::fmt;

use tracing_core::field::{Field, Visit};

use crate::emit::{self, TrackEventWriter};
use crate::runtime::FieldValue;
use crate::sequence::SequenceState;

/// Collects span fields into owned storage (spans outlive the callback that
/// carries their field data). Per `tracing` semantics a re-recorded field
/// replaces the previous value.
pub(crate) struct StoreVisitor<'a> {
    pub fields: &'a mut Vec<(&'static str, FieldValue)>,
}

impl StoreVisitor<'_> {
    fn set(&mut self, name: &'static str, value: FieldValue) {
        match self.fields.iter_mut().find(|(n, _)| *n == name) {
            Some(slot) => slot.1 = value,
            None => self.fields.push((name, value)),
        }
    }
}

impl Visit for StoreVisitor<'_> {
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.set(field.name(), FieldValue::Bool(value));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.set(field.name(), FieldValue::I64(value));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.set(field.name(), FieldValue::U64(value));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.set(field.name(), FieldValue::F64(value));
    }

    fn record_i128(&mut self, field: &Field, value: i128) {
        let value = match i64::try_from(value) {
            Ok(v) => FieldValue::I64(v),
            Err(_) => FieldValue::Str(value.to_string()),
        };
        self.set(field.name(), value);
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        let value = match u64::try_from(value) {
            Ok(v) => FieldValue::U64(v),
            Err(_) => FieldValue::Str(value.to_string()),
        };
        self.set(field.name(), value);
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.set(field.name(), FieldValue::Str(value.to_owned()));
    }

    fn record_bytes(&mut self, field: &Field, value: &[u8]) {
        self.set(field.name(), FieldValue::Str(format!("{value:02x?}")));
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.set(field.name(), FieldValue::Str(value.to_string()));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.set(field.name(), FieldValue::Str(format!("{value:?}")));
    }
}

pub(crate) struct AttributeVisitor<'writer, 'buffer> {
    event: &'writer mut TrackEventWriter<'buffer>,
    sequence: &'writer mut SequenceState,
}

impl<'writer, 'buffer> AttributeVisitor<'writer, 'buffer> {
    #[inline(always)]
    pub(crate) fn new(
        event: &'writer mut TrackEventWriter<'buffer>,
        sequence: &'writer mut SequenceState,
    ) -> Self {
        Self { event, sequence }
    }

    #[inline(always)]
    fn name_iid(&mut self, field: &Field) -> u64 {
        self.sequence.intern_annotation_name(field.name())
    }
}

impl Visit for AttributeVisitor<'_, '_> {
    fn record_bool(&mut self, field: &Field, value: bool) {
        let name_iid = self.name_iid(field);
        self.event.annotation_bool(name_iid, value);
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        let name_iid = self.name_iid(field);
        self.event.annotation_i64(name_iid, value);
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        let name_iid = self.name_iid(field);
        self.event.annotation_u64(name_iid, value);
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        let name_iid = self.name_iid(field);
        self.event.annotation_f64(name_iid, value);
    }

    fn record_i128(&mut self, field: &Field, value: i128) {
        let name_iid = self.name_iid(field);
        match i64::try_from(value) {
            Ok(value) => self.event.annotation_i64(name_iid, value),
            Err(_) => self.event.annotation_display(name_iid, &value),
        }
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        let name_iid = self.name_iid(field);
        match u64::try_from(value) {
            Ok(value) => self.event.annotation_u64(name_iid, value),
            Err(_) => self.event.annotation_display(name_iid, &value),
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        let name_iid = self.name_iid(field);
        self.event.annotation_str(name_iid, value);
    }

    fn record_bytes(&mut self, field: &Field, value: &[u8]) {
        let name_iid = self.name_iid(field);
        self.event
            .annotation_debug(name_iid, &format_args!("{value:02x?}"));
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        let name_iid = self.name_iid(field);
        self.event.annotation_display(name_iid, &value);
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        let name_iid = self.name_iid(field);
        self.event.annotation_debug(name_iid, value);
    }
}

pub(crate) struct EventVisitor<'writer, 'buffer> {
    event: &'writer mut TrackEventWriter<'buffer>,
    sequence: &'writer mut SequenceState,
    annotations: bool,
    counters: bool,
    counter_count: usize,
    use_message_as_name: bool,
    wrote_name: bool,
}

impl<'writer, 'buffer> EventVisitor<'writer, 'buffer> {
    #[inline(always)]
    pub(crate) fn new(
        event: &'writer mut TrackEventWriter<'buffer>,
        sequence: &'writer mut SequenceState,
        annotations: bool,
        counters: bool,
        use_message_as_name: bool,
    ) -> Self {
        Self {
            event,
            sequence,
            annotations,
            counters,
            counter_count: 0,
            use_message_as_name,
            wrote_name: false,
        }
    }

    #[inline(always)]
    pub(crate) fn wrote_name(&self) -> bool {
        self.wrote_name
    }

    #[inline(always)]
    fn annotation_name(&mut self, field: &Field) -> u64 {
        self.sequence.intern_annotation_name(field.name())
    }

    #[inline(always)]
    fn counter_uuid(&self, field: &Field) -> Option<u64> {
        self.sequence.counter_track_uuid(field.name())
    }
}

impl Visit for EventVisitor<'_, '_> {
    fn record_bool(&mut self, field: &Field, value: bool) {
        if !self.annotations {
            return;
        }
        let name_iid = self.annotation_name(field);
        self.event.annotation_bool(name_iid, value);
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        let is_counter = self.counters && field.name().starts_with("counter.");
        if is_counter
            && self.counter_count < emit::MAX_EXTRA_COUNTERS
            && let Some(track_uuid) = self.counter_uuid(field)
        {
            self.event.counter_i64(track_uuid, value);
            self.counter_count += 1;
            return;
        }
        if is_counter || self.annotations {
            let name_iid = self.annotation_name(field);
            self.event.annotation_i64(name_iid, value);
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        let is_counter = self.counters && field.name().starts_with("counter.");
        if is_counter
            && self.counter_count < emit::MAX_EXTRA_COUNTERS
            && let Some(track_uuid) = self.counter_uuid(field)
        {
            self.event.counter_i64(track_uuid, value as i64);
            self.counter_count += 1;
            return;
        }
        if is_counter || self.annotations {
            let name_iid = self.annotation_name(field);
            self.event.annotation_u64(name_iid, value);
        }
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        let is_counter = self.counters && field.name().starts_with("counter.");
        if is_counter
            && self.counter_count < emit::MAX_EXTRA_COUNTERS
            && let Some(track_uuid) = self.counter_uuid(field)
        {
            self.event.counter_f64(track_uuid, value);
            self.counter_count += 1;
            return;
        }
        if is_counter || self.annotations {
            let name_iid = self.annotation_name(field);
            self.event.annotation_f64(name_iid, value);
        }
    }

    fn record_i128(&mut self, field: &Field, value: i128) {
        if !self.annotations {
            return;
        }
        let name_iid = self.annotation_name(field);
        match i64::try_from(value) {
            Ok(value) => self.event.annotation_i64(name_iid, value),
            Err(_) => self.event.annotation_display(name_iid, &value),
        }
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        if !self.annotations {
            return;
        }
        let name_iid = self.annotation_name(field);
        match u64::try_from(value) {
            Ok(value) => self.event.annotation_u64(name_iid, value),
            Err(_) => self.event.annotation_display(name_iid, &value),
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if self.use_message_as_name && !self.wrote_name && field.name() == "message" {
            self.wrote_name = true;
            self.event.event_name_str(value);
            return;
        }
        if !self.annotations {
            return;
        }
        let name_iid = self.annotation_name(field);
        self.event.annotation_str(name_iid, value);
    }

    fn record_bytes(&mut self, field: &Field, value: &[u8]) {
        if !self.annotations {
            return;
        }
        let name_iid = self.annotation_name(field);
        self.event
            .annotation_debug(name_iid, &format_args!("{value:02x?}"));
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        if !self.annotations {
            return;
        }
        let name_iid = self.annotation_name(field);
        self.event.annotation_display(name_iid, &value);
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if self.use_message_as_name && !self.wrote_name && field.name() == "message" {
            self.wrote_name = true;
            self.event.event_name_debug(value);
            return;
        }
        if !self.annotations {
            return;
        }
        let name_iid = self.annotation_name(field);
        self.event.annotation_debug(name_iid, value);
    }
}
