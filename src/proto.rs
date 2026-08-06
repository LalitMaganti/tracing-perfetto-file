// Copyright 2026 Lalit Maganti
// SPDX-License-Identifier: Apache-2.0

//! Minimal hand-written protobuf wire-format encoder.
//!
//! Only the pieces needed to emit Perfetto `Trace` messages: varints,
//! fixed64/double values and length-delimited (nested message / string)
//! fields.

use std::ops::{Deref, DerefMut};

/// Protobuf wire types used by the Perfetto trace schema.
#[derive(Clone, Copy)]
pub(crate) enum WireType {
    Varint = 0,
    Fixed64 = 1,
    Delimited = 2,
}

/// Appends `value` to `out` as a base-128 varint.
#[inline(always)]
pub(crate) fn write_varint(mut value: u64, out: &mut Vec<u8>) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

// As in Perfetto's protozero encoder, reserve four bytes for message lengths.
// This supports messages up to 256 MiB and lets us backfill lengths without
// moving the already-written payload.
const MESSAGE_LEN_PREFIX_SIZE: usize = 4;
const MAX_MESSAGE_LEN: usize = (1 << (MESSAGE_LEN_PREFIX_SIZE * 7)) - 1;

/// Writes `value` as a fixed-width, non-canonical protobuf varint.
#[inline(always)]
fn write_redundant_varint(mut value: usize, out: &mut [u8]) {
    assert!(
        value <= MAX_MESSAGE_LEN,
        "length-delimited protobuf field exceeds 256 MiB"
    );
    let last = out.len() - 1;
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = value as u8 | if i == last { 0 } else { 0x80 };
        value >>= 7;
    }
}

/// Append-only protobuf message builder over a reusable byte buffer.
#[derive(Default)]
pub(crate) struct ProtoBuffer {
    buf: Vec<u8>,
}

#[derive(Clone, Copy)]
pub(crate) struct MessageToken {
    len_prefix: usize,
    payload_start: usize,
}

/// A nested protobuf message whose length is backfilled when dropped.
pub(crate) struct Message<'a> {
    buffer: &'a mut ProtoBuffer,
    token: MessageToken,
}

impl Deref for Message<'_> {
    type Target = ProtoBuffer;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.buffer
    }
}

impl DerefMut for Message<'_> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buffer
    }
}

impl Drop for Message<'_> {
    #[inline(always)]
    fn drop(&mut self) {
        self.buffer.finish_message(self.token);
    }
}

impl ProtoBuffer {
    pub(crate) fn new() -> Self {
        ProtoBuffer {
            buf: Vec::with_capacity(256),
        }
    }

    pub(crate) fn clear(&mut self) {
        self.buf.clear();
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    pub(crate) fn truncate(&mut self, len: usize) {
        self.buf.truncate(len);
    }

    #[inline(always)]
    fn tag(&mut self, field_id: u32, wire_type: WireType) {
        write_varint(((field_id << 3) | wire_type as u32) as u64, &mut self.buf);
    }

    /// Writes a varint field (proto types: uint32/uint64/int32/int64/bool/enum).
    /// Signed values must be cast to u64 first (proto2 int64 semantics).
    #[inline(always)]
    pub fn varint_field(&mut self, field_id: u32, value: u64) {
        self.tag(field_id, WireType::Varint);
        write_varint(value, &mut self.buf);
    }

    /// Writes a uint64 field as a valid fixed-width ten-byte protobuf varint.
    #[inline(always)]
    pub fn redundant_varint_field(&mut self, field_id: u32, value: u64) {
        self.tag(field_id, WireType::Varint);
        let encoded = [
            value as u8 | 0x80,
            (value >> 7) as u8 | 0x80,
            (value >> 14) as u8 | 0x80,
            (value >> 21) as u8 | 0x80,
            (value >> 28) as u8 | 0x80,
            (value >> 35) as u8 | 0x80,
            (value >> 42) as u8 | 0x80,
            (value >> 49) as u8 | 0x80,
            (value >> 56) as u8 | 0x80,
            (value >> 63) as u8,
        ];
        self.buf.extend_from_slice(&encoded);
    }

    /// Writes a fixed64 field.
    #[inline(always)]
    pub fn fixed64_field(&mut self, field_id: u32, value: u64) {
        self.tag(field_id, WireType::Fixed64);
        self.buf.extend_from_slice(&value.to_le_bytes());
    }

    /// Writes a double field.
    #[inline(always)]
    pub fn double_field(&mut self, field_id: u32, value: f64) {
        self.fixed64_field(field_id, value.to_bits());
    }

    /// Writes a length-delimited string field.
    #[inline(always)]
    pub fn string_field(&mut self, field_id: u32, value: &str) {
        self.tag(field_id, WireType::Delimited);
        write_varint(value.len() as u64, &mut self.buf);
        self.buf.extend_from_slice(value.as_bytes());
    }

    /// Writes a `Debug`-formatted string field, streaming straight into the
    /// buffer: no intermediate `String` allocation.
    pub fn debug_field(&mut self, field_id: u32, value: &dyn std::fmt::Debug) {
        self.fmt_field(field_id, format_args!("{value:?}"));
    }

    /// Writes a `Display`-formatted string field, streaming straight into
    /// the buffer: no intermediate `String` allocation.
    pub fn display_field(&mut self, field_id: u32, value: &dyn std::fmt::Display) {
        self.fmt_field(field_id, format_args!("{value}"));
    }

    fn fmt_field(&mut self, field_id: u32, args: std::fmt::Arguments<'_>) {
        struct Sink<'a>(&'a mut Vec<u8>);
        impl std::fmt::Write for Sink<'_> {
            fn write_str(&mut self, s: &str) -> std::fmt::Result {
                self.0.extend_from_slice(s.as_bytes());
                Ok(())
            }
        }
        let message = self.message(field_id);
        let _ = std::fmt::Write::write_fmt(&mut Sink(&mut message.buffer.buf), args);
    }

    /// Begins a nested message whose length is backfilled when the returned
    /// handle is dropped.
    #[inline(always)]
    pub(crate) fn message(&mut self, field_id: u32) -> Message<'_> {
        let token = self.begin_message(field_id);
        Message {
            buffer: self,
            token,
        }
    }

    #[inline(always)]
    pub(crate) fn begin_message(&mut self, field_id: u32) -> MessageToken {
        self.tag(field_id, WireType::Delimited);
        let len_prefix = self.buf.len();
        self.buf.extend_from_slice(&[0; MESSAGE_LEN_PREFIX_SIZE]);
        MessageToken {
            len_prefix,
            payload_start: self.buf.len(),
        }
    }

    #[inline(always)]
    pub(crate) fn finish_message(&mut self, token: MessageToken) {
        let payload_len = self.buf.len() - token.payload_start;
        write_redundant_varint(
            payload_len,
            &mut self.buf[token.len_prefix..token.payload_start],
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn buffer() -> ProtoBuffer {
        ProtoBuffer::new()
    }

    fn varint(v: u64) -> Vec<u8> {
        let mut out = Vec::new();
        write_varint(v, &mut out);
        out
    }

    fn decode_varint(bytes: &[u8]) -> (u64, usize) {
        let mut value = 0;
        for (i, byte) in bytes.iter().copied().enumerate() {
            value |= u64::from(byte & 0x7f) << (i * 7);
            if byte & 0x80 == 0 {
                return (value, i + 1);
            }
        }
        panic!("unterminated varint");
    }

    #[test]
    fn varint_encoding() {
        assert_eq!(varint(0), [0x00]);
        assert_eq!(varint(1), [0x01]);
        assert_eq!(varint(127), [0x7f]);
        assert_eq!(varint(128), [0x80, 0x01]);
        assert_eq!(varint(1234), [0xd2, 0x09]);
        assert_eq!(
            varint(u64::MAX),
            [0xff; 9].iter().copied().chain([0x01]).collect::<Vec<_>>()
        );
    }

    #[test]
    fn negative_int64_is_ten_bytes() {
        assert_eq!(varint(-1i64 as u64).len(), 10);
    }

    #[test]
    fn redundant_uint64_field_is_ten_bytes() {
        for value in [0, 1, 127, 128, u64::MAX] {
            let mut b = buffer();
            b.redundant_varint_field(1, value);
            assert_eq!(b.buf.len(), 11);
            assert_eq!(b.buf[0], 0x08);
            assert_eq!(decode_varint(&b.buf[1..]), (value, 10));
        }
    }

    #[test]
    fn redundant_varint_encoding() {
        let mut out = [0; MESSAGE_LEN_PREFIX_SIZE];
        write_redundant_varint(1, &mut out);
        assert_eq!(out, [0x81, 0x80, 0x80, 0x00]);

        write_redundant_varint(MAX_MESSAGE_LEN, &mut out);
        assert_eq!(out, [0xff, 0xff, 0xff, 0x7f]);
    }

    #[test]
    fn simple_fields() {
        let mut b = buffer();
        // TracePacket { timestamp(8): 1000, trusted_packet_sequence_id(10): 1,
        //               sequence_flags(13): 3 }
        b.varint_field(8, 1000);
        b.varint_field(10, 1);
        b.varint_field(13, 3);
        assert_eq!(b.buf.as_slice(), [0x40, 0xe8, 0x07, 0x50, 0x01, 0x68, 0x03]);
    }

    #[test]
    fn string_and_double_fields() {
        let mut b = buffer();
        b.string_field(2, "hi");
        assert_eq!(b.buf.as_slice(), [0x12, 0x02, b'h', b'i']);
        b.buf.clear();
        b.display_field(2, &"hi");
        assert_eq!(b.buf.as_slice(), [0x12, 0x82, 0x80, 0x80, 0x00, b'h', b'i']);
        b.buf.clear();
        b.double_field(5, 1.0);
        assert_eq!(b.buf.as_slice(), [0x29, 0, 0, 0, 0, 0, 0, 0xf0, 0x3f]);
        b.buf.clear();
        b.fixed64_field(47, 0x0102030405060708);
        assert_eq!(b.buf.as_slice(), [0xf9, 0x02, 8, 7, 6, 5, 4, 3, 2, 1]);
    }

    #[test]
    fn nested_small() {
        let mut b = buffer();
        {
            let mut message = b.message(11);
            message.varint_field(9, 1);
        }
        // tag(11, delimited) = 0x5a, redundant len = 2, then type(9)=1.
        assert_eq!(b.buf.as_slice(), [0x5a, 0x82, 0x80, 0x80, 0x00, 0x48, 0x01]);
    }

    #[test]
    fn nested_length_boundaries() {
        for payload_len in [127usize, 128, 16383, 16384] {
            let mut b = buffer();
            let s = "x".repeat(payload_len);
            {
                let mut message = b.message(1);
                message.string_field(2, &s);
            }
            let bytes = b.buf.as_slice();
            assert_eq!(bytes[0], 0x0a);
            // Decode the redundant length prefix and check it matches.
            let (len, prefix_len) = decode_varint(&bytes[1..]);
            assert_eq!(prefix_len, MESSAGE_LEN_PREFIX_SIZE);
            assert_eq!(len as usize, bytes.len() - 1 - prefix_len);
        }
    }

    #[test]
    fn nested_deeply() {
        let mut b = buffer();
        {
            let mut outer = b.message(1);
            outer.varint_field(8, 42);
            let mut inner = outer.message(11);
            inner.varint_field(9, 1);
            let mut leaf = inner.message(4);
            leaf.string_field(10, "name");
        }
        // Outer: tag 0x0a. Inner slice: 4.10 string "name".
        let bytes = b.buf.as_slice();
        assert_eq!(bytes[0], 0x0a);
        let (len, prefix_len) = decode_varint(&bytes[1..]);
        assert_eq!(prefix_len, MESSAGE_LEN_PREFIX_SIZE);
        assert_eq!(len as usize, bytes.len() - 1 - prefix_len);
    }
}
