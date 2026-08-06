// Copyright 2026 Lalit Maganti
// SPDX-License-Identifier: Apache-2.0

//! Small, deeply nested workload used for the README screenshot.
//! Usage: `readme_demo [output.pftrace]`.

use std::time::Duration;

use tracing_perfetto_file::PerfettoLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

fn pause(milliseconds: u64) {
    std::thread::sleep(Duration::from_millis(milliseconds));
}

fn verify_signature() {
    tracing::info_span!("verify_signature", algorithm = "Ed25519").in_scope(|| pause(35));
}

fn decode_claims() {
    tracing::info_span!("decode_claims", claims = 7).in_scope(|| {
        pause(15);
        verify_signature();
        pause(10);
    });
}

fn validate_token() {
    tracing::info_span!("validate_token", token_bytes = 384).in_scope(|| {
        pause(15);
        decode_claims();
        pause(10);
    });
}

fn authenticate() {
    tracing::info_span!("authenticate", method = "bearer").in_scope(|| {
        pause(10);
        validate_token();
        pause(10);
    });
}

fn decode_rows() {
    tracing::info_span!("decode_rows", rows = 42).in_scope(|| pause(40));
}

fn query_database() {
    tracing::info_span!("query_database", table = "widgets").in_scope(|| {
        pause(25);
        decode_rows();
        tracing::info!(counter.rows_processed = 42_u64, "rows decoded");
        pause(20);
    });
}

fn load_widgets() {
    tracing::info_span!("load_widgets", widget_count = 6).in_scope(|| {
        pause(15);
        query_database();
        pause(15);
    });
}

fn compress_body() {
    tracing::info_span!("compress_body", encoding = "gzip").in_scope(|| pause(30));
}

fn serialize_response() {
    tracing::info_span!("serialize_response", format = "json").in_scope(|| {
        pause(20);
        compress_body();
        pause(15);
    });
}

fn render_template() {
    tracing::info_span!("render_template", template = "dashboard").in_scope(|| {
        pause(15);
        serialize_response();
        pause(15);
    });
}

fn render_dashboard() {
    tracing::info_span!("render_dashboard", user_id = 17).in_scope(|| {
        load_widgets();
        pause(15);
        render_template();
    });
}

fn await_acknowledgement() {
    tracing::info_span!("await_acknowledgement", peer = "127.0.0.1").in_scope(|| pause(30));
}

fn flush_socket() {
    tracing::info_span!("flush_socket", bytes = 12_480).in_scope(|| {
        pause(20);
        await_acknowledgement();
        pause(10);
    });
}

fn write_response() {
    tracing::info_span!("write_response", status = 200).in_scope(|| {
        pause(15);
        flush_socket();
        pause(10);
    });
}

fn main() -> std::io::Result<()> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/tracing-perfetto-file-readme.pftrace".into());
    let file = std::fs::File::create(&path)?;
    let (layer, guard) = PerfettoLayer::builder(file)
        .with_debug_annotations()
        .with_source_locations()
        .with_counters()
        .build();
    tracing_subscriber::registry().with(layer).init();

    tracing::info_span!(
        "http_request",
        method = "GET",
        path = "/dashboard",
        request_id = 42
    )
    .in_scope(|| {
        authenticate();
        pause(20);
        render_dashboard();
        pause(20);
        write_response();
    });

    guard.flush()?;
    println!("wrote {path}");
    Ok(())
}
