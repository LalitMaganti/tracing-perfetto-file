// Copyright 2026 Lalit Maganti
// SPDX-License-Identifier: Apache-2.0

//! Spans polled from multiple threads, mimicking an async runtime. The
//! default per-span lifetime track contains one nested slice per poll.
//! Usage: `cross_thread [output.pftrace]`.

use tracing_perfetto_file::PerfettoLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

fn main() -> std::io::Result<()> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/tracing-perfetto-file-cross-thread.pftrace".into());
    let file = std::fs::File::create(&path)?;
    let (layer, guard) = PerfettoLayer::builder(file)
        .with_debug_annotations()
        .with_poll_slices()
        .build();
    tracing_subscriber::registry().with(layer).init();

    let request = tracing::info_span!("request", id = 42u64);

    // Each "poll" of the request happens on a different worker thread, like
    // a future migrating across a runtime's thread pool.
    for poll in 0..3 {
        let span = request.clone();
        std::thread::Builder::new()
            .name(format!("worker-{poll}"))
            .spawn(move || {
                let _guard = span.enter();
                std::thread::sleep(std::time::Duration::from_millis(2));
                tracing::info!(poll, "polled");
            })
            .unwrap()
            .join()
            .unwrap();
    }

    // A follow-up span linked with a Perfetto flow arrow.
    let followup = tracing::info_span!("write_response");
    followup.follows_from(request.id());
    drop(request);
    followup.in_scope(|| std::thread::sleep(std::time::Duration::from_millis(1)));
    drop(followup);

    guard.flush()?;
    println!("wrote {path}");
    Ok(())
}
