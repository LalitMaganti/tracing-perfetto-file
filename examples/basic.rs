// Copyright 2026 Lalit Maganti
// SPDX-License-Identifier: Apache-2.0

//! Writes a small synchronous trace. Usage: `basic [output.pftrace]`.

use tracing_perfetto_file::PerfettoLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

fn main() -> std::io::Result<()> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/tracing-perfetto-file-basic.pftrace".into());
    let file = std::fs::File::create(&path)?;
    let (layer, guard) = PerfettoLayer::builder(file)
        .with_debug_annotations()
        .with_source_locations()
        .with_counters()
        .build();
    tracing_subscriber::registry().with(layer).init();

    let span = tracing::info_span!("load", items = 100u64, source = "disk");
    span.in_scope(|| {
        for i in 0..3u64 {
            tracing::info_span!("parse", index = i).in_scope(|| {
                std::thread::sleep(std::time::Duration::from_millis(2));
                tracing::info!(index = i, counter.parsed_items = i + 1, "parsed item");
            });
        }
    });

    let workers: Vec<_> = (0..2)
        .map(|w| {
            std::thread::Builder::new()
                .name(format!("worker-{w}"))
                .spawn(move || {
                    tracing::info_span!("work", worker = w).in_scope(|| {
                        std::thread::sleep(std::time::Duration::from_millis(3));
                    });
                })
                .unwrap()
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }

    guard.flush()?;
    println!("wrote {path}");
    Ok(())
}
