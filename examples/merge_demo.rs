// Copyright 2026 Lalit Maganti
// SPDX-License-Identifier: Apache-2.0

//! Workload for validating merges with system/perf traces: several threads
//! alternating busy work and sleeps for a few seconds.
//! Usage: `merge_demo [output.pftrace] [seconds]`.

use tracing_perfetto_file::PerfettoLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

fn spin(ms: u64) -> u64 {
    let start = std::time::Instant::now();
    let mut acc = 0u64;
    while start.elapsed().as_millis() < u128::from(ms) {
        acc = acc.wrapping_mul(6364136223846793005).wrapping_add(1);
    }
    acc
}

fn main() -> std::io::Result<()> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/merge-demo.pftrace".into());
    let secs: u64 = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(6);
    let file = std::fs::File::create(&path)?;
    let (layer, guard) = PerfettoLayer::builder(file)
        .with_debug_annotations()
        .with_counters()
        .build();
    tracing_subscriber::registry().with(layer).init();
    println!("pid {}", std::process::id());

    let workers: Vec<_> = (0..3)
        .map(|w| {
            std::thread::Builder::new()
                .name(format!("demo-worker-{w}"))
                .spawn(move || {
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(secs);
                    let mut iteration = 0u64;
                    while std::time::Instant::now() < deadline {
                        tracing::info_span!("demo_compute", worker = w, iteration)
                            .in_scope(|| spin(5));
                        tracing::info_span!("demo_idle", worker = w).in_scope(|| {
                            std::thread::sleep(std::time::Duration::from_millis(10));
                        });
                        tracing::info!(counter.demo_iterations = iteration as i64, "demo tick");
                        iteration += 1;
                    }
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
