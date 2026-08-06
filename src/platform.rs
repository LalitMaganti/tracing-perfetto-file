// Copyright 2026 Lalit Maganti
// SPDX-License-Identifier: Apache-2.0

/// Reads an OS clock in nanoseconds through `clock_gettime`.
#[cfg(any(target_os = "linux", target_os = "android"))]
#[inline(always)]
fn os_clock_ns(clock_id: i32) -> Option<u64> {
    #[repr(C)]
    struct Timespec {
        tv_sec: i64,
        tv_nsec: i64,
    }
    unsafe extern "C" {
        fn clock_gettime(clockid: i32, tp: *mut Timespec) -> i32;
    }
    let mut timestamp = Timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `timestamp` is a valid out-pointer for the call.
    if unsafe { clock_gettime(clock_id, &mut timestamp) } != 0 {
        return None;
    }
    Some(
        (timestamp.tv_sec as u64)
            .wrapping_mul(1_000_000_000)
            .wrapping_add(timestamp.tv_nsec as u64),
    )
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[expect(
    dead_code,
    reason = "each target uses one compile-time-selected clock domain"
)]
pub(crate) enum ClockDomain {
    Builtin(u64),
    SequenceLocal,
}

/// Perfetto itself uses BOOTTIME on Linux and Android and MONOTONIC on Apple
/// and Windows.
#[cfg(any(target_os = "linux", target_os = "android"))]
const TRACE_CLOCK_DOMAIN: ClockDomain = ClockDomain::Builtin(6);
#[cfg(any(target_vendor = "apple", windows))]
const TRACE_CLOCK_DOMAIN: ClockDomain = ClockDomain::Builtin(3);
#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_vendor = "apple",
    windows
)))]
const TRACE_CLOCK_DOMAIN: ClockDomain = ClockDomain::SequenceLocal;

pub(crate) fn trace_clock_domain() -> ClockDomain {
    TRACE_CLOCK_DOMAIN
}

#[cfg(any(target_os = "linux", target_os = "android"))]
#[inline(always)]
pub(super) fn trace_clock_ns() -> Option<u64> {
    const CLOCK_BOOTTIME: i32 = 7;
    os_clock_ns(CLOCK_BOOTTIME)
}

#[cfg(target_vendor = "apple")]
#[inline(always)]
pub(super) fn trace_clock_ns() -> Option<u64> {
    unsafe extern "C" {
        fn clock_gettime_nsec_np(clock_id: u32) -> u64;
    }
    // This has the same mach-absolute-time basis Perfetto labels MONOTONIC on
    // Apple and, unlike realtime, cannot jump when the wall clock is adjusted.
    const CLOCK_UPTIME_RAW: u32 = 8;
    // SAFETY: the function has no preconditions.
    match unsafe { clock_gettime_nsec_np(CLOCK_UPTIME_RAW) } {
        0 => None,
        nanoseconds => Some(nanoseconds),
    }
}

#[cfg(windows)]
#[inline(always)]
pub(super) fn trace_clock_ns() -> Option<u64> {
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn QueryPerformanceCounter(count: *mut i64) -> i32;
        fn QueryPerformanceFrequency(frequency: *mut i64) -> i32;
    }
    let mut counter = 0;
    let mut frequency = 0;
    // SAFETY: both arguments are valid out-pointers.
    let valid = unsafe {
        QueryPerformanceFrequency(&mut frequency) != 0 && QueryPerformanceCounter(&mut counter) != 0
    };
    if !valid || frequency <= 0 || counter < 0 {
        return None;
    }
    Some((counter as u128 * 1_000_000_000 / frequency as u128) as u64)
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_vendor = "apple",
    windows
)))]
#[inline(always)]
pub(super) fn trace_clock_ns() -> Option<u64> {
    None
}

#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) fn monotonic_ns() -> Option<u64> {
    const CLOCK_MONOTONIC: i32 = 1;
    os_clock_ns(CLOCK_MONOTONIC)
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
pub(crate) fn monotonic_ns() -> Option<u64> {
    None
}

#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) fn os_tid() -> Option<u64> {
    let link = std::fs::read_link("/proc/thread-self").ok()?;
    link.file_name()?.to_str()?.parse().ok()
}

#[cfg(target_os = "macos")]
pub(crate) fn os_tid() -> Option<u64> {
    unsafe extern "C" {
        fn pthread_threadid_np(thread: usize, thread_id: *mut u64) -> i32;
    }
    let mut tid = 0;
    // SAFETY: zero selects the calling thread and `tid` is a valid out-pointer.
    if unsafe { pthread_threadid_np(0, &mut tid) } == 0 {
        Some(tid)
    } else {
        None
    }
}

#[cfg(windows)]
pub(crate) fn os_tid() -> Option<u64> {
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetCurrentThreadId() -> u32;
    }
    // SAFETY: the function has no preconditions.
    Some(u64::from(unsafe { GetCurrentThreadId() }))
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    windows
)))]
pub(crate) fn os_tid() -> Option<u64> {
    None
}

pub(crate) fn process_name() -> String {
    std::env::current_exe()
        .ok()
        .and_then(|path| {
            path.file_name()
                .map(|name| name.to_string_lossy().into_owned())
        })
        .unwrap_or_else(|| format!("process {}", std::process::id()))
}
