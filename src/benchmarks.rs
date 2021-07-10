use dashmap::DashMap;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

#[derive(Debug, Default)]
struct BenchmarksInner {
    map: DashMap<u64, Instant>,
    auto_increaser: AtomicU64,
}

#[derive(Debug, Clone, Default)]
pub(super) struct Benchmarks {
    inner: Arc<BenchmarksInner>,
}

impl Benchmarks {
    #[inline]
    pub(super) fn new() -> Self {
        Default::default()
    }

    #[inline]
    pub(super) fn new_key(&self) -> u64 {
        let key = self.inner.auto_increaser.fetch_add(1, Ordering::SeqCst);
        self.inner.map.insert(key, Instant::now());
        key
    }

    #[inline]
    pub(super) fn done(&self, key: u64) -> Option<Duration> {
        self.inner
            .map
            .remove(&key)
            .map(|(_, started_at)| started_at.elapsed())
    }
}
