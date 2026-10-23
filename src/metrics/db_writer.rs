//! Prometheus gauges for the audit write queue.

use once_cell::sync::Lazy;
use prometheus::{register_int_gauge, IntGauge};

use crate::db::writer::WriterSnapshot;

static QUEUE_DEPTH: Lazy<IntGauge> =
    Lazy::new(|| register_int_gauge!("db_write_queue_depth", "Pending audit write items").unwrap());
static QUEUE_CAPACITY: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!("db_write_queue_capacity", "Audit write queue capacity").unwrap()
});
static ENQUEUED: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!("db_write_enqueued", "Audit items enqueued since start").unwrap()
});
static ROWS: Lazy<IntGauge> =
    Lazy::new(|| register_int_gauge!("db_write_rows", "Audit rows committed since start").unwrap());
static BATCHES: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!("db_write_batches", "Audit batches committed since start").unwrap()
});
static DROPPED: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!("db_write_dropped", "Audit items dropped since start").unwrap()
});
static FAILED_BATCHES: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "db_write_failed_batches",
        "Audit batch failures since start"
    )
    .unwrap()
});
static LAST_BATCH_MS: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!("db_write_last_batch_ms", "Last audit batch duration").unwrap()
});
static MAX_BATCH_MS: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!("db_write_max_batch_ms", "Maximum audit batch duration").unwrap()
});

fn set(gauge: &IntGauge, value: u64) {
    gauge.set(i64::try_from(value).unwrap_or(i64::MAX));
}

pub fn update(snapshot: &WriterSnapshot) {
    set(
        &QUEUE_DEPTH,
        u64::try_from(snapshot.queue_depth).unwrap_or(u64::MAX),
    );
    set(
        &QUEUE_CAPACITY,
        u64::try_from(snapshot.queue_capacity).unwrap_or(u64::MAX),
    );
    set(&ENQUEUED, snapshot.enqueued);
    set(&ROWS, snapshot.flushed_rows);
    set(&BATCHES, snapshot.batches);
    set(&DROPPED, snapshot.dropped);
    set(&FAILED_BATCHES, snapshot.failed_batches);
    set(&LAST_BATCH_MS, snapshot.last_batch_ms);
    set(&MAX_BATCH_MS, snapshot.max_batch_ms);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_publishes_snapshot() {
        let snapshot = WriterSnapshot {
            enqueued: 5,
            flushed_rows: 4,
            batches: 2,
            dropped: 1,
            failed_batches: 0,
            last_batch_ms: 7,
            max_batch_ms: 9,
            queue_depth: 3,
            queue_capacity: 10,
        };
        update(&snapshot);
        assert_eq!(QUEUE_DEPTH.get(), 3);
        assert_eq!(QUEUE_CAPACITY.get(), 10);
        assert_eq!(ENQUEUED.get(), 5);
        assert_eq!(ROWS.get(), 4);
        assert_eq!(BATCHES.get(), 2);
        assert_eq!(DROPPED.get(), 1);
        assert_eq!(FAILED_BATCHES.get(), 0);
        assert_eq!(LAST_BATCH_MS.get(), 7);
        assert_eq!(MAX_BATCH_MS.get(), 9);
    }
}
