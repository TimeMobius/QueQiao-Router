use super::super::params::HOUR_MS;
use super::*;

#[test]
fn range_slices_cover_endpoints_contiguously_without_overlap() {
    let from = 1_000_000_i64;
    let to = from + 30 * DAY_MS;
    let slices = range_slices(from, to);
    assert_eq!(slices.len(), 4);
    assert_eq!(slices.first().unwrap().0, from);
    assert_eq!(slices.last().unwrap().1, to);
    for pair in slices.windows(2) {
        assert_eq!(pair[1].0, pair[0].1 + 1, "slices must be gap-free");
    }
    for &(start, end) in &slices {
        assert!(start <= end, "slice must be non-empty");
    }
    let covered: i64 = slices.iter().map(|(s, e)| e - s + 1).sum();
    assert_eq!(covered, to - from + 1);
}

#[test]
fn range_slices_collapse_short_and_reversed_ranges() {
    assert_eq!(range_slices(0, HOUR_MS), vec![(0, HOUR_MS)]);
    assert_eq!(range_slices(0, DAY_MS - 1), vec![(0, DAY_MS - 1)]);
    assert_eq!(range_slices(5, 5), vec![(5, 5)]);
    assert_eq!(range_slices(10, 5), vec![(10, 5)]);
    assert_eq!(range_slices(0, 2 * DAY_MS).len(), 2);
}
