#![allow(missing_docs, unreachable_pub)]
use alloy_primitives::B256;
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkGroup, BenchmarkId, Criterion,
};
use reth_trie_common::utils::{extend_sorted_vec, extend_sorted_vec_custom, extend_sorted_vec_new};

/// Generate a sorted vector of (u64, u64) tuples
fn generate_sorted_vec(size: usize, start: u64) -> Vec<(u64, u64)> {
    (start..start + size as u64).map(|i| (i, i * 2)).collect()
}

/// Generate two overlapping sorted vectors with some common keys
fn generate_overlapping_vecs(
    target_size: usize,
    other_size: usize,
) -> (Vec<(u64, u64)>, Vec<(u64, u64)>) {
    // Target vec starts at 0
    let target = generate_sorted_vec(target_size, 0);

    // Other vec starts at target_size/2, creating 90% overlap
    let other = generate_sorted_vec(other_size, (target_size / 10) as u64);

    (target, other)
}

fn bench_extend_sorted_vec_old(
    group: &mut BenchmarkGroup<'_, criterion::measurement::WallTime>,
    target_size: usize,
    other_size: usize,
) {
    let id = BenchmarkId::new("extend_sorted_vec_default", format!("t{}_o{}", target_size, other_size));

    group.bench_function(id, |b| {
        b.iter_batched_ref(
            || generate_overlapping_vecs(target_size, other_size),
            |(target, other)| {
                extend_sorted_vec(black_box(target), black_box(other));
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_extend_sorted_vec_custom_merge(
    group: &mut BenchmarkGroup<'_, criterion::measurement::WallTime>,
    target_size: usize,
    other_size: usize,
) {
    let id =
        BenchmarkId::new("extend_sorted_vec_custom_merge", format!("t{}_o{}", target_size, other_size));

    group.bench_function(id, |b| {
        b.iter_batched_ref(
            || generate_overlapping_vecs(target_size, other_size),
            |(target, other)| {
                extend_sorted_vec_custom(black_box(target), black_box(other));
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_extend_sorted_vec_itertool_merge(
    group: &mut BenchmarkGroup<'_, criterion::measurement::WallTime>,
    target_size: usize,
    other_size: usize,
) {
    let id = BenchmarkId::new("extend_sorted_vec_itertool_merge", format!("t{}_o{}", target_size, other_size));

    group.bench_function(id, |b| {
        b.iter_batched_ref(
            || generate_overlapping_vecs(target_size, other_size),
            |(target, other)| {
                extend_sorted_vec_new(black_box(target), black_box(other));
            },
            BatchSize::SmallInput,
        );
    });
}

/// Format size for benchmark labels (e.g., 100000 -> "100k", 1000 -> "1k")
fn format_size(size: usize) -> String {
    if size >= 1000 {
        format!("{}k", size / 1000)
    } else {
        size.to_string()
    }
}

/// Generate two overlapping sorted vectors with configurable overlap ratio
/// Uses B256 keys similar to the extend_ref benchmark
fn generate_overlapping_vecs_b256(
    target_size: usize,
    other_size: usize,
    overlap_ratio: f64,
) -> (Vec<(B256, u64)>, Vec<(B256, u64)>) {
    let mut target: Vec<(B256, u64)> = (0..target_size)
        .map(|i| {
            let mut bytes = [0u8; 32];
            bytes[24..32].copy_from_slice(&(i as u64).to_be_bytes());
            (B256::from(bytes), i as u64)
        })
        .collect();
    target.sort_by_key(|(k, _)| *k);

    // Calculate how many overlapping keys we want
    let overlap_count = ((target_size.min(other_size) as f64) * overlap_ratio) as usize;
    let unique_count = other_size.saturating_sub(overlap_count);

    let mut other: Vec<(B256, u64)> = Vec::with_capacity(other_size);

    // Add overlapping keys (from target)
    for i in 0..overlap_count.min(target_size) {
        other.push((target[i].0, i as u64 + 1000000)); // Different value
    }

    // Add unique keys
    for i in 0..unique_count {
        let mut bytes = [0u8; 32];
        let val = (target_size + i) as u64;
        bytes[24..32].copy_from_slice(&val.to_be_bytes());
        other.push((B256::from(bytes), val));
    }

    other.sort_by_key(|(k, _)| *k);
    (target, other)
}

fn bench_extend_sorted_vec_default(
    group: &mut BenchmarkGroup<'_, criterion::measurement::WallTime>,
    target_size: usize,
    other_size: usize,
    overlap_ratio: f64,
) {
    let id = BenchmarkId::new(
        "default",
        format!("{}_{}_{:.0}%", format_size(target_size), format_size(other_size), overlap_ratio * 100.0),
    );

    group.bench_function(id, |b| {
        b.iter_batched(
            || generate_overlapping_vecs_b256(target_size, other_size, overlap_ratio),
            |(mut target, other)| {
                extend_sorted_vec(black_box(&mut target), black_box(&other));
                black_box(target)
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_extend_sorted_vec_custom_merge_large(
    group: &mut BenchmarkGroup<'_, criterion::measurement::WallTime>,
    target_size: usize,
    other_size: usize,
    overlap_ratio: f64,
) {
    let id = BenchmarkId::new(
        "merge",
        format!("{}_{}_{:.0}%", format_size(target_size), format_size(other_size), overlap_ratio * 100.0),
    );

    group.bench_function(id, |b| {
        b.iter_batched(
            || generate_overlapping_vecs_b256(target_size, other_size, overlap_ratio),
            |(mut target, other)| {
                extend_sorted_vec_custom(black_box(&mut target), black_box(&other));
                black_box(target)
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_extend_sorted_vec_itertool_merge_large(
    group: &mut BenchmarkGroup<'_, criterion::measurement::WallTime>,
    target_size: usize,
    other_size: usize,
    overlap_ratio: f64,
) {
    let id = BenchmarkId::new(
        "itertool",
        format!("{}_{}_{:.0}%", format_size(target_size), format_size(other_size), overlap_ratio * 100.0),
    );

    group.bench_function(id, |b| {
        b.iter_batched(
            || generate_overlapping_vecs_b256(target_size, other_size, overlap_ratio),
            |(mut target, other)| {
                extend_sorted_vec_new(black_box(&mut target), black_box(&other));
                black_box(target)
            },
            BatchSize::SmallInput,
        );
    });
}

fn extend_sorted_vec_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("extend_sorted_vec_comparison");

    // Test different combinations of target size and other size
    let target_sizes = vec![10, 100, 1000, 5000];
    let other_sizes = vec![10, 100, 1000, 5000];

    for &target_size in &target_sizes {
        for &other_size in &other_sizes {
            bench_extend_sorted_vec_old(&mut group, target_size, other_size);
            bench_extend_sorted_vec_custom_merge(&mut group, target_size, other_size);
            bench_extend_sorted_vec_itertool_merge(&mut group, target_size, other_size);
        }
    }

    group.finish();
}

fn extend_sorted_vec_large_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("b256");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(5));

    // Test with sizes similar to extend_ref benchmark (~100k accounts)
    let configs = vec![
        // (target_size, other_size, overlap_ratio)
        (100_000, 93_000, 0.50), // Similar to extend_ref benchmark
        (100_000, 93_000, 0.10), // Low overlap
        (100_000, 93_000, 0.90), // High overlap
        (10_000, 10_000, 0.50),  // Smaller scale
        (1_000, 1_000, 0.50),    // Even smaller
    ];

    for (target_size, other_size, overlap_ratio) in configs {
        println!(
            "\nBenchmarking: target={}, other={}, overlap={:.0}%",
            target_size,
            other_size,
            overlap_ratio * 100.0
        );

        bench_extend_sorted_vec_default(&mut group, target_size, other_size, overlap_ratio);
        bench_extend_sorted_vec_custom_merge_large(&mut group, target_size, other_size, overlap_ratio);
        bench_extend_sorted_vec_itertool_merge_large(&mut group, target_size, other_size, overlap_ratio);
    }

    group.finish();
}

criterion_group!(benches, extend_sorted_vec_benchmark, extend_sorted_vec_large_benchmark);
criterion_main!(benches);
