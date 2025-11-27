#![allow(missing_docs, unreachable_pub)]

use alloy_primitives::{map::B256Map, B256, U256};
use bincode::deserialize;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use reth_trie_common::{HashedPostStateSorted, HashedStorageSorted};
use std::{fs, hint::black_box};

/// Deserializes HashedPostStateSorted from binary format.
fn deserialize_hashed_post_state_sorted(data: &[u8]) -> Result<HashedPostStateSorted, String> {
    if data.len() < 16 {
        return Err("Invalid data: too short".to_string());
    }

    // Read accounts_len
    let accounts_len = u64::from_le_bytes([
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ]) as usize;

    // Read accounts_data
    if data.len() < 8 + accounts_len {
        return Err("Invalid data: accounts data too short".to_string());
    }
    let accounts_data = &data[8..8 + accounts_len];
    let accounts: Vec<(B256, Option<reth_primitives_traits::Account>)> =
        deserialize(accounts_data)
            .map_err(|e| format!("Failed to deserialize accounts: {}", e))?;

    // Read storages_len
    let storages_start = 8 + accounts_len;
    if data.len() < storages_start + 8 {
        return Err("Invalid data: storages length missing".to_string());
    }
    let storages_len = u64::from_le_bytes([
        data[storages_start],
        data[storages_start + 1],
        data[storages_start + 2],
        data[storages_start + 3],
        data[storages_start + 4],
        data[storages_start + 5],
        data[storages_start + 6],
        data[storages_start + 7],
    ]) as usize;

    // Read storages_data
    let storages_data_start = storages_start + 8;
    if data.len() < storages_data_start + storages_len {
        return Err("Invalid data: storages data too short".to_string());
    }
    let storages_data = &data[storages_data_start..storages_data_start + storages_len];
    let storages_vec: Vec<(B256, (Vec<(B256, U256)>, bool))> = deserialize(storages_data)
        .map_err(|e| format!("Failed to deserialize storages: {}", e))?;

    // Convert back to B256Map<HashedStorageSorted>
    let mut storages = B256Map::default();
    for (addr, (storage_slots, wiped)) in storages_vec {
        let storage = HashedStorageSorted {
            storage_slots,
            wiped,
        };
        storages.insert(addr, storage);
    }

    Ok(HashedPostStateSorted { accounts, storages })
}

/// Loads the test data from binary files.
/// Returns (aggregated_state, hashed_state) or panics on error.
fn load_test_data(
    aggregated_path: &str,
    hashed_path: &str,
) -> (HashedPostStateSorted, HashedPostStateSorted) {
    let aggregated_data = fs::read(aggregated_path)
        .unwrap_or_else(|e| panic!("Failed to read {}: {}", aggregated_path, e));
    let aggregated = deserialize_hashed_post_state_sorted(&aggregated_data)
        .unwrap_or_else(|e| panic!("Failed to deserialize aggregated state: {}", e));

    let hashed_data = fs::read(hashed_path)
        .unwrap_or_else(|e| panic!("Failed to read {}: {}", hashed_path, e));
    let hashed = deserialize_hashed_post_state_sorted(&hashed_data)
        .unwrap_or_else(|e| panic!("Failed to deserialize hashed state: {}", e));

    (aggregated, hashed)
}

/// Verifies that the HashedPostStateSorted data is properly sorted.
/// This is critical because extend_sorted_vec functions assume sorted input.
fn verify_sorted(state: &HashedPostStateSorted, name: &str) -> Result<(), String> {
    // Verify accounts are sorted by B256 key
    let accounts = state.accounts();
    for i in 1..accounts.len() {
        if accounts[i - 1].0 > accounts[i].0 {
            return Err(format!(
                "{}: accounts not sorted at index {}: {:?} > {:?}",
                name, i, accounts[i - 1].0, accounts[i].0
            ));
        }
    }

    // Verify storage slots are sorted for each address
    for (addr, storage) in state.account_storages() {
        let slots = storage.storage_slots_ref();
        for i in 1..slots.len() {
            if slots[i - 1].0 > slots[i].0 {
                return Err(format!(
                    "{}: storage slots not sorted for address {:?} at index {}: {:?} > {:?}",
                    name, addr, i, slots[i - 1].0, slots[i].0
                ));
            }
        }
    }

    Ok(())
}

/// Reduces overlap between two states by changing overlapping keys to random values.
/// This preserves the data size while achieving the target overlap ratio.
fn reduce_overlap(
    aggregated: &HashedPostStateSorted,
    hashed: &HashedPostStateSorted,
    target_overlap_ratio: f64,
) -> HashedPostStateSorted {
    // Calculate current overlap (using union-based ratio to match statistics)
    let aggregated_accounts: std::collections::HashSet<B256> =
        aggregated.accounts().iter().map(|(addr, _)| *addr).collect();
    let hashed_accounts: std::collections::HashSet<B256> =
        hashed.accounts().iter().map(|(addr, _)| *addr).collect();
    
    let current_overlap = aggregated_accounts.intersection(&hashed_accounts).count();
    let account_union = aggregated_accounts.union(&hashed_accounts).count();
    let current_overlap_ratio = if account_union > 0 {
        current_overlap as f64 / account_union as f64
    } else {
        0.0
    };

    // If current overlap is already less than target, return original
    if current_overlap_ratio <= target_overlap_ratio {
        return hashed.clone();
    }

    // Calculate target overlap count based on union (to match statistics calculation)
    // When we change X overlapping accounts to random values:
    //   new_overlap = current_overlap - X
    //   new_union = current_union + X (because we're adding X new unique addresses)
    //   We want: new_overlap / new_union = target_overlap_ratio
    //   So: (current_overlap - X) / (current_union + X) = target_overlap_ratio
    //   Solving for X:
    //     current_overlap - X = target_overlap_ratio * (current_union + X)
    //     current_overlap - X = target_overlap_ratio * current_union + target_overlap_ratio * X
    //     current_overlap - target_overlap_ratio * current_union = X + target_overlap_ratio * X
    //     current_overlap - target_overlap_ratio * current_union = X * (1 + target_overlap_ratio)
    //     X = (current_overlap - target_overlap_ratio * current_union) / (1 + target_overlap_ratio)
    let accounts_to_change = if current_overlap_ratio > target_overlap_ratio {
        let numerator = current_overlap as f64 - (target_overlap_ratio * account_union as f64);
        let denominator = 1.0 + target_overlap_ratio;
        (numerator / denominator).ceil() as usize
    } else {
        0
    };

    // Collect all existing addresses (aggregated + hashed) to avoid collisions
    let mut existing_addresses: std::collections::HashSet<B256> = aggregated_accounts
        .union(&hashed_accounts)
        .copied()
        .collect();

    // Collect overlapping accounts and change some to random values
    let mut overlapping_accounts: Vec<B256> = aggregated_accounts
        .intersection(&hashed_accounts)
        .copied()
        .collect();
    overlapping_accounts.sort(); // Sort for deterministic selection

    // Create mapping: old_address -> new_random_address for accounts to change
    let mut address_mapping: std::collections::HashMap<B256, B256> = std::collections::HashMap::new();
    
    for &old_addr in overlapping_accounts.iter().take(accounts_to_change) {
        // Generate a new random address that doesn't collide with existing ones
        loop {
            let new_addr = B256::random();
            
            if !existing_addresses.contains(&new_addr) {
                address_mapping.insert(old_addr, new_addr);
                existing_addresses.insert(new_addr);
                break;
            }
        }
    }

    // Build new accounts list (change overlapping keys to random values)
    let mut new_accounts: Vec<(B256, Option<reth_primitives_traits::Account>)> = hashed
        .accounts()
        .iter()
        .map(|(addr, account)| {
            if let Some(&new_addr) = address_mapping.get(addr) {
                (new_addr, *account)
            } else {
                (*addr, *account)
            }
        })
        .collect();

    // Build new storages (remap addresses for changed accounts)
    let mut new_storages = B256Map::default();
    for (addr, storage) in hashed.account_storages() {
        let new_addr = address_mapping.get(addr).copied().unwrap_or(*addr);
        new_storages.insert(new_addr, storage.clone());
    }

    // Ensure accounts are still sorted
    new_accounts.sort_by_key(|(addr, _)| *addr);

    HashedPostStateSorted {
        accounts: new_accounts,
        storages: new_storages,
    }
}

/// Calculates overlap statistics between two HashedPostStateSorted states.
fn calculate_overlap(
    aggregated: &HashedPostStateSorted,
    hashed: &HashedPostStateSorted,
) -> (f64, f64, usize, usize, usize, usize) {
    // Calculate account overlap
    let aggregated_accounts: std::collections::HashSet<B256> =
        aggregated.accounts().iter().map(|(addr, _)| *addr).collect();
    let hashed_accounts: std::collections::HashSet<B256> =
        hashed.accounts().iter().map(|(addr, _)| *addr).collect();

    let account_overlap = aggregated_accounts.intersection(&hashed_accounts).count();
    let account_union = aggregated_accounts.union(&hashed_accounts).count();
    let account_overlap_ratio = if account_union > 0 {
        account_overlap as f64 / account_union as f64
    } else {
        0.0
    };

    // Calculate storage overlap
    let mut storage_overlap_count = 0;
    let mut storage_total_count = 0;
    let mut storage_hashed_total = 0;

    // Count storage slots in aggregated
    for (_addr, storage) in aggregated.account_storages() {
        storage_total_count += storage.storage_slots_ref().len();
    }

    // Count storage slots in hashed and find overlaps
    for (addr, hashed_storage) in hashed.account_storages() {
        storage_hashed_total += hashed_storage.storage_slots_ref().len();

        if let Some(agg_storage) = aggregated.account_storages().get(addr) {
            let agg_slots: std::collections::HashSet<B256> = agg_storage
                .storage_slots_ref()
                .iter()
                .map(|(slot, _)| *slot)
                .collect();
            let hashed_slots: std::collections::HashSet<B256> = hashed_storage
                .storage_slots_ref()
                .iter()
                .map(|(slot, _)| *slot)
                .collect();

            storage_overlap_count += agg_slots.intersection(&hashed_slots).count();
        }
    }

    let storage_union_count = storage_total_count + storage_hashed_total - storage_overlap_count;
    let storage_overlap_ratio = if storage_union_count > 0 {
        storage_overlap_count as f64 / storage_union_count as f64
    } else {
        0.0
    };

    (
        account_overlap_ratio,
        storage_overlap_ratio,
        account_overlap,
        account_union,
        storage_overlap_count,
        storage_union_count,
    )
}

pub fn bench_extend_ref(c: &mut Criterion) {
    // Get file paths from environment or use defaults
    // Default paths are relative to the crate root: crates/trie/common/testdata/
    let default_aggregated = concat!(env!("CARGO_MANIFEST_DIR"), "/testdata/test-hashed-state-data/aggregated_hashed_post_state_sorted.bin");
    let default_hashed = concat!(env!("CARGO_MANIFEST_DIR"), "/testdata/test-hashed-state-data/hashed_state.bin");
    
    let aggregated_path = std::env::var("AGGREGATED_STATE_PATH")
        .unwrap_or_else(|_| default_aggregated.to_string());
    let hashed_path = std::env::var("HASHED_STATE_PATH")
        .unwrap_or_else(|_| default_hashed.to_string());

    // Load test data once
    let (aggregated, hashed_original) = load_test_data(&aggregated_path, &hashed_path);

    // Verify that input data is sorted (required for extend_sorted_vec functions)
    verify_sorted(&aggregated, "aggregated")
        .unwrap_or_else(|e| panic!("Input validation failed: {}", e));
    verify_sorted(&hashed_original, "hashed")
        .unwrap_or_else(|e| panic!("Input validation failed: {}", e));

    // Optionally reduce overlap for testing different scenarios
    // Get target overlap from environment variable (0.0 to 1.0) or default to 0.5 (50%)
    // Set TARGET_OVERLAP_RATIO=0.3 for 30% overlap, TARGET_OVERLAP_RATIO=0.5 for 50%, etc.
    let target_overlap = std::env::var("TARGET_OVERLAP_RATIO")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|&r| r >= 0.0 && r <= 1.0)
        .unwrap_or(0.5); // Default to 50% overlap
    
    let hashed = if target_overlap < 1.0 {
        println!("Reducing overlap from original to {:.1}%...", target_overlap * 100.0);
        let reduced = reduce_overlap(&aggregated, &hashed_original, target_overlap);
        verify_sorted(&reduced, "hashed_reduced")
            .unwrap_or_else(|e| panic!("Reduced data validation failed: {}", e));
        reduced
    } else {
        hashed_original
    };

    println!(
        "Loaded test data: {} accounts, {} storages (aggregated); {} accounts, {} storages (hashed)",
        aggregated.accounts().len(),
        aggregated.account_storages().len(),
        hashed.accounts().len(),
        hashed.account_storages().len()
    );
    println!("✓ Verified: Both inputs are properly sorted");

    // Calculate and display overlap statistics
    let (account_overlap_ratio, storage_overlap_ratio, account_overlap, account_union, storage_overlap, storage_union) =
        calculate_overlap(&aggregated, &hashed);
    
    println!("\n=== Overlap Statistics ===");
    println!("Accounts:");
    println!("  Overlap: {} / {} ({:.2}%)", account_overlap, account_union, account_overlap_ratio * 100.0);
    println!("  Aggregated unique: {}", aggregated.accounts().len() - account_overlap);
    println!("  Hashed unique: {}", hashed.accounts().len() - account_overlap);
    println!("Storage:");
    println!("  Overlap: {} / {} ({:.2}%)", storage_overlap, storage_union, storage_overlap_ratio * 100.0);
    println!("  Aggregated total slots: {}", aggregated.account_storages().values().map(|s| s.storage_slots_ref().len()).sum::<usize>());
    println!("  Hashed total slots: {}", hashed.account_storages().values().map(|s| s.storage_slots_ref().len()).sum::<usize>());
    println!("  Overlap ratio: {:.2}%", storage_overlap_ratio * 100.0);
    
    let mut group = c.benchmark_group("extend_ref");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(10));

    // Benchmark extend_ref_new
    // Use iter_batched to clone outside the measured iteration
    group.bench_function(BenchmarkId::new("extend_ref", "itertool_merge"), |b| {
        b.iter_batched(
            || aggregated.clone(),
            |mut agg: HashedPostStateSorted| {
                agg.extend_ref_new(black_box(&hashed));
                black_box(agg)
            },
            criterion::BatchSize::SmallInput,
        );
    });

    // Benchmark extend_ref_custom (merge algorithm - optimized for high overlap)
    // This uses extend_sorted_vec_custom which does a single merge pass
    // Best for cases with high overlap (like 93% in this dataset)
    group.bench_function(BenchmarkId::new("extend_ref", "merge"), |b| {
        b.iter_batched(
            || aggregated.clone(),
            |mut agg| {
                agg.extend_ref_custom(black_box(&hashed));
                black_box(agg)
            },
            criterion::BatchSize::SmallInput,
        );
    });

    // Benchmark extend_ref
    // Use iter_batched to clone outside the measured iteration
    group.bench_function(BenchmarkId::new("extend_ref", "default"), |b| {
        b.iter_batched(
            || aggregated.clone(),
            |mut agg| {
                agg.extend_ref(black_box(&hashed));
                black_box(agg)
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_extend_ref);
criterion_main!(benches);

