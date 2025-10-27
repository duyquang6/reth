#![allow(missing_docs)]

use alloy_primitives::{keccak256, TxHash, TxNumber};
use reth_db::{test_utils::create_test_rw_db_with_path, DatabaseEnv, TransactionHashNumbers};
use reth_db_api::{
    cursor::DbCursorRW,
    database::Database,
    table::Table,
    transaction::{DbTx, DbTxMut},
};
use reth_fs_util as fs;
use std::sync::Arc;
use std::time::Instant;

const TEST_DB_PATH: &str = "/root/quang/db";

/// Generate a batch of transaction hashes for a specific batch number.
/// Each batch gets different hashes (using batch_num as seed) but same size.
/// Uses keccak256 to generate realistic transaction hashes.
fn generate_batch(
    batch_num: usize,
    start_tx_num: TxNumber,
    batch_size: usize,
) -> Vec<(TxHash, TxNumber)> {
    let mut data = Vec::with_capacity(batch_size);

    for i in 0..batch_size {
        // Create input data for keccak256 hash
        // Include batch_num, index, and tx_num to ensure uniqueness across batches
        let mut input = Vec::with_capacity(24); // 8 + 8 + 8 bytes
        input.extend_from_slice(&batch_num.to_be_bytes());
        input.extend_from_slice(&i.to_be_bytes());
        input.extend_from_slice(&(start_tx_num + i as TxNumber).to_be_bytes());

        // Generate keccak256 hash (same as real Ethereum transaction hashes)
        let hash = keccak256(input);
        let tx_num = start_tx_num + i as TxNumber;
        data.push((hash, tx_num));
    }

    // Sort by hash for better cache locality (as done in real code)
    data.sort_by_key(|(hash, _)| *hash);
    data
}

fn show_table_stats(db: &DatabaseEnv) {
    db.view(|tx| {
        let table_db = tx
            .inner
            .open_db(Some(TransactionHashNumbers::NAME))
            .map_err(|_| "Could not open db.")
            .unwrap();

        let stats = tx
            .inner
            .db_stat(&table_db)
            .map_err(|_| format!("Could not find table: {}", TransactionHashNumbers::NAME))
            .unwrap();

        // Count entries in the table
        let record_count = tx.entries::<TransactionHashNumbers>().unwrap();

        let num_pages = stats.leaf_pages() + stats.branch_pages() + stats.overflow_pages();
        let size = num_pages * stats.page_size() as usize;

        eprintln!("\n  Table Statistics:");
        eprintln!("    Records: {} ({:.2}M)", record_count, record_count as f64 / 1_000_000.0);
        eprintln!("    Page size: {} bytes", stats.page_size());
        eprintln!("    Leaf pages: {}", stats.leaf_pages());
        eprintln!("    Branch pages: {}", stats.branch_pages());
        eprintln!("    Overflow pages: {}", stats.overflow_pages());
        eprintln!("    Total pages: {}", num_pages);
        eprintln!("    Total size: {:.2} MB\n", size as f64 / 1_048_576.0);
        eprintln!("   all stats: {:?}", stats);
    })
    .unwrap();
}

#[test]
fn test_tx_hash_numbers_throughput() {
    // Configuration
    const PRE_INSERT_SIZE: usize = 0; // 15M records to pre-insert
    const BATCH_SIZE: usize = 70_000;
    const NUM_BATCHES: usize = 60;
    const TOTAL_INSERTS: usize = BATCH_SIZE * NUM_BATCHES;

    // Setup: Reset DB
    // let _ = fs::remove_dir_all(TEST_DB_PATH);
    let db = Arc::try_unwrap(create_test_rw_db_with_path(TEST_DB_PATH)).unwrap().into_inner_db();

    let overall_start = Instant::now();

    // Phase 1: Pre-insert 15M records
    eprintln!("Phase 1: Pre-inserting {}M records...", PRE_INSERT_SIZE / 1_000_000);
    let pre_insert_start = Instant::now();
    let mut next_tx_num = 0u64;

    // Calculate how many batches we need for 15M records
    let pre_insert_batches = (PRE_INSERT_SIZE + BATCH_SIZE - 1) / BATCH_SIZE; // Ceiling division
    let mut pre_inserted = 0;

    for batch_num in 0..pre_insert_batches {
        let batch_start = Instant::now();
        let remaining = PRE_INSERT_SIZE - pre_inserted;
        let current_batch_size = remaining.min(BATCH_SIZE);

        // Generate a new batch for this batch number (different hashes)
        let batch = generate_batch(batch_num, next_tx_num, current_batch_size);
        next_tx_num += current_batch_size as TxNumber;
        pre_inserted += current_batch_size;

        // Insert the batch
        {
            let tx = db.tx_mut().expect("tx");
            let mut cursor = tx.cursor_write::<TransactionHashNumbers>().expect("cursor");

            for (hash, tx_num) in batch {
                cursor.insert(hash, &tx_num).expect("insert");
            }

            tx.inner.commit().unwrap();
        }

        let batch_duration = batch_start.elapsed();
        let batch_rate = (current_batch_size as f64 / batch_duration.as_secs_f64()) as u64;

        eprintln!(
            "  Pre-insert batch {}: Inserted {}k in {:?} ({:.2}k inserts/sec)",
            batch_num + 1,
            current_batch_size / 1000,
            batch_duration,
            batch_rate / 1000
        );
    }

    let pre_insert_duration = pre_insert_start.elapsed();
    let pre_insert_rate = (PRE_INSERT_SIZE as f64 / pre_insert_duration.as_secs_f64()) as u64;
    eprintln!(
        "\n  Pre-insert complete: Inserted {}M in {:?} (avg {:.2}k inserts/sec)",
        PRE_INSERT_SIZE / 1_000_000,
        pre_insert_duration,
        pre_insert_rate / 1000
    );

    // Phase 2: Insert test batches
    eprintln!("\nPhase 2: Inserting {} batches of {}k...", NUM_BATCHES, BATCH_SIZE / 1000);
    let test_start = Instant::now();

    let mut next_tx_num = 63_000_000u64;
    for batch_num in 0..NUM_BATCHES {
        let batch_start = Instant::now();

        // Generate a new batch for this batch number (different hashes)
        // Use a different seed range to avoid collisions with pre-inserted data
        let batch = generate_batch(pre_insert_batches + batch_num, next_tx_num, BATCH_SIZE);
        next_tx_num += BATCH_SIZE as TxNumber;

        // Insert the batch
        {
            let tx = db.tx_mut().expect("tx");
            let mut cursor = tx.cursor_write::<TransactionHashNumbers>().expect("cursor");

            for (hash, tx_num) in batch {
                cursor.insert(hash, &tx_num).expect("insert");
            }

            tx.inner.commit().unwrap();
        }

        let batch_duration = batch_start.elapsed();
        let batch_rate = (BATCH_SIZE as f64 / batch_duration.as_secs_f64()) as u64;

        eprintln!(
            "  Test batch {}: Inserted {}k in {:?} ({:.2}k inserts/sec)",
            batch_num + 1,
            BATCH_SIZE / 1000,
            batch_duration,
            batch_rate / 1000
        );
    }

    let test_duration = test_start.elapsed();
    let test_rate = (TOTAL_INSERTS as f64 / test_duration.as_secs_f64()) as u64;

    let total_duration = overall_start.elapsed();
    let total_records = PRE_INSERT_SIZE + TOTAL_INSERTS;
    let total_rate = (total_records as f64 / total_duration.as_secs_f64()) as u64;

    // Count records in the table
    let record_count = db
        .view(|tx| {
            tx.entries::<TransactionHashNumbers>()
                .map_err(|e| format!("Failed to count entries: {:?}", e))
        })
        .unwrap()
        .unwrap();

    eprintln!(
        "\n  Test batches complete: Inserted {}M in {:?} (avg {:.2}k inserts/sec)",
        TOTAL_INSERTS / 1_000_000,
        test_duration,
        test_rate / 1000
    );
    eprintln!(
        "\n  Total: Inserted {}M in {:?} (avg {:.2}k inserts/sec)",
        total_records / 1_000_000,
        total_duration,
        total_rate / 1000
    );
    eprintln!(
        "  Records in table: {} ({:.2}M)",
        record_count,
        record_count as f64 / 1_000_000.0
    );

    // Show database statistics
    db.view(|tx| {
        let table_db = tx
            .inner
            .open_db(Some(TransactionHashNumbers::NAME))
            .map_err(|_| "Could not open db.")
            .unwrap();

        let stats = tx
            .inner
            .db_stat(&table_db)
            .map_err(|_| {
                format!("Could not find table: {}", TransactionHashNumbers::NAME)
            })
            .unwrap();

        let num_pages =
            stats.leaf_pages() + stats.branch_pages() + stats.overflow_pages();
        let size = num_pages * stats.page_size() as usize;

        show_table_stats(&db);

        eprintln!("\n  Database Statistics:");
        eprintln!("    Page size: {} bytes", stats.page_size());
        eprintln!("    Leaf pages: {}", stats.leaf_pages());
        eprintln!("    Branch pages: {}", stats.branch_pages());
        eprintln!("    Overflow pages: {}", stats.overflow_pages());
        eprintln!("    Total pages: {}", num_pages);
        eprintln!("    Total size: {:.2} MB", size as f64 / 1_048_576.0);
        eprintln!("   all stats: {:?}", stats);
    })
    .unwrap();

    // Verify we have the expected number of records
    assert_eq!(
        record_count,
        total_records,
        "Expected {} records, but found {}",
        total_records,
        record_count
    );
}

