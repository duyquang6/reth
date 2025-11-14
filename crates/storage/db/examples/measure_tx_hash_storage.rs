#![allow(missing_docs)]

use alloy_primitives::TxHash;
use reth_db::{test_utils::create_test_rw_db_with_path, Database, DatabaseEnv, TransactionHashNumbers};
use reth_db_api::{cursor::DbCursorRW, transaction::{DbTx, DbTxMut}, table::Table};
use rocksdb::{BlockBasedOptions, CompactionPri, Options, WriteBatch, DB};
use std::{fs, sync::Arc};

const MDBX_BENCH_PATH: &str = "/tmp/bench_mdbx_comparison";
const ROCKSDB_BENCH_PATH: &str = "/tmp/bench_rocksdb_comparison";

/// Calculate directory size in bytes
fn get_dir_size(path: &str) -> std::io::Result<u64> {
    let mut total = 0u64;
    
    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.flatten() {
            let metadata = entry.metadata()?;
            if metadata.is_file() {
                total += metadata.len();
            } else if metadata.is_dir() {
                total += get_dir_size(&entry.path().to_string_lossy())?;
            }
        }
    }
    
    Ok(total)
}

/// Format bytes as human-readable string
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    
    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}

/// Get actual MDBX data size from table statistics (not pre-allocated file size)
fn get_mdbx_table_size(db: &DatabaseEnv, table_name: &str) -> u64 {
    db.view(|tx| {
        let table_db = tx.inner.open_db(Some(table_name)).ok()?;
        let stats = tx.inner.db_stat(&table_db).ok()?;
        
        let page_size = stats.page_size() as u64;
        let leaf_pages = stats.leaf_pages() as u64;
        let branch_pages = stats.branch_pages() as u64;
        let overflow_pages = stats.overflow_pages() as u64;
        let num_pages = leaf_pages + branch_pages + overflow_pages;
        let table_size = page_size * num_pages;
        
        Some(table_size)
    }).ok().flatten().unwrap_or(0)
}

/// Simple RocksDB wrapper for TransactionHashNumbers
struct RocksDbTxHashNumbers {
    db: DB,
}

impl RocksDbTxHashNumbers {
    fn new(path: &str) -> Self {
        let _ = fs::remove_dir_all(path);
        let mut opts = Options::default();
        opts.create_if_missing(true);

        let db = DB::open(&opts, path).expect("Failed to open RocksDB");
        Self { db }
    }

    fn new_tuned(path: &str) -> Self {
        let _ = fs::remove_dir_all(path);
        let mut opts = Options::default();
        opts.create_if_missing(true);
        
        // Performance tuning options
        opts.set_level_compaction_dynamic_level_bytes(true);
        opts.set_max_background_jobs(6);
        opts.set_bytes_per_sync(1048576);
        opts.set_compaction_pri(CompactionPri::MinOverlappingRatio);
        
        // Block-based table options
        let mut table_opts = BlockBasedOptions::default();
        table_opts.set_block_size(16 * 1024);
        table_opts.set_cache_index_and_filter_blocks(true);
        table_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
        table_opts.set_format_version(5);
        
        opts.set_block_based_table_factory(&table_opts);

        let db = DB::open(&opts, path).expect("Failed to open RocksDB");
        Self { db }
    }

    fn insert_batch(&self, hashes: &[(TxHash, u64)]) {
        let batch_size = 100_000;
        for chunk in hashes.chunks(batch_size) {
            let mut batch = WriteBatch::default();
            for (hash, num) in chunk {
                batch.put(hash.as_slice(), num.to_be_bytes());
            }
            self.db.write(batch).expect("Failed to write batch");
        }
        self.db.flush().expect("Failed to flush");
    }
}

fn main() {
    println!("\n=== Storage Size Measurement ===\n");
    
    let test_sizes = vec![
        ("1M", 1_000_000),
        ("5M", 5_000_000),
        ("15M", 15_000_000),
    ];
    
    for (label, num_entries) in test_sizes {
        println!("--- {} records ---", label);
        
        // Generate test data
        println!("  Generating {} records...", label);
        let mut hashes = Vec::with_capacity(num_entries);
        for i in 0..num_entries {
            hashes.push((TxHash::random(), i as u64));
        }
        
        // MDBX
        println!("  Writing to MDBX...");
        let _ = fs::remove_dir_all(MDBX_BENCH_PATH);
        let mdbx_size = {
            let db = create_test_rw_db_with_path(MDBX_BENCH_PATH);
            {
                let batch_size = 100_000;
                for chunk in hashes.chunks(batch_size) {
                    let tx = db.tx_mut().expect("tx");
                    {
                        let mut cursor = tx.cursor_write::<TransactionHashNumbers>().expect("cursor");
                        
                        for (hash, num) in chunk {
                            cursor.insert(*hash, num).expect("write failed");
                        }
                    }
                    
                    tx.commit().expect("commit failed");
                }
            }
            // Get actual data size from MDBX statistics (not pre-allocated file size)
            let size = get_mdbx_table_size(db.db(), TransactionHashNumbers::NAME);
            drop(db);
            size
        };
        println!("    MDBX:                {}", format_bytes(mdbx_size));
        
        // RocksDB default
        println!("  Writing to RocksDB (default)...");
        let _ = fs::remove_dir_all(ROCKSDB_BENCH_PATH);
        {
            let rocksdb = RocksDbTxHashNumbers::new(ROCKSDB_BENCH_PATH);
            rocksdb.insert_batch(&hashes);
            drop(rocksdb);
        }
        
        let rocksdb_default_size = get_dir_size(ROCKSDB_BENCH_PATH).unwrap_or(0);
        println!("    RocksDB (default):   {}", format_bytes(rocksdb_default_size));
        
        // RocksDB tuned
        println!("  Writing to RocksDB (tuned)...");
        let _ = fs::remove_dir_all(ROCKSDB_BENCH_PATH);
        {
            let rocksdb = RocksDbTxHashNumbers::new_tuned(ROCKSDB_BENCH_PATH);
            rocksdb.insert_batch(&hashes);
            drop(rocksdb);
        }
        
        let rocksdb_tuned_size = get_dir_size(ROCKSDB_BENCH_PATH).unwrap_or(0);
        println!("    RocksDB (tuned):     {}", format_bytes(rocksdb_tuned_size));
        
        // Comparison
        let mdbx_vs_default = rocksdb_default_size as f64 / mdbx_size as f64;
        let mdbx_vs_tuned = rocksdb_tuned_size as f64 / mdbx_size as f64;
        println!("    RocksDB/MDBX ratio:  default={:.2}x, tuned={:.2}x\n", mdbx_vs_default, mdbx_vs_tuned);
        
        // Cleanup RocksDB dirs (MDBX auto-cleans via TempDatabase)
        let _ = fs::remove_dir_all(ROCKSDB_BENCH_PATH);
    }
    
    println!("=== Measurement Complete ===\n");
}

