#![allow(missing_docs)]

use alloy_primitives::TxHash;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::Rng;
use reth_db::{test_utils::create_test_rw_db_with_path, Database, TransactionHashNumbers};
use reth_db_api::{cursor::DbCursorRW, transaction::{DbTx, DbTxMut}};
use rocksdb::{BlockBasedOptions, CompactionPri, Options, WriteBatch, DB};
use std::{fs, sync::Arc};

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = tx_hash_write_1m, tx_hash_write_5m, tx_hash_write_15m, 
              tx_hash_read_1m, tx_hash_read_5m, tx_hash_read_15m
}
criterion_main!(benches);

// Uncomment below to measure storage sizes instead of running benchmarks
// fn main() {
//     measure_storage_sizes();
// }

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
        table_opts.set_format_version(5); // Latest version
        
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

    fn get(&self, hash: TxHash) -> Option<u64> {
        self.db
            .get(hash.as_slice())
            .expect("Failed to read")
            .and_then(|bytes| {
                let arr: [u8; 8] = bytes.as_slice().try_into().ok()?;
                Some(u64::from_be_bytes(arr))
            })
    }
}

/// Benchmark write performance: 1M records
fn tx_hash_write_1m(c: &mut Criterion) {
    let mut group = c.benchmark_group("TransactionHashNumbers Write 1M");
    group.sample_size(10);

    println!("Generating 1M random transaction hashes...");
    let num_entries = 1_000_000;
    let mut hashes = Vec::with_capacity(num_entries);
    for i in 0..num_entries {
        hashes.push((TxHash::random(), i as u64));
    }
    println!("✓ Test data generated");

    // Benchmark MDBX
    group.bench_function("MDBX_write", |b| {
        b.iter_with_setup(
            || {
                // Setup: clean and create fresh DB (not measured)
                let _ = fs::remove_dir_all(MDBX_BENCH_PATH);
                Arc::try_unwrap(create_test_rw_db_with_path(MDBX_BENCH_PATH)).unwrap()
            },
            |db| {
                // Actual benchmark: write all 1M hashes in 100K batches
                let batch_size = 100_000;
                for chunk in hashes.chunks(batch_size) {
                    let tx = db.tx_mut().expect("tx");
                    let mut cursor = tx.cursor_write::<TransactionHashNumbers>().expect("cursor");

                    for (hash, num) in chunk {
                        cursor.insert(*hash, num).expect("write failed");
                    }

                    tx.commit().expect("commit failed");
                }
            },
        )
    });

    // Cleanup
    let _ = fs::remove_dir_all(MDBX_BENCH_PATH);

    // Benchmark RocksDB (default settings)
    group.bench_function("RocksDB_write_default", |b| {
        b.iter_with_setup(
            || {
                RocksDbTxHashNumbers::new(ROCKSDB_BENCH_PATH)
            },
            |db| {
                db.insert_batch(&hashes);
            },
        )
    });

    // Cleanup
    let _ = fs::remove_dir_all(ROCKSDB_BENCH_PATH);

    // Benchmark RocksDB (tuned settings)
    group.bench_function("RocksDB_write_tuned", |b| {
        b.iter_with_setup(
            || {
                RocksDbTxHashNumbers::new_tuned(ROCKSDB_BENCH_PATH)
            },
            |db| {
                db.insert_batch(&hashes);
            },
        )
    });

    // Cleanup
    let _ = fs::remove_dir_all(ROCKSDB_BENCH_PATH);

    group.finish();
}

/// Benchmark write performance: 5M records
fn tx_hash_write_5m(c: &mut Criterion) {
    let mut group = c.benchmark_group("TransactionHashNumbers Write 5M");
    group.sample_size(10);

    println!("Generating 5M random transaction hashes...");
    let num_entries = 5_000_000;
    let mut hashes = Vec::with_capacity(num_entries);
    for i in 0..num_entries {
        hashes.push((TxHash::random(), i as u64));
    }
    println!("✓ Test data generated");

    // Benchmark MDBX
    group.bench_function("MDBX_write", |b| {
        b.iter_with_setup(
            || {
                // Setup: clean and create fresh DB (not measured)
                let _ = fs::remove_dir_all(MDBX_BENCH_PATH);
                Arc::try_unwrap(create_test_rw_db_with_path(MDBX_BENCH_PATH)).unwrap()
            },
            |db| {
                // Actual benchmark: write all 5M hashes in 100K batches
                let batch_size = 100_000;
                for chunk in hashes.chunks(batch_size) {
                    let tx = db.tx_mut().expect("tx");
                    let mut cursor = tx.cursor_write::<TransactionHashNumbers>().expect("cursor");

                    for (hash, num) in chunk {
                        cursor.insert(*hash, num).expect("write failed");
                    }

                    tx.commit().expect("commit failed");
                }
            },
        )
    });

    // Cleanup
    let _ = fs::remove_dir_all(MDBX_BENCH_PATH);

    // Benchmark RocksDB (default settings)
    group.bench_function("RocksDB_write_default", |b| {
        b.iter_with_setup(
            || {
                RocksDbTxHashNumbers::new(ROCKSDB_BENCH_PATH)
            },
            |db| {
                db.insert_batch(&hashes);
            },
        )
    });

    // Cleanup
    let _ = fs::remove_dir_all(ROCKSDB_BENCH_PATH);

    // Benchmark RocksDB (tuned settings)
    group.bench_function("RocksDB_write_tuned", |b| {
        b.iter_with_setup(
            || {
                RocksDbTxHashNumbers::new_tuned(ROCKSDB_BENCH_PATH)
            },
            |db| {
                db.insert_batch(&hashes);
            },
        )
    });

    // Cleanup
    let _ = fs::remove_dir_all(ROCKSDB_BENCH_PATH);

    group.finish();
}

/// Benchmark write performance: 15M records
fn tx_hash_write_15m(c: &mut Criterion) {
    let mut group = c.benchmark_group("TransactionHashNumbers Write 15M");
    group.sample_size(10);

    println!("Generating 15M random transaction hashes...");
    let num_entries = 15_000_000;
    let mut hashes = Vec::with_capacity(num_entries);
    for i in 0..num_entries {
        hashes.push((TxHash::random(), i as u64));
    }
    println!("✓ Test data generated");

    // Benchmark MDBX
    group.bench_function("MDBX_write", |b| {
        b.iter_with_setup(
            || {
                // Setup: clean and create fresh DB (not measured)
                let _ = fs::remove_dir_all(MDBX_BENCH_PATH);
                Arc::try_unwrap(create_test_rw_db_with_path(MDBX_BENCH_PATH)).unwrap()
            },
            |db| {
                // Actual benchmark: write all 15M hashes in 100K batches
                let batch_size = 100_000;
                for chunk in hashes.chunks(batch_size) {
                    let tx = db.tx_mut().expect("tx");
                    let mut cursor = tx.cursor_write::<TransactionHashNumbers>().expect("cursor");

                    for (hash, num) in chunk {
                        cursor.insert(*hash, num).expect("write failed");
                    }

                    tx.commit().expect("commit failed");
                }
            },
        )
    });

    // Cleanup
    let _ = fs::remove_dir_all(MDBX_BENCH_PATH);

    // Benchmark RocksDB (default settings)
    group.bench_function("RocksDB_write_default", |b| {
        b.iter_with_setup(
            || {
                RocksDbTxHashNumbers::new(ROCKSDB_BENCH_PATH)
            },
            |db| {
                db.insert_batch(&hashes);
            },
        )
    });

    // Cleanup
    let _ = fs::remove_dir_all(ROCKSDB_BENCH_PATH);

    // Benchmark RocksDB (tuned settings)
    group.bench_function("RocksDB_write_tuned", |b| {
        b.iter_with_setup(
            || {
                RocksDbTxHashNumbers::new_tuned(ROCKSDB_BENCH_PATH)
            },
            |db| {
                db.insert_batch(&hashes);
            },
        )
    });

    // Cleanup
    let _ = fs::remove_dir_all(ROCKSDB_BENCH_PATH);

    group.finish();
}

/// Benchmark read performance: MDBX vs RocksDB - random reads on 1M records
fn tx_hash_read_1m(c: &mut Criterion) {
    let mut group = c.benchmark_group("TransactionHashNumbers Read 1M");
    group.sample_size(100_000);

    // Setup test data - 1M entries
    println!("Setting up 1M records for read benchmark...");
    let num_entries = 1_000_000;
    let num_reads = 100_000; // 100K random reads
    let mut hashes = Vec::with_capacity(num_entries);
    
    // Generate some hashes that DON'T exist for not-found testing
    let not_found_hashes: Vec<TxHash> = (0..10_000).map(|_| TxHash::random()).collect();

    // Populate MDBX database with 1M records
    println!("Populating MDBX with 1M records...");
    let _ = fs::remove_dir_all(MDBX_BENCH_PATH);
    let db = create_test_rw_db_with_path(MDBX_BENCH_PATH);
    {
        let tx = db.tx_mut().expect("tx");
        let mut cursor = tx.cursor_write::<TransactionHashNumbers>().expect("cursor");

        for i in 0..num_entries {
            let hash = TxHash::random();
            hashes.push(hash);
            let value = i as u64;
            cursor.insert(hash, &value).expect("write failed");
            
            if (i + 1) % 500_000 == 0 {
                println!("  Inserted {}K records", (i + 1) / 1_000);
            }
        }

        tx.commit().expect("commit failed");
    }
    println!("✓ MDBX populated with 1M records");

    let read_samples: Vec<TxHash> = (0..num_reads)
        .map(|i| {
            if i % 10 == 0 {
                not_found_hashes[i % not_found_hashes.len()]
            } else {
                let idx = (i * 7919) % hashes.len();
                hashes[idx]
            }
        })
        .collect();

    // Benchmark MDBX reads - 100K random lookups (mix of found + not found)
    group.bench_function("MDBX_read", |b| {
        b.iter_with_setup(
            || {
                let rand_idx = rand::rng().random_range(0..read_samples.len());
                (db.tx().expect("tx"), read_samples[rand_idx])
            },
            |(tx, hash)| {
                black_box(tx.get::<TransactionHashNumbers>(hash).expect("read failed"));
            },
        )
    });

    // Cleanup
    drop(db);
    let _ = fs::remove_dir_all(MDBX_BENCH_PATH);

    // Populate RocksDB database with 1M records (default settings)
    println!("Populating RocksDB with 1M records (default settings)...");
    let rocksdb = RocksDbTxHashNumbers::new(ROCKSDB_BENCH_PATH);
    let rocksdb_hashes: Vec<(TxHash, u64)> =
        hashes.iter().enumerate().map(|(i, &h)| (h, i as u64)).collect();
    rocksdb.insert_batch(&rocksdb_hashes);
    println!("✓ RocksDB populated with 1M records");

    group.bench_function("RocksDB_read", |b| {
        b.iter_with_setup(
            || {
                let rand_idx = rand::rng().random_range(0..read_samples.len());
                read_samples[rand_idx]
            },
            |hash| {
                black_box(rocksdb.get(hash));
            },
        )
    });

    // Cleanup
    drop(rocksdb);
    let _ = fs::remove_dir_all(ROCKSDB_BENCH_PATH);

    // Populate RocksDB database with 1M records (tuned settings)
    println!("Populating RocksDB with 1M records (tuned settings)...");
    let rocksdb_tuned = RocksDbTxHashNumbers::new_tuned(ROCKSDB_BENCH_PATH);
    rocksdb_tuned.insert_batch(&rocksdb_hashes);
    println!("✓ RocksDB (tuned) populated with 1M records");

    group.bench_function("RocksDB_read_tuned", |b| {
        b.iter_with_setup(
            || {
                let rand_idx = rand::rng().random_range(0..read_samples.len());
                read_samples[rand_idx]
            },
            |hash| {
                black_box(rocksdb_tuned.get(hash));
            },
        )
    });

    // Cleanup
    drop(rocksdb_tuned);
    let _ = fs::remove_dir_all(ROCKSDB_BENCH_PATH);

    group.finish();
}

/// Benchmark read performance: MDBX vs RocksDB - random reads on 5M records
fn tx_hash_read_5m(c: &mut Criterion) {
    let mut group = c.benchmark_group("TransactionHashNumbers Read 5M");
    group.sample_size(100_000);

    // Setup test data - 5M entries
    println!("Setting up 5M records for read benchmark...");
    let num_entries = 5_000_000;
    let num_reads = 100_000; // 100K random reads
    let mut hashes = Vec::with_capacity(num_entries);
    
    // Generate some hashes that DON'T exist for not-found testing
    let not_found_hashes: Vec<TxHash> = (0..10_000).map(|_| TxHash::random()).collect();

    // Populate MDBX database with 5M records
    println!("Populating MDBX with 5M records...");
    let _ = fs::remove_dir_all(MDBX_BENCH_PATH);
    let db = create_test_rw_db_with_path(MDBX_BENCH_PATH);
    {
        let tx = db.tx_mut().expect("tx");
        let mut cursor = tx.cursor_write::<TransactionHashNumbers>().expect("cursor");

        for i in 0..num_entries {
            let hash = TxHash::random();
            hashes.push(hash);
            let value = i as u64;
            cursor.insert(hash, &value).expect("write failed");
            
            if (i + 1) % 1_000_000 == 0 {
                println!("  Inserted {}M records", (i + 1) / 1_000_000);
            }
        }

        tx.commit().expect("commit failed");
    }
    println!("✓ MDBX populated with 5M records");

    let read_samples: Vec<TxHash> = (0..num_reads)
        .map(|i| {
            if i % 10 == 0 {
                not_found_hashes[i % not_found_hashes.len()]
            } else {
                let idx = (i * 7919) % hashes.len();
                hashes[idx]
            }
        })
        .collect();

    // Benchmark MDBX reads - 100K random lookups (mix of found + not found)
    group.bench_function("MDBX_read", |b| {
        b.iter_with_setup(
            || {
                let rand_idx = rand::rng().random_range(0..read_samples.len());
                (db.tx().expect("tx"), read_samples[rand_idx])
            },
            |(tx, hash)| {
                black_box(tx.get::<TransactionHashNumbers>(hash).expect("read failed"));
            },
        )
    });

    // Cleanup
    drop(db);
    let _ = fs::remove_dir_all(MDBX_BENCH_PATH);

    // Populate RocksDB database with 5M records (default settings)
    println!("Populating RocksDB with 5M records (default settings)...");
    let rocksdb = RocksDbTxHashNumbers::new(ROCKSDB_BENCH_PATH);
    let rocksdb_hashes: Vec<(TxHash, u64)> =
        hashes.iter().enumerate().map(|(i, &h)| (h, i as u64)).collect();
    rocksdb.insert_batch(&rocksdb_hashes);
    println!("✓ RocksDB populated with 5M records");

    group.bench_function("RocksDB_read", |b| {
        b.iter_with_setup(
            || {
                let rand_idx = rand::rng().random_range(0..read_samples.len());
                read_samples[rand_idx]
            },
            |hash| {
                black_box(rocksdb.get(hash));
            },
        )
    });

    // Cleanup
    drop(rocksdb);
    let _ = fs::remove_dir_all(ROCKSDB_BENCH_PATH);

    // Populate RocksDB database with 5M records (tuned settings)
    println!("Populating RocksDB with 5M records (tuned settings)...");
    let rocksdb_tuned = RocksDbTxHashNumbers::new_tuned(ROCKSDB_BENCH_PATH);
    rocksdb_tuned.insert_batch(&rocksdb_hashes);
    println!("✓ RocksDB (tuned) populated with 5M records");

    group.bench_function("RocksDB_read_tuned", |b| {
        b.iter_with_setup(
            || {
                let rand_idx = rand::rng().random_range(0..read_samples.len());
                read_samples[rand_idx]
            },
            |hash| {
                black_box(rocksdb_tuned.get(hash));
            },
        )
    });

    // Cleanup
    drop(rocksdb_tuned);
    let _ = fs::remove_dir_all(ROCKSDB_BENCH_PATH);

    group.finish();
}

/// Benchmark read performance: MDBX vs RocksDB - random reads on 15M records
fn tx_hash_read_15m(c: &mut Criterion) {
    let mut group = c.benchmark_group("TransactionHashNumbers Read 15M");
    group.sample_size(100_000);

    // Setup test data - 15M entries
    println!("Setting up 15M records for read benchmark...");
    let num_entries = 15_000_000;
    let num_reads = 100_000; // 100K random reads
    let mut hashes = Vec::with_capacity(num_entries);
    
    // Generate some hashes that DON'T exist for not-found testing
    let not_found_hashes: Vec<TxHash> = (0..10_000).map(|_| TxHash::random()).collect();

    // Populate MDBX database with 15M records
    println!("Populating MDBX with 15M records...");
    let _ = fs::remove_dir_all(MDBX_BENCH_PATH);
    let db = create_test_rw_db_with_path(MDBX_BENCH_PATH);
    {
        let tx = db.tx_mut().expect("tx");
        let mut cursor = tx.cursor_write::<TransactionHashNumbers>().expect("cursor");

        for i in 0..num_entries {
            let hash = TxHash::random();
            hashes.push(hash);
            let value = i as u64;
            cursor.insert(hash, &value).expect("write failed");
            
            if (i + 1) % 1_000_000 == 0 {
                println!("  Inserted {}M records", (i + 1) / 1_000_000);
            }
        }

        tx.commit().expect("commit failed");
    }
    println!("✓ MDBX populated with 15M records");

    let read_samples: Vec<TxHash> = (0..num_reads)
        .map(|i| {
            if i % 10 == 0 {
                not_found_hashes[i % not_found_hashes.len()]
            } else {
                let idx = (i * 7919) % hashes.len();
                hashes[idx]
            }
        })
        .collect();

    // Benchmark MDBX reads - 100K random lookups (mix of found + not found)
    group.bench_function("MDBX_read", |b| {
        b.iter_with_setup(
            || {
                let rand_idx = rand::rng().random_range(0..read_samples.len());
                (db.tx().expect("tx"), read_samples[rand_idx])
            },
            |(tx, hash)| {
                black_box(tx.get::<TransactionHashNumbers>(hash).expect("read failed"));
            },
        )
    });

    // Cleanup
    drop(db);
    let _ = fs::remove_dir_all(MDBX_BENCH_PATH);

    // Populate RocksDB database with 15M records (default settings)
    println!("Populating RocksDB with 15M records (default settings)...");
    let rocksdb = RocksDbTxHashNumbers::new(ROCKSDB_BENCH_PATH);
    let rocksdb_hashes: Vec<(TxHash, u64)> =
        hashes.iter().enumerate().map(|(i, &h)| (h, i as u64)).collect();
    rocksdb.insert_batch(&rocksdb_hashes);
    println!("✓ RocksDB populated with 15M records");

    group.bench_function("RocksDB_read", |b| {
        b.iter_with_setup(
            || {
                let rand_idx = rand::rng().random_range(0..read_samples.len());
                read_samples[rand_idx]
            },
            |hash| {
                black_box(rocksdb.get(hash));
            },
        )
    });

    // Cleanup
    drop(rocksdb);
    let _ = fs::remove_dir_all(ROCKSDB_BENCH_PATH);

    // Populate RocksDB database with 15M records (tuned settings)
    println!("Populating RocksDB with 15M records (tuned settings)...");
    let rocksdb_tuned = RocksDbTxHashNumbers::new_tuned(ROCKSDB_BENCH_PATH);
    rocksdb_tuned.insert_batch(&rocksdb_hashes);
    println!("✓ RocksDB (tuned) populated with 15M records");

    group.bench_function("RocksDB_read_tuned", |b| {
        b.iter_with_setup(
            || {
                let rand_idx = rand::rng().random_range(0..read_samples.len());
                read_samples[rand_idx]
            },
            |hash| {
                black_box(rocksdb_tuned.get(hash));
            },
        )
    });

    // Cleanup
    drop(rocksdb_tuned);
    let _ = fs::remove_dir_all(ROCKSDB_BENCH_PATH);

    group.finish();
}

/// Helper function to measure storage sizes for different configurations
/// To run: Comment out criterion_main!() and uncomment the main() below, then:
/// cargo run --bin tx_hash_number --features test-utils --release
#[allow(dead_code)]
fn measure_storage_sizes() {
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
        {
            let db = create_test_rw_db_with_path(MDBX_BENCH_PATH);
            let batch_size = 100_000;
            for chunk in hashes.chunks(batch_size) {
                let tx = db.tx_mut().expect("tx");
                let mut cursor = tx.cursor_write::<TransactionHashNumbers>().expect("cursor");
                
                for (hash, num) in chunk {
                    cursor.insert(*hash, num).expect("write failed");
                }
                
                tx.commit().expect("commit failed");
            }
        }
        
        let mdbx_size = get_dir_size(MDBX_BENCH_PATH).unwrap_or(0);
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
        
        // Cleanup
        let _ = fs::remove_dir_all(MDBX_BENCH_PATH);
        let _ = fs::remove_dir_all(ROCKSDB_BENCH_PATH);
    }
    
    println!("=== Measurement Complete ===\n");
}
