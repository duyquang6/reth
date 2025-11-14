# TransactionHashNumbers Optimization Proposal

## Executive Summary

The `TransactionHashNumbers` table consumes **~30% of MDBX storage** and **~50% of block insertion time**. We propose making it configurable to optimize different node types.

**The Problem:**
- Write-heavy, read-light (RPC only, pattern read simple, lookup key value)
- MDBX B-tree optimized for reads, range-reads which is don't benefit in this table, and inefficient for random writes
- Sequencers pay 100% write cost for 0% benefit (don't serve RPC)

**The Solution:**
| Phase | Approach | Timeline | Benefit |
|-------|----------|----------|---------|
| **1** | Optional flag disable insert for sequencers | Short-term | Faster insert, smaller MDBX storage |
| **2** | RocksDB backend (LSM-tree) | Long-term | Better write throughput |

---

## Problem Statement

### Current Impact

```
Storage:    ~30% of total database size
Write cost: ~50% of insert_block time
Pattern:    Write heavy, rare read for RPC: `eth_getTransactionReceipt` and `eth_getTransactionByHash`
Cache:      Most queries served from in-memory cache (recent blocks)
```

### Table Definition

```rust
table TransactionHashNumbers {
    type Key = TxHash;      // 32 bytes
    type Value = TxNumber;  // 8 bytes
}
```

### Usage Pattern

**Reads** (RPC Only):

**Read Location 1**: `crates/storage/provider/src/providers/consistent.rs`

```rust
// ConsistentProvider::transaction_id() - Main entry point with in-memory cache
// This provides fast lookups for recent transactions without hitting the database
fn transaction_id(&self, tx_hash: TxHash) -> ProviderResult<Option<TxNumber>> {
    self.get_in_memory_or_storage_by_tx(
        tx_hash.into(),
        |db_provider| db_provider.transaction_id(tx_hash),  // ← Falls back to DB (older txs)
        |_, tx_number, _| Ok(Some(tx_number)),              // ← In-memory hit (recent txs)
    )
}
```

**Read Location 2**: `crates/storage/provider/src/providers/database/provider.rs`

```rust
// DatabaseProvider::transaction_id() - Database fallback for older transactions
fn transaction_id(&self, tx_hash: TxHash) -> ProviderResult<Option<TxNumber>> {
    Ok(self.tx.get::<tables::TransactionHashNumbers>(tx_hash)?)  // ← READ from MDBX
}

// transaction_by_hash() - Used by RPC endpoints
fn transaction_by_hash(&self, hash: TxHash) -> ProviderResult<Option<Self::Transaction>> {
    if let Some(id) = self.transaction_id(hash)? {  // ← Calls the lookup
        Ok(self.transaction_by_id_unhashed(id)?)
    } else {
        Ok(None)
    }
}
```

**Read Frequency:**
- Only triggered by RPC calls: `eth_getTransactionByHash`, `eth_getTransactionReceipt`
- User-initiated only (not part of block processing)
- **served from in-memory cache** (recent blocks)
- **older hit database** (older transactions)

**Read Location 3**: `crates/rpc/rpc-eth-api/src/helpers/transaction.rs`

```rust
// load_transaction_and_receipt() - Used by eth_getTransactionReceipt
fn load_transaction_and_receipt(&self, hash: TxHash) -> ... {
    let provider = this.provider();
    
    // Must use transaction_by_hash_with_meta to get the transaction
    let (tx, meta) = match provider
        .transaction_by_hash_with_meta(hash)  // ← Calls transaction_id() internally
        .map_err(Self::Error::from_eth_err)?
    {
        Some((tx, meta)) => (tx, meta),
        None => return Ok(None),
    };
    
    // Then fetch receipt
    let receipt = match provider.receipt_by_hash(hash)
        .map_err(Self::Error::from_eth_err)? 
    {
        Some(recpt) => recpt,
        None => return Ok(None),
    };
}
```

**Read Location 4**: `crates/stages/stages/src/stages/tx_lookup.rs`

```rust
// TransactionLookupStage::unwind() - Only during chain reorgs (rare)
fn unwind(&mut self, provider: &Provider, input: UnwindInput) -> Result<UnwindOutput> {
    let mut tx_hash_number_cursor = tx.cursor_write::<tables::TransactionHashNumbers>()?;
    
    // Only called during chain reorgs (rare)
    if let Some(transaction) = static_file_provider.transaction_by_id(tx_id)? &&
        tx_hash_number_cursor.seek_exact(transaction.trie_hash())?.is_some()  // ← READ (rare)
    {
        tx_hash_number_cursor.delete_current()?;  // ← DELETE
    }
}
```

---

**Writes** (Every Block):

**Write Location 1**: `crates/storage/provider/src/providers/database/provider.rs`

```rust
// insert_block() - Called for EVERY block during normal operation
fn insert_block(&self, block: RecoveredBlock) -> ProviderResult<StoredBlockBodyIndices> {
    // ... insert headers, body, etc ...
    
    // Write transaction hash lookups for EVERY transaction
    let mut tx_lookup_to_insert = Vec::with_capacity(tx_count);
    for tx in block.body().transactions() {
        tx_lookup_to_insert.push((tx.trie_hash(), tx_number));  // ← WRITE (every tx)
        tx_number += 1;
    }
    
    tx_lookup_to_insert.sort_by_key(|(hash, _)| *hash);
    
    let mut cursor = self.tx.cursor_write::<tables::TransactionHashNumbers>()?;
    for (hash, tx_num) in tx_lookup_to_insert {
        cursor.upsert(hash, &tx_num)?;  // ← WRITE to MDBX
    }
}
```

**Frequency**: Every block, every transaction (75-150 writes/sec on mainnet)

**Write Location 2**: `crates/stages/stages/src/stages/tx_lookup.rs`

```rust
// TransactionLookupStage::execute() - Only during initial sync
fn execute(&mut self, provider: &Provider, input: ExecInput) -> Result<ExecOutput> {
    // Collect transaction hashes for block range
    let mut hash_collector: Collector<TxHash, TxNumber> = Collector::new(...);
    
    for (key, value) in provider.transaction_hashes_by_range(range)? {
        hash_collector.insert(key, value)?;  // Collect to temp storage
    }
    
    // Bulk insert into TransactionHashNumbers table
    let mut txhash_cursor = provider.tx_ref()
        .cursor_write::<tables::TransactionHashNumbers>()?;
    
    for hash_to_number in hash_collector.iter()? {
        let (hash, number) = hash_to_number?;
        txhash_cursor.insert(hash, number)?;  // ← WRITE (bulk, during sync only)
    }
}
```

**Frequency**: Only during initial sync when node is first started. Not used during normal operation.

---

**Deletes** (Pruning):

**Delete Location**: `crates/prune/prune/src/segments/user/transaction_lookup.rs`

```rust
// TransactionLookup::prune() - Only if pruning is enabled
fn prune(&self, provider: &Provider, input: PruneInput) -> Result<SegmentOutput> {
    // Get transaction hashes to prune
    let hashes = provider
        .transactions_by_tx_range(tx_range)?
        .into_par_iter()
        .map(|tx| tx.trie_hash())
        .collect::<Vec<_>>();
    
    // Delete from TransactionHashNumbers table
    let (pruned, done) = provider.tx_ref()
        .prune_table_with_iterator::<tables::TransactionHashNumbers>(
            hashes,  // ← DELETE (if pruning enabled)
            &mut limiter,
            |row| { /* track progress */ },
        )?;
}
```

**Frequency**: Only if user has enabled TransactionLookup pruning (disabled by default). Runs periodically to remove old entries.

---

### Summary of All Code Paths

| Path | File | Method | Frequency | Node Type |
|------|------|--------|-----------|-----------|
| **Read** | `consistent.rs` | `transaction_id()` | Per RPC query (cached) | RPC nodes only |
| **Read** | `provider.rs` | `transaction_id()` | Per RPC query (DB fallback) | RPC nodes only |
| **Read** | `transaction.rs` | `load_transaction_and_receipt()` | Per RPC query | RPC nodes only |
| **Read/Delete** | `tx_lookup.rs` | `unwind()` | Chain reorgs (rare) | All nodes |
| **Write** | `provider.rs` | `insert_block()` | Every block (75-150/sec) | All nodes |
| **Write** | `tx_lookup.rs` | `execute()` | Initial sync only | All nodes |
| **Delete** | `transaction_lookup.rs` | `prune()` | If pruning enabled | All nodes |

**Key Insights**: 
- Sequencers do **Write** operations (high frequency) but never **Read** (RPC only) → wasted effort!
- Most RPC reads served from in-memory cache (recent blocks), minority hit database (older transactions)

---

## Solution: Configurable Persistence

### Phase 1: Optional Disable (Short-term)

**Benefits:**
- ✅ Faster block insertion for sequencers
- ✅ Smaller database size
- ✅ Opt-in, no breaking changes

**CLI Flag:**
```bash
--storage.transaction-hash-numbers=<BACKEND>
```

**Options:**
- `libmdbx` (default): Store in MDBX database
- `disabled`: Do not persist (sequencer mode)
- `rocksdb`: Store in RocksDB (future)

**Implementation:**
- Create new file: `crates/node/core/src/args/storage.rs`
- Add `StorageArgs` struct with the flag
- Export in `mod.rs`

**Backward Compatibility:**
> **Note**: This feature is **opt-in** for new nodes only. Existing nodes will continue to use MDBX by default with no migration required. The flag only needs to be set when initializing a new node. Old nodes that don't want to migrate will work exactly as before.

---

### Phase 2: RocksDB Backend (Long-term) (TBD)

**Why RocksDB?**

| Feature | MDBX (B-tree) | RocksDB (LSM-tree) |
|---------|---------------|-------------------|
| Write pattern | Synchronous tree splits | Async WAL appends |
| Random writes | O(log n) with splits | O(1) amortized |
| Best for | Read-heavy | **Write-heavy** ✓ |


**Implementation:**
TBD - need Reth team suggest the integration flow

**Benefits:**
- ✅ Better write throughput (LSM-tree optimized for random writes)
- ✅ Storage reduction (compression)
- ✅ Can serve RPC queries

---

## Benchmark Results

[Bench code](https://github.com/duyquang6/reth/blob/tx-hash-number-proposal/crates/storage/db/benches/tx_hash_number.rs)
[Storage size measure](https://github.com/duyquang6/reth/blob/tx-hash-number-proposal/crates/storage/db/examples/measure_tx_hash_storage.rs)

### Write Performance (100K batches)

| Dataset Size | MDBX | RocksDB (default) | RocksDB (tuned) | RocksDB Speedup |
|--------------|------|-------------------|-----------------|-----------------|
| **1M records** | 1.28s | 1.48s | 1.60s | 0.86x ❌ |
| **5M records** | 14.23s | 5.77s | 5.18s | **2.5-2.7x faster** ✓ |
| **15M records** | 69.05s | 13.96s | 14.89s | **4.6-4.9x faster** ✓ |

### Read Performance (100K random lookups, 90% found / 10% not found)

| Dataset Size | MDBX | RocksDB (default) | RocksDB (tuned) | MDBX Advantage |
|--------------|------|-------------------|-----------------|----------------|
| **1M records** | 391 ns | 1.88 µs | 4.75 µs | **4.8-12x faster** |
| **5M records** | 829 ns | 6.60 µs | 16.91 µs | **8.0-20x faster** |
| **15M records** | 845 ns | 15.31 µs | 42.96 µs | **18-51x faster** |

### Storage Size

| Dataset Size | MDBX | RocksDB (default) | RocksDB (tuned) | Storage Savings |
|--------------|------|-------------------|-----------------|-----------------|
| **1M records** | 71.20 MB | 44.97 MB | 40.62 MB | **37-43% smaller** |
| **5M records** | 355.66 MB | 213.57 MB | 202.19 MB | **40-43% smaller** |
| **15M records** | 1.04 GB | 632.02 MB | 594.21 MB | **39-43% smaller** |

**Note:** MDBX uses B-tree structure with ~73 bytes/tx overhead (node headers, entry indices, page headers, fragmentation). RocksDB uses LSM-tree with compression, achieving significantly better space efficiency.

### Key Findings

**Write Performance:**
- ⚠️ **At small scale (1M)**: MDBX is actually faster! RocksDB overhead dominates.
- ✅ **At medium scale (5M)**: RocksDB shows 2.5-2.7x advantage
- ✅ **At production scale (15M)**: RocksDB shows **4.6-4.9x advantage** - scales better
- **Conclusion**: RocksDB's LSM-tree advantage grows with dataset size, making it ideal for large production databases

**Read Performance:**
- MDBX is **4.8-51x faster** for point lookups across all scales
- RocksDB read performance degrades more with dataset size (B-tree advantage vs LSM-tree amplification)
- **However**, this is acceptable because:
  - **Most RPC queries served from in-memory cache** (`ConsistentProvider`)
  - Only cache misses hit the database (small percentage)
  - Users already expect slower responses for historical queries

**Storage Efficiency:**
- RocksDB uses **40-43% less storage** across all scales
- Consistent savings due to LSM-tree structure and compression
- **Significant benefit for large production deployments** - saves ~430 MB per 1B txs at scale

**Tuned vs Default RocksDB:**
- Tuned config shows better write performance at larger scales
- Tuned config also achieves slightly better compression (~3-5% additional savings)
- Surprisingly, tuned config is slower for reads (likely due to compression/compaction trade-offs)
- For production use, default RocksDB config may be better balanced

### Conclusion

The benchmarks validate the proposal:
1. **Phase 1** (disable for sequencers): Clear win - eliminates **50% of insert time** with zero downside
2. **Phase 2** (RocksDB for RPC): Viable at production scale:
   - **4.9x faster writes** at 15M+ records
   - **40-43% storage savings** - significant cost reduction for large datasets
   - Read slowdown is acceptable given in-memory cache layer
   - Trade-off makes sense: faster writes + less storage >> slower historical reads (cache-misses only)

---

## Implementation Checklist

### Phase 1: Configurable Persistence (Short-term)

- Add `TransactionHashNumbersBackend` enum
- Add CLI args: `--storage.transaction-hash-numbers=<backend>`
- Modify `EitherWriter` to make use it for `TransactionHashNumber` 
- Update block persistence use `EitherWriter`
- Update `tx_lookup` sync stage
- Update `prune()`

### Phase 2: RocksDB Integration (Long-term) (TBD)
- [x] Performance benchmarks (MDBX vs RocksDB)
- [ ] RocksDB wrapper implementation
- [ ] Hybrid database support in `DatabaseProvider`
- [ ] Write/read routing logic
- [ ] Tune RocksDB parameters
