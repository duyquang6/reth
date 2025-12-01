use crate::providers::RocksDBProvider;

/// RocksDB provider factory.
///
/// This trait provides access to the RocksDB provider for tables that are stored in RocksDB
/// instead of the main database (MDBX).
pub trait RocksDBProviderFactory {
    /// Returns a reference to the RocksDB provider.
    fn rocksdb_provider(&self) -> &RocksDBProvider;
}
