//! Stub implementation of RocksDB provider for non-Unix platforms.
//!
//! This module provides placeholder types that allow the code to compile on non-Unix platforms,
//! but will produce errors if RocksDB operations are actually attempted.

use reth_db_api::table::{Decompress, Table};
use reth_storage_errors::provider::ProviderResult;
use std::path::Path;

/// A stub RocksDB provider for non-Unix platforms.
///
/// This type exists to allow code to compile on all platforms, but RocksDB functionality
/// is only available on Unix. On non-Unix platforms, the `transaction_hash_numbers_in_rocksdb`
/// flag should be set to `false` to ensure all operations route to MDBX instead.
#[derive(Debug, Clone)]
pub struct RocksDBProvider;

impl RocksDBProvider {
    /// Creates a new stub RocksDB provider.
    ///
    /// On non-Unix platforms, this returns an error indicating RocksDB is not supported.
    pub fn new<P: AsRef<Path>>(_path: P) -> ProviderResult<Self> {
        Err(reth_storage_errors::provider::ProviderError::UnsupportedProvider)
    }

    /// Creates a new stub RocksDB provider builder.
    pub fn builder<P: AsRef<Path>>(path: P) -> RocksDBBuilder {
        RocksDBBuilder::new(path)
    }

    /// Get a value from RocksDB (stub implementation).
    pub fn get<T>(&self, _key: T::Key) -> ProviderResult<Option<T::Value>>
    where
        T: Table,
        T::Value: Decompress,
    {
        Err(reth_storage_errors::provider::ProviderError::UnsupportedProvider)
    }

    /// Put a value into RocksDB (stub implementation).
    pub fn put<T>(&self, _key: T::Key, _value: T::Value) -> ProviderResult<()>
    where
        T: Table,
    {
        Err(reth_storage_errors::provider::ProviderError::UnsupportedProvider)
    }

    /// Delete a value from RocksDB (stub implementation).
    pub fn delete<T>(&self, _key: T::Key) -> ProviderResult<()>
    where
        T: Table,
    {
        Err(reth_storage_errors::provider::ProviderError::UnsupportedProvider)
    }

    /// Write a batch of operations (stub implementation).
    pub fn write_batch<F>(&self, _f: F) -> ProviderResult<()>
    where
        F: FnOnce(&mut RocksDBBatch) -> ProviderResult<()>,
    {
        Err(reth_storage_errors::provider::ProviderError::UnsupportedProvider)
    }
}

/// A stub batch writer for RocksDB on non-Unix platforms.
#[derive(Debug)]
pub struct RocksDBBatch;

impl RocksDBBatch {
    /// Put a value into the batch (stub implementation).
    pub fn put<T>(&mut self, _key: T::Key, _value: T::Value) -> ProviderResult<()>
    where
        T: Table,
    {
        Err(reth_storage_errors::provider::ProviderError::UnsupportedProvider)
    }
}

/// A stub builder for RocksDB on non-Unix platforms.
#[derive(Debug)]
pub struct RocksDBBuilder;

impl RocksDBBuilder {
    /// Creates a new stub builder.
    pub fn new<P: AsRef<Path>>(_path: P) -> Self {
        Self
    }

    /// Adds a column family for a specific table type (stub implementation).
    pub fn with_table<T: Table>(self) -> Self {
        self
    }

    /// Enables metrics (stub implementation).
    pub const fn with_metrics(self) -> Self {
        self
    }

    /// Enables RocksDB internal statistics collection (stub implementation).
    pub const fn with_statistics(self) -> Self {
        self
    }

    /// Sets the log level from DatabaseArgs configuration (stub implementation).
    pub const fn with_database_log_level(self, _log_level: reth_storage_errors::db::LogLevel) -> Self {
        self
    }

    /// Sets a custom block cache size (stub implementation).
    pub fn with_block_cache_size(self, _capacity_bytes: usize) -> Self {
        self
    }

    /// Build the RocksDB provider (stub implementation).
    pub fn build(self) -> ProviderResult<RocksDBProvider> {
        Err(reth_storage_errors::provider::ProviderError::UnsupportedProvider)
    }
}

