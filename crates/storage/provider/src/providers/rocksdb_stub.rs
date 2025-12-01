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
        Err(reth_storage_errors::provider::ProviderError::Custom(
            "RocksDB is only supported on Unix platforms".to_string(),
        ))
    }

    /// Get a value from RocksDB (stub implementation).
    pub fn get<T>(&self, _key: T::Key) -> ProviderResult<Option<T::Value>>
    where
        T: Table,
        T::Value: Decompress,
    {
        Err(reth_storage_errors::provider::ProviderError::Custom(
            "RocksDB is only supported on Unix platforms".to_string(),
        ))
    }

    /// Put a value into RocksDB (stub implementation).
    pub fn put<T>(&self, _key: T::Key, _value: T::Value) -> ProviderResult<()>
    where
        T: Table,
    {
        Err(reth_storage_errors::provider::ProviderError::Custom(
            "RocksDB is only supported on Unix platforms".to_string(),
        ))
    }

    /// Delete a value from RocksDB (stub implementation).
    pub fn delete<T>(&self, _key: T::Key) -> ProviderResult<()>
    where
        T: Table,
    {
        Err(reth_storage_errors::provider::ProviderError::Custom(
            "RocksDB is only supported on Unix platforms".to_string(),
        ))
    }

    /// Write a batch of operations (stub implementation).
    pub fn write_batch<F>(&self, _f: F) -> ProviderResult<()>
    where
        F: FnOnce(&mut RocksDBBatch) -> ProviderResult<()>,
    {
        Err(reth_storage_errors::provider::ProviderError::Custom(
            "RocksDB is only supported on Unix platforms".to_string(),
        ))
    }
}

/// A stub batch writer for RocksDB on non-Unix platforms.
pub struct RocksDBBatch;

impl RocksDBBatch {
    /// Put a value into the batch (stub implementation).
    pub fn put<T>(&mut self, _key: T::Key, _value: T::Value) -> ProviderResult<()>
    where
        T: Table,
    {
        Err(reth_storage_errors::provider::ProviderError::Custom(
            "RocksDB is only supported on Unix platforms".to_string(),
        ))
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

    /// Build the RocksDB provider (stub implementation).
    pub fn build(self) -> ProviderResult<RocksDBProvider> {
        Err(reth_storage_errors::provider::ProviderError::Custom(
            "RocksDB is only supported on Unix platforms".to_string(),
        ))
    }
}

