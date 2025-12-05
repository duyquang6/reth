//! Generic reader and writer abstractions for interacting with either database tables or static
//! files.

use std::ops::Range;

use crate::{
    providers::{RocksDBProvider, StaticFileProvider, StaticFileProviderRWRefMut},
    RocksDBProviderFactory, StaticFileProviderFactory,
};
use alloy_primitives::{map::HashMap, Address, BlockNumber, TxHash, TxNumber};
use reth_db::{
    static_file::TransactionSenderMask,
    table::Value,
    transaction::{CursorMutTy, CursorTy, DbTx, DbTxMut},
};
use reth_db_api::{cursor::{DbCursorRO, DbCursorRW}, tables};
use reth_errors::ProviderError;
use reth_node_types::NodePrimitives;
use reth_primitives_traits::ReceiptTy;
use reth_static_file_types::StaticFileSegment;
use reth_storage_api::{DBProvider, NodePrimitivesProvider, StorageSettingsCache};
use reth_storage_errors::provider::ProviderResult;
use strum::{Display, EnumIs};

/// Type alias for [`EitherReader`] constructors.
type EitherReaderTy<P, T> =
    EitherReader<CursorTy<<P as DBProvider>::Tx, T>, <P as NodePrimitivesProvider>::Primitives>;

/// Type alias for [`EitherWriter`] constructors.
type EitherWriterTy<'a, P, T> = EitherWriter<
    'a,
    CursorMutTy<<P as DBProvider>::Tx, T>,
    <P as NodePrimitivesProvider>::Primitives,
>;

/// Represents a destination for writing data, either to database, static files, or RocksDB.
#[derive(Debug, Display)]
pub enum EitherWriter<'a, CURSOR, N> {
    /// Write to database table via cursor
    Database(CURSOR),
    /// Write to static file
    StaticFile(StaticFileProviderRWRefMut<'a, N>),
    /// Write to RocksDB
    RocksDB(&'a RocksDBProvider),
}

impl<'a> EitherWriter<'a, (), ()> {
    /// Creates a new [`EitherWriter`] for transaction hash numbers based on storage settings.
    ///
    /// Routes to RocksDB if `transaction_hash_numbers_in_rocksdb` is enabled, otherwise to MDBX.
    pub fn new_transaction_hash_numbers<P>(
        provider: &'a P,
    ) -> ProviderResult<EitherWriterTy<'a, P, tables::TransactionHashNumbers>>
    where
        P: DBProvider + NodePrimitivesProvider + StorageSettingsCache + RocksDBProviderFactory,
        P::Tx: DbTxMut,
    {
        if provider.cached_storage_settings().transaction_hash_numbers_in_rocksdb {
            return Ok(EitherWriter::RocksDB(provider.rocksdb_provider()));
        }
        Ok(EitherWriter::Database(
            provider.tx_ref().cursor_write::<tables::TransactionHashNumbers>()?,
        ))
    }

    /// Creates a new [`EitherWriter`] for receipts based on storage settings and prune modes.
    pub fn new_receipts<P>(
        provider: &'a P,
        block_number: BlockNumber,
    ) -> ProviderResult<EitherWriterTy<'a, P, tables::Receipts<ReceiptTy<P::Primitives>>>>
    where
        P: DBProvider + NodePrimitivesProvider + StorageSettingsCache + StaticFileProviderFactory,
        P::Tx: DbTxMut,
        ReceiptTy<P::Primitives>: Value,
    {
        if Self::receipts_destination(provider).is_static_file() {
            Ok(EitherWriter::StaticFile(
                provider.get_static_file_writer(block_number, StaticFileSegment::Receipts)?,
            ))
        } else {
            Ok(EitherWriter::Database(
                provider.tx_ref().cursor_write::<tables::Receipts<ReceiptTy<P::Primitives>>>()?,
            ))
        }
    }

    /// Returns the destination for writing receipts.
    ///
    /// The rules are as follows:
    /// - If the node should not always write receipts to static files, and any receipt pruning is
    ///   enabled, write to the database.
    /// - If the node should always write receipts to static files, but receipt log filter pruning
    ///   is enabled, write to the database.
    /// - Otherwise, write to static files.
    pub fn receipts_destination<P: DBProvider + StorageSettingsCache>(
        provider: &P,
    ) -> EitherWriterDestination {
        let receipts_in_static_files = provider.cached_storage_settings().receipts_in_static_files;
        let prune_modes = provider.prune_modes_ref();

        if !receipts_in_static_files && prune_modes.has_receipts_pruning() ||
            // TODO: support writing receipts to static files with log filter pruning enabled
            receipts_in_static_files && !prune_modes.receipts_log_filter.is_empty()
        {
            EitherWriterDestination::Database
        } else {
            EitherWriterDestination::StaticFile
        }
    }

    /// Returns the destination for transaction hash numbers based on storage settings.
    pub fn transaction_hash_numbers_destination<P: DBProvider + StorageSettingsCache>(
        provider: &P,
    ) -> EitherWriterDestination {
        if provider.cached_storage_settings().transaction_hash_numbers_in_rocksdb {
            EitherWriterDestination::RocksDB
        } else {
            EitherWriterDestination::Database
        }
    }

    /// Creates a new [`EitherWriter`] for senders based on storage settings.
    pub fn new_senders<P>(
        provider: &'a P,
        block_number: BlockNumber,
    ) -> ProviderResult<EitherWriterTy<'a, P, tables::TransactionSenders>>
    where
        P: DBProvider + NodePrimitivesProvider + StorageSettingsCache + StaticFileProviderFactory,
        P::Tx: DbTxMut,
    {
        if EitherWriterDestination::senders(provider).is_static_file() {
            Ok(EitherWriter::StaticFile(
                provider
                    .get_static_file_writer(block_number, StaticFileSegment::TransactionSenders)?,
            ))
        } else {
            Ok(EitherWriter::Database(
                provider.tx_ref().cursor_write::<tables::TransactionSenders>()?,
            ))
        }
    }

}

impl<'a, CURSOR, N: NodePrimitives> EitherWriter<'a, CURSOR, N> {
    /// Increment the block number.
    ///
    /// Relevant only for [`Self::StaticFile`]. It is a no-op for [`Self::Database`] and [`Self::RocksDB`].
    pub fn increment_block(&mut self, expected_block_number: BlockNumber) -> ProviderResult<()> {
        match self {
            Self::Database(_) | Self::RocksDB(_) => Ok(()),
            Self::StaticFile(writer) => writer.increment_block(expected_block_number),
        }
    }

    /// Ensures that the writer is positioned at the specified block number.
    ///
    /// If the writer is positioned at a greater block number than the specified one, the writer
    /// will NOT be unwound and the error will be returned.
    ///
    /// Relevant only for [`Self::StaticFile`]. It is a no-op for [`Self::Database`] and [`Self::RocksDB`].
    pub fn ensure_at_block(&mut self, block_number: BlockNumber) -> ProviderResult<()> {
        match self {
            Self::Database(_) | Self::RocksDB(_) => Ok(()),
            Self::StaticFile(writer) => writer.ensure_at_block(block_number),
        }
    }
}

impl<'a, CURSOR, N: NodePrimitives> EitherWriter<'a, CURSOR, N>
where
    N::Receipt: Value,
    CURSOR: DbCursorRW<tables::Receipts<N::Receipt>>,
{
    /// Append a transaction receipt.
    pub fn append_receipt(&mut self, tx_num: TxNumber, receipt: &N::Receipt) -> ProviderResult<()> {
        match self {
            Self::Database(cursor) => Ok(cursor.append(tx_num, receipt)?),
            Self::StaticFile(writer) => writer.append_receipt(tx_num, receipt),
            Self::RocksDB(_) => {
                // Receipts are not stored in RocksDB
                Ok(())
            }
        }
    }
}

impl<'a, CURSOR, N: NodePrimitives> EitherWriter<'a, CURSOR, N>
where
    CURSOR: DbCursorRW<tables::TransactionSenders>,
{
    /// Append a transaction sender to the destination
    pub fn append_sender(&mut self, tx_num: TxNumber, sender: &Address) -> ProviderResult<()> {
        match self {
            Self::Database(cursor) => Ok(cursor.append(tx_num, sender)?),
            Self::StaticFile(writer) => writer.append_transaction_sender(tx_num, sender),
            // Transaction senders are not stored in RocksDB
            Self::RocksDB(_) => Ok(())
        }
    }

    /// Append transaction senders to the destination
    pub fn append_senders<I>(&mut self, senders: I) -> ProviderResult<()>
    where
        I: Iterator<Item = (TxNumber, Address)>,
    {
        match self {
            Self::Database(cursor) => {
                for (tx_num, sender) in senders {
                    cursor.append(tx_num, &sender)?;
                }
                Ok(())
            }
            Self::StaticFile(writer) => writer.append_transaction_senders(senders),
            // Transaction senders are not stored in RocksDB
            Self::RocksDB(_) => Ok(())
        }
    }

    /// Removes all transaction senders above the given transaction number, and stops at the given
    /// block number.
    pub fn prune_senders(
        &mut self,
        unwind_tx_from: TxNumber,
        block: BlockNumber,
    ) -> ProviderResult<()>
    where
        CURSOR: DbCursorRO<tables::TransactionSenders>,
    {
        match self {
            Self::Database(cursor) => {
                let mut walker = cursor.walk_range(unwind_tx_from..)?;
                while walker.next().transpose()?.is_some() {
                    walker.delete_current()?;
                }
            }
            Self::StaticFile(writer) => {
                let static_file_transaction_sender_num = writer
                    .reader()
                    .get_highest_static_file_tx(StaticFileSegment::TransactionSenders);

                let to_delete = static_file_transaction_sender_num
                    .map(|static_num| (static_num + 1).saturating_sub(unwind_tx_from))
                    .unwrap_or_default();

                writer.prune_transaction_senders(to_delete, block)?;
            }
            // RocksDB doesn't support transaction senders
            Self::RocksDB(_) => {}
        }

        Ok(())
    }
}

impl<'a, CURSOR, N: NodePrimitives> EitherWriter<'a, CURSOR, N>
where
    CURSOR: DbCursorRW<tables::TransactionHashNumbers>,
{
    /// Insert transaction hash number mappings in bulk.
    ///
    /// Unlike `append_transaction_hash_numbers`, this method uses insert operation
    /// which can handle unsorted keys. Use this for transaction hashes within a block
    /// where hashes are not naturally sorted.
    ///
    /// # Duplicate Key Behavior
    ///
    /// **Important**: This method has different behavior for duplicate keys depending
    /// on the storage backend:
    ///
    /// - **MDBX (Database)**: `cursor.insert()` will **error** if the key already exists.
    ///   This helps catch bugs during development and testing.
    ///
    /// - **RocksDB**: `batch.put()` will **silently overwrite** existing values.
    ///   This provides better performance in production.
    ///
    /// ## Why This Difference is Safe
    ///
    /// Transaction hash numbers (`TransactionHashNumbers` table) serve as a lookup index
    /// for RPC queries (e.g., `eth_getTransactionByHash`) and have these properties:
    ///
    /// 1. **Non-consensus-critical**: Not used for block validation or state execution
    /// 2. **Reconstructable**: Can be rebuilt from blockchain data if corrupted
    /// 3. **Cryptographically unique**: SHA256 transaction hashes have no collisions
    /// 4. **Properly managed**: Chain reorgs delete old entries before re-inserting
    ///
    /// Therefore:
    /// - In normal operation, duplicates never occur
    /// - During reorgs, data is cleaned up before re-insertion
    /// - If duplicates somehow occur (bugs/corruption), the impact is limited to
    ///   RPC query responses, not chain processing
    ///
    /// The silent overwrite behavior in RocksDB provides better performance without
    /// compromising safety for this read-only lookup table. The error behavior in MDBX
    /// serves as a useful safety net during development.
    pub fn insert_transaction_hash_numbers<I>(&mut self, mappings: I) -> ProviderResult<()>
    where
        I: Iterator<Item = (TxHash, TxNumber)>,
    {
        match self {
            Self::Database(cursor) => {
                for (tx_hash, tx_num) in mappings {
                    cursor.insert(tx_hash, &tx_num)?;
                }
                Ok(())
            }
            Self::RocksDB(rocksdb) => {
                rocksdb.write_batch(|batch| {
                    for (tx_hash, tx_num) in mappings {
                        batch.put::<tables::TransactionHashNumbers>(tx_hash, &tx_num)?;
                    }
                    Ok(())
                })
            }
            Self::StaticFile(_) => {
                // TransactionHashNumbers are not stored in static files
                Ok(())
            }
        }
    }

    /// Delete transaction hash number mappings in bulk.
    ///
    /// This method uses batch deletion for efficient removal of multiple transaction hash mappings.
    /// For RocksDB, it uses write batching for atomic bulk deletes.
    pub fn delete_transaction_hash_numbers<I>(&mut self, hashes: I) -> ProviderResult<()>
    where
        I: Iterator<Item = TxHash>,
        CURSOR: DbCursorRO<tables::TransactionHashNumbers>,
    {
        match self {
            Self::Database(cursor) => {
                for tx_hash in hashes {
                    if cursor.seek_exact(tx_hash)?.is_some() {
                        cursor.delete_current()?;
                    }
                }
                Ok(())
            }
            Self::RocksDB(rocksdb) => {
                rocksdb.write_batch(|batch| {
                    for tx_hash in hashes {
                        batch.delete::<tables::TransactionHashNumbers>(tx_hash)?;
                    }
                    Ok(())
                })
            }
            Self::StaticFile(_) => {
                // TransactionHashNumbers are not stored in static files
                Ok(())
            }
        }
    }

    /// Append transaction hash number mappings in bulk.
    pub fn append_transaction_hash_numbers<I>(&mut self, mappings: I) -> ProviderResult<()>
    where
        I: Iterator<Item = (TxHash, TxNumber)>,
    {
        match self {
            Self::Database(cursor) => {
                for (tx_hash, tx_num) in mappings {
                    cursor.append(tx_hash, &tx_num)?;
                }
                Ok(())
            }
            Self::RocksDB(rocksdb) => {
                rocksdb.write_batch(|batch| {
                    for (tx_hash, tx_num) in mappings {
                        batch.put::<tables::TransactionHashNumbers>(tx_hash, &tx_num)?;
                    }
                    Ok(())
                })
            }
            Self::StaticFile(_) => {
                // TransactionHashNumbers are not stored in static files
                Ok(())
            }
        }
    }
}

/// Represents a source for reading data, either from database or static files.
#[derive(Debug, Display)]
pub enum EitherReader<CURSOR, N> {
    /// Read from database table via cursor
    Database(CURSOR),
    /// Read from static file
    StaticFile(StaticFileProvider<N>),
}

impl EitherReader<(), ()> {
    /// Creates a new [`EitherReader`] for senders based on storage settings.
    pub fn new_senders<P>(
        provider: &P,
    ) -> ProviderResult<EitherReaderTy<P, tables::TransactionSenders>>
    where
        P: DBProvider + NodePrimitivesProvider + StorageSettingsCache + StaticFileProviderFactory,
        P::Tx: DbTx,
    {
        if EitherWriterDestination::senders(provider).is_static_file() {
            Ok(EitherReader::StaticFile(provider.static_file_provider()))
        } else {
            Ok(EitherReader::Database(
                provider.tx_ref().cursor_read::<tables::TransactionSenders>()?,
            ))
        }
    }
}

impl<CURSOR, N: NodePrimitives> EitherReader<CURSOR, N>
where
    CURSOR: DbCursorRO<tables::TransactionSenders>,
{
    /// Fetches the senders for a range of transactions.
    pub fn senders_by_tx_range(
        &mut self,
        range: Range<TxNumber>,
    ) -> ProviderResult<HashMap<TxNumber, Address>> {
        match self {
            Self::Database(cursor) => cursor
                .walk_range(range)?
                .map(|result| result.map_err(ProviderError::from))
                .collect::<ProviderResult<HashMap<_, _>>>(),
            Self::StaticFile(provider) => range
                .clone()
                .zip(provider.fetch_range_iter(
                    StaticFileSegment::TransactionSenders,
                    range,
                    |cursor, number| cursor.get_one::<TransactionSenderMask>(number.into()),
                )?)
                .filter_map(|(tx_num, sender)| {
                    let result = sender.transpose()?;
                    Some(result.map(|sender| (tx_num, sender)))
                })
                .collect::<ProviderResult<HashMap<_, _>>>(),
        }
    }
}

/// Destination for writing data.
#[derive(Debug, EnumIs)]
pub enum EitherWriterDestination {
    /// Write to database table
    Database,
    /// Write to static file
    StaticFile,
    /// Write to RocksDB
    RocksDB,
}

impl EitherWriterDestination {
    /// Returns the destination for writing senders based on storage settings.
    pub fn senders<P>(provider: &P) -> Self
    where
        P: StorageSettingsCache,
    {
        // Write senders to static files only if they're explicitly enabled
        if provider.cached_storage_settings().transaction_senders_in_static_files {
            Self::StaticFile
        } else {
            Self::Database
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::create_test_provider_factory;

    use super::*;
    use alloy_primitives::Address;
    use reth_storage_api::{DatabaseProviderFactory, StorageSettings};

    #[test]
    fn test_reader_senders_by_tx_range() {
        let factory = create_test_provider_factory();

        // Insert senders only from 1 to 4, but we will query from 0 to 5.
        let senders = [
            (1, Address::random()),
            (2, Address::random()),
            (3, Address::random()),
            (4, Address::random()),
        ];

        for transaction_senders_in_static_files in [false, true] {
            factory.set_storage_settings_cache(
                StorageSettings::legacy()
                    .with_transaction_senders_in_static_files(transaction_senders_in_static_files),
            );

            let provider = factory.database_provider_rw().unwrap();
            let mut writer = EitherWriter::new_senders(&provider, 0).unwrap();
            if transaction_senders_in_static_files {
                assert!(matches!(writer, EitherWriter::StaticFile(_)));
            } else {
                assert!(matches!(writer, EitherWriter::Database(_)));
            }

            writer.increment_block(0).unwrap();
            writer.append_senders(senders.iter().copied()).unwrap();
            drop(writer);
            provider.commit().unwrap();

            let provider = factory.database_provider_ro().unwrap();
            let mut reader = EitherReader::new_senders(&provider).unwrap();
            if transaction_senders_in_static_files {
                assert!(matches!(reader, EitherReader::StaticFile(_)));
            } else {
                assert!(matches!(reader, EitherReader::Database(_)));
            }

            assert_eq!(
                reader.senders_by_tx_range(0..6).unwrap(),
                senders.iter().copied().collect::<HashMap<_, _>>(),
                "{reader}"
            );
        }
    }
}
