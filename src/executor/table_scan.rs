// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::*;
use crate::array::{ArrayBuilder, ArrayBuilderImpl, DataChunk, I64ArrayBuilder};
use crate::binder::BoundExpr;
use crate::optimizer::plan_nodes::PhysicalTableScan;
use crate::storage::{Storage, StorageColumnRef, Table, Transaction, TxnIterator};

/// The executor of table scan operation.
pub struct TableScanExecutor<S: Storage> {
    pub plan: PhysicalTableScan,
    pub expr: Option<BoundExpr>,
    pub storage: Arc<S>,
}

impl<S: Storage> TableScanExecutor<S> {
    /// Some executors will fail if no chunk is returned from `SeqScanExecutor`. After we have
    /// schema information in executors, this function can be removed.
    fn build_empty_chunk(&self, table: &impl Table) -> Result<DataChunk, ExecutorError> {
        let columns = table.columns()?;
        let mut col_idx = self
            .plan
            .logical()
            .column_ids()
            .iter()
            .map(|x| StorageColumnRef::Idx(*x))
            .collect_vec();

        // Add an extra column for RowHandler at the end
        if self.plan.logical().with_row_handler() {
            col_idx.push(StorageColumnRef::RowHandler);
        }

        // Get n array builders
        let mut builders = self
            .plan
            .logical()
            .column_ids()
            .iter()
            .map(|&id| columns.iter().find(|col| col.id() == id).unwrap())
            .map(|col| ArrayBuilderImpl::new(&col.datatype()))
            .collect::<Vec<ArrayBuilderImpl>>();

        if self.plan.logical().with_row_handler() {
            builders.push(ArrayBuilderImpl::Int64(I64ArrayBuilder::new()));
        }

        let chunk = builders.into_iter().collect();

        Ok(chunk)
    }

    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute_inner(self, token: CancellationToken) {
        let table = self.storage.get_table(self.plan.logical().table_ref_id())?;

        // TODO: remove this when we have schema
        let empty_chunk = self.build_empty_chunk(&table)?;
        let mut have_chunk = false;

        let mut col_idx = self
            .plan
            .logical()
            .column_ids()
            .iter()
            .map(|x| StorageColumnRef::Idx(*x))
            .collect_vec();

        // Add an extra column for RowHandler at the end
        if self.plan.logical().with_row_handler() {
            col_idx.push(StorageColumnRef::RowHandler);
        }

        if token.is_cancelled() {
            return Err(ExecutorError::Abort);
        }
        let txn = table.read().await?;

        let mut it = match executor_token_await(
            &token,
            txn.scan(
                None,
                None,
                &col_idx,
                self.plan.logical().is_sorted(),
                false,
                self.expr,
            ),
        )
        .await
        {
            Ok(it) => it,
            Err(err) => {
                txn.abort().await?;
                return Err(err);
            }
        };

        loop {
            let chunk = match token_await(&token, it.next_batch(None)).await {
                Ok(chunk) => chunk,
                Err(err) => {
                    txn.abort().await?;
                    return Err(err);
                }
            };
            match chunk {
                Ok(x) => {
                    if let Some(x) = x {
                        yield x;
                        have_chunk = true;
                    } else {
                        break;
                    }
                }
                Err(err) => {
                    txn.abort().await?;
                    return Err(err.into());
                }
            }
        }

        txn.abort().await?;

        if !have_chunk {
            yield empty_chunk;
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self, context: Arc<Context>) {
        // Buffer at most 128 chunks in memory
        let (tx, mut rx) = tokio::sync::mpsc::channel(128);

        let child_token = context.child_cancellation_token();
        let handler = tokio::spawn(async move {
            let mut stream = self.execute_inner(child_token);
            while let Some(result) = stream.next().await {
                if tx.send(result).await.is_err() {
                    warn!("Abort");
                    return Err(ExecutorError::Abort);
                }
            }
            Ok(())
        });

        while let Some(item) = rx.recv().await {
            yield item?;
        }
        handler.await.expect("failed to join scan thread")?;
    }
}
