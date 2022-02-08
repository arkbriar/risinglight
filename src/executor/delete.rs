// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::debug;

use super::*;
use crate::array::DataChunk;
use crate::catalog::TableRefId;
use crate::storage::{RowHandler, Storage, Table, Transaction};

/// The executor of `delete` statement.
pub struct DeleteExecutor<S: Storage> {
    pub table_ref_id: TableRefId,
    pub storage: Arc<S>,
    pub child: BoxedExecutor,
}

impl<S: Storage> DeleteExecutor<S> {
    pub async fn execute_inner(self, ticker_tx: mpsc::Sender<()>) -> Result<i32, ExecutorError> {
        let table = self.storage.get_table(self.table_ref_id)?;
        let mut txn = table.update().await?;
        let mut cnt = 0;
        #[for_await]
        for chunk in self.child {
            // TODO: we do not need a filter executor. We can simply get the boolean value from
            // the child.
            let chunk = chunk?;
            let row_handlers = chunk.array_at(chunk.column_count() - 1);
            for row_handler_idx in 0..row_handlers.len() {
                let row_handler = <S::TransactionType as Transaction>::RowHandlerType::from_column(
                    row_handlers,
                    row_handler_idx,
                );

                if ticker_tx.send(()).await.is_err() {
                    debug!("Abort");
                    return Err(ExecutorError::Abort);
                }
                txn.delete(&row_handler).await?;
                cnt += 1;
            }
        }
        txn.commit().await?;

        Ok(cnt as i32)
    }

    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        let (ticker_tx, mut ticker_rx) = tokio::sync::mpsc::channel(1);

        let handler = tokio::spawn(self.execute_inner(ticker_tx));

        while ticker_rx.recv().await.is_some() {}

        let rows = handler.await.expect("failed to join delete thread")?;
        yield DataChunk::single(rows);
    }
}
