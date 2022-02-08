// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fs::{self, File};
use std::path::PathBuf;

use tokio::sync::mpsc;

use super::*;
use crate::binder::FileFormat;

/// The executor of saving data to file.
pub struct CopyToFileExecutor {
    pub path: PathBuf,
    pub format: FileFormat,
    pub child: BoxedExecutor,
}

impl CopyToFileExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        let Self {
            path,
            format,
            child,
        } = self;

        let (tx, rx) = mpsc::channel(1);
        let (complete_tx, mut complete_rx) = mpsc::channel(1);
        let writer = tokio::task::spawn_blocking(|| {
            Self::write_file_blocking(path, format, rx, complete_tx)
        });

        #[for_await]
        for batch in child {
            let res = tx.send(batch?).await;
            if res.is_err() {
                // send error means the background IO task returns error.
                break;
            }
        }
        drop(tx);

        // Block on the complete signal.
        complete_rx.recv().await;

        let rows = writer.await.unwrap()?;

        yield DataChunk::single(rows as _);
    }

    fn write_file_blocking(
        path: PathBuf,
        format: FileFormat,
        mut rx: mpsc::Receiver<DataChunk>,
        complete_tx: mpsc::Sender<()>,
    ) -> Result<usize, ExecutorError> {
        let file = File::create(&path)?;
        let mut writer = match format {
            FileFormat::Csv {
                delimiter,
                quote,
                escape,
                header,
            } => csv::WriterBuilder::new()
                .delimiter(delimiter as u8)
                .quote(quote as u8)
                .escape(escape.unwrap_or(quote) as u8)
                .has_headers(header)
                .from_writer(file),
        };

        let mut rows = 0;
        while let Some(chunk) = rx.blocking_recv() {
            for i in 0..chunk.cardinality() {
                // TODO(wrj): avoid dynamic memory allocation (String)
                let row = chunk.arrays().iter().map(|a| a.get_to_string(i));
                writer.write_record(row)?;
            }
            writer.flush()?;
            rows += chunk.cardinality();
        }

        // Emit complete signal. Error means complete_rx is closed, i.e. tx is closed too.
        // In such situation, we remove the file.
        if complete_tx.blocking_send(()).is_err() {
            drop(writer);
            fs::remove_file(&path)?;
            return Err(ExecutorError::Abort);
        }

        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::ArrayImpl;

    #[tokio::test]
    async fn write_csv() {
        let file = tempfile::NamedTempFile::new().expect("failed to create temp file");

        let executor = CopyToFileExecutor {
            path: file.path().into(),
            format: FileFormat::Csv {
                delimiter: ',',
                quote: '"',
                escape: None,
                header: false,
            },
            child: async_stream::try_stream! {
                yield [
                    ArrayImpl::Int32([1, 2].into_iter().collect()),
                    ArrayImpl::Float64([1.5, 2.5].into_iter().collect()),
                    ArrayImpl::Utf8(["one", "two"].iter().map(Some).collect()),
                ]
                .into_iter()
                .collect();
            }
            .boxed(),
        };
        executor.execute().next().await.unwrap().unwrap();

        let actual = std::fs::read_to_string(file.path()).unwrap();
        let expected = "1,1.5,one\n2,2.5,two\n";
        assert_eq!(actual, expected);
    }
}
