//! The purpose of this module is to read chunks of data from a file (or any other source) and parse
//! them into `Transaction`s.  The parsed `Transaction`s should be made available as a stream for other modules
//! to consume.

use crate::transaction::RawTransaction;
use async_stream::stream;
use futures_core::stream::Stream;
use futures_util::pin_mut;
use std::io;
use std::path::Path;
use std::pin::Pin;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_stream::wrappers::LinesStream;

/// Reads bytes from a file into a stream
/// # Errors
/// Returns an error if the file cannot be read
#[inline]
pub async fn read_from_file(path: &Path) -> Result<impl Stream<Item = String>, io::Error> {
    let file = tokio::fs::File::open(path).await?;
    let reader = BufReader::new(file).lines();
    let string_result_stream = LinesStream::new(reader);

    Ok(stream! {
        for await result_data in string_result_stream {
            if let Ok(data) = result_data {
                yield data;
            } else {
                // TODO: Log to stderr
            }
        }
    })
}

/// Reads a chunk of data from an input stream and parses it into a stream of `Transaction`s.
#[inline]
pub async fn process_raw_data(
    source: impl Stream<Item = String>,
) -> impl Stream<Item = RawTransaction> {
    stream! {
        for await data in source {
            let mut rdr = csv::ReaderBuilder::new()
                .has_headers(false)
                .from_reader(data.as_bytes());
            let mut iter = rdr.deserialize::<RawTransaction>();
            if let Some(transaction) = iter.next() {
                if let Ok(t) = transaction {
                    yield t;
                }
            }
        }
    }
}

/// Reads a chunk of data from an input file and parses it into a stream of `Transaction`s.
/// # Errors
/// Returns an error if the file cannot be read
#[inline]
pub async fn read_transactions_from_file(
    path: &Path,
) -> Result<impl Stream<Item = RawTransaction>, io::Error> {
    let raw_stream = read_from_file(path).await?;
    Ok(process_raw_data(raw_stream).await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::RawTransactionVariant;
    use anyhow::Result;
    use futures_util::pin_mut;
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn it_reads_from_file() -> Result<()> {
        let path = Path::new("test_data/test_data.csv");
        let stream = read_transactions_from_file(path).await?;
        pin_mut!(stream);
        let mut count = 0_u32;
        while let Some(_transaction) = stream.next().await {
            count += 1_u32;
        }
        assert_eq!(count, 10_u32);
        Ok(())
    }

    #[tokio::test]
    async fn it_reads_from_file_with_garbage_lines() -> Result<()> {
        let path = Path::new("test_data/test_data_garbage.csv");
        let stream = read_transactions_from_file(path).await?;
        pin_mut!(stream);
        let mut count = 0_u32;
        while let Some(_transaction) = stream.next().await {
            count += 1_u32;
        }
        assert_eq!(count, 3_u32);
        Ok(())
    }

    #[tokio::test]
    async fn it_reads_from_file_with_deposits_withdrawals_disputes_and_resolves() -> Result<()> {
        let path = Path::new("test_data/test_data_run1.csv");
        let stream = read_transactions_from_file(path).await?;
        pin_mut!(stream);
        let mut count = 0_usize;
        while let Some(transaction) = stream.next().await {
            match count {
                0 => {
                    assert_eq!(
                        transaction,
                        RawTransaction {
                            client_id: 1,
                            tx_id: 1,
                            variant: RawTransactionVariant::Deposit,
                            amount: Some(1000_f64)
                        }
                    );
                }
                1 => {
                    assert_eq!(
                        transaction,
                        RawTransaction {
                            client_id: 1,
                            tx_id: 2,
                            variant: RawTransactionVariant::Withdrawal,
                            amount: Some(500_f64)
                        }
                    );
                }
                2 => {
                    assert_eq!(
                        transaction,
                        RawTransaction {
                            client_id: 1,
                            tx_id: 1,
                            variant: RawTransactionVariant::Dispute,
                            amount: None
                        }
                    );
                }
                3 => {
                    assert_eq!(
                        transaction,
                        RawTransaction {
                            client_id: 1,
                            tx_id: 2,
                            variant: RawTransactionVariant::Dispute,
                            amount: None
                        }
                    );
                }
                4 => {
                    assert_eq!(
                        transaction,
                        RawTransaction {
                            client_id: 1,
                            tx_id: 1,
                            variant: RawTransactionVariant::Resolve,
                            amount: None
                        }
                    );
                }
                5 => {
                    assert_eq!(
                        transaction,
                        RawTransaction {
                            client_id: 1,
                            tx_id: 2,
                            variant: RawTransactionVariant::Resolve,
                            amount: None
                        }
                    );
                }
                _ => panic!("Too many transactions"),
            }
            count += 1;
        }

        Ok(())
    }
}
