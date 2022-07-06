//! Holds the `Processor`

use crate::client::Client;
use crate::reader::RawTransactionStream;
use crate::transaction::RawTransaction;
use async_stream::stream;
use futures_util::future::join_all;
use std::collections::HashMap;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

/// An error type for the transaction module.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ProcessorError {
    /// Triggered if the channel to send transactions is closed or fails
    #[error("Failed to send transaction to client: {0}")]
    SendError(#[from] SendError<RawTransaction>),
    /// Triggered if the client is not present
    #[error("Failed to find client for transaction")]
    ClientError,
}

/// The The `Processor` takes an input stream of `Transaction`s and sends them to their respective `Client`s.
#[non_exhaustive]
#[derive(Debug, Default)]
pub struct Processor {
    /// The `Sender` for each of the client streams.
    client_senders: HashMap<u16, Sender<RawTransaction>>,
    /// The handle for the stream sender.
    client_handles: HashMap<u16, JoinHandle<Client>>,
}

impl Processor {
    /// Processes a stream of `RawTransaction`s and sends them to their respective `Client`s.
    /// # Errors
    /// Returns an error if the `Sender` for the `Client` fails to send the `RawTransaction`.
    /// Returns an error if the `Client` cannot be found
    #[inline]
    pub async fn process_transactions(
        &mut self,
        mut transactions: RawTransactionStream,
    ) -> Result<Vec<Client>, ProcessorError> {
        while let Some(transaction) = transactions.next().await {
            if let std::collections::hash_map::Entry::Vacant(e) =
                self.client_handles.entry(transaction.client_id)
            {
                let mut client = Client::new(transaction.client_id);
                let (tx, mut rx) = tokio::sync::mpsc::channel(10);
                self.client_senders.insert(transaction.client_id, tx);

                let stream = RawTransactionStream::new(stream! {
                    while let Some(t) = rx.recv().await {
                        yield t;
                    }
                });

                let handle = tokio::spawn(async move {
                    client.process_activity(stream).await;
                    client
                });

                e.insert(handle);
                self.client_senders
                    .get(&transaction.client_id)
                    .ok_or(ProcessorError::ClientError)?
                    .send(transaction)
                    .await?;
            } else if let Some(sender) = self.client_senders.get_mut(&transaction.client_id) {
                sender.send(transaction).await?;
            } else {
                return Err(ProcessorError::ClientError);
            }
        }

        self.client_senders.clear();

        Ok(self.join_clients().await)
    }

    /// Joins the `Client` handles into a vector of the finished `Client`s.
    #[inline]
    async fn join_clients(&mut self) -> Vec<Client> {
        join_all(
            self.client_handles
                .drain()
                .map(|(_, handle)| async { handle.await }),
        )
        .await
        .into_iter()
        .flatten()
        .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::RawTransactionVariant;
    use anyhow::Result;

    #[tokio::test]
    async fn it_processes_transactions_for_a_single_client() -> Result<()> {
        let mut processor = Processor::default();
        let raw_transactions = RawTransactionStream::new(stream! {
            yield RawTransaction {
                client_id: 1,
                tx_id: 1,
                amount: Some(1000.0_f64),
                variant: RawTransactionVariant::Deposit
            };
            yield RawTransaction {
                client_id: 1,
                tx_id: 2,
                amount: Some(500.0_f64),
                variant: RawTransactionVariant::Withdrawal
            };
        });
        let clients = processor.process_transactions(raw_transactions).await?;
        assert_eq!(clients.len(), 1);
        let client = &clients[0];
        assert_eq!(client.id, 1);
        assert!((client.available_balance - 500.0).abs() < f64::EPSILON);
        assert!((client.total_balance - 500.0).abs() < f64::EPSILON);
        assert!(client.held_balance.abs() < f64::EPSILON);
        assert!(!client.locked);
        Ok(())
    }

    #[tokio::test]
    async fn it_processes_transactions_for_multiple_clients() -> Result<()> {
        let mut processor = Processor::default();

        let raw_transactions = RawTransactionStream::new(stream! {
            yield RawTransaction {
                client_id: 1,
                tx_id: 1,
                amount: Some(1000.0_f64),
                variant: RawTransactionVariant::Deposit
            };
            yield RawTransaction {
                client_id: 1,
                tx_id: 2,
                amount: Some(500.0_f64),
                variant: RawTransactionVariant::Withdrawal
            };
            yield RawTransaction {
                client_id: 2,
                tx_id: 3,
                amount: Some(500.0_f64),
                variant: RawTransactionVariant::Deposit
            };
        });

        let mut clients = processor.process_transactions(raw_transactions).await?;
        clients.sort_by_key(|c| c.id);

        assert_eq!(clients.len(), 2);
        let client1 = &clients[0];
        assert_eq!(client1.id, 1);
        assert!((client1.available_balance - 500.0).abs() < f64::EPSILON);
        assert!((client1.total_balance - 500.0).abs() < f64::EPSILON);
        assert!(client1.held_balance.abs() < f64::EPSILON);
        assert!(!client1.locked);
        let client2 = &clients[1];
        assert_eq!(client2.id, 2);
        assert!((client2.available_balance - 500.0).abs() < f64::EPSILON);
        assert!((client2.total_balance - 500.0).abs() < f64::EPSILON);
        assert!(client2.held_balance.abs() < f64::EPSILON);
        assert!(!client2.locked);

        Ok(())
    }
}
