//! This module holds the logic regarding Client accounts, such as their balances, held balances, ids,
//! and whether or not they are locked.

use crate::transaction::{
    truncate_to_decimal_places, Chargeback, Deposit, Dispute, RawTransaction,
    RawTransactionVariant, Resolve, Transaction, Withdrawal,
};
use futures_core::stream::Stream;
use futures_util::pin_mut;
use serde::{Serialize, Serializer};
use std::collections::{HashMap, VecDeque};
use tokio_stream::StreamExt;

/// The window size of transactions that can be disputed
const WINDOW_SIZE: usize = 1000;

/// Serializer for client balances
fn truncate_to_4_decimals<S: Serializer>(value: &f64, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_f64(truncate_to_decimal_places(*value, 4))
}

/// A `Client` represents an account that can hold funds.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct Client {
    /// The id of the client.
    #[serde(rename = "client")]
    pub id: u16,
    /// The balance of the client.
    #[serde(rename = "available")]
    #[serde(serialize_with = "truncate_to_4_decimals")]
    pub available_balance: f64,
    /// The balance of the client that is held.
    #[serde(rename = "held")]
    #[serde(serialize_with = "truncate_to_4_decimals")]
    pub held_balance: f64,
    /// The total balance of the client.  This is the sum of the `available_balance` and `held_balance`.
    #[serde(rename = "total")]
    #[serde(serialize_with = "truncate_to_4_decimals")]
    pub total_balance: f64,
    /// Whether or not the client is locked.
    pub locked: bool,
    /// The processed transactions of the client.
    #[serde(skip)]
    processed_transactions: HashMap<u32, Transaction>,
    /// The window of transactions that can be disputed.  If a transaction is not disputed, it is
    /// removed from this window.  If a transaction is disputed but not resolved, the dispute will
    /// by default be resolved and then removed.
    #[serde(skip)]
    dispute_window: VecDeque<u32>,
}

impl Client {
    /// Creates a new `Client`
    #[inline]
    #[must_use]
    pub fn new(id: u16) -> Self {
        Self {
            id,
            available_balance: 0.0,
            held_balance: 0.0,
            total_balance: 0.0,
            locked: false,
            processed_transactions: HashMap::new(),
            dispute_window: VecDeque::with_capacity(WINDOW_SIZE),
        }
    }

    /// Processes all the activity of the client, and computes the final balances and status of the client.
    #[inline]
    pub async fn process_activity(&mut self, activity_stream: impl Stream<Item = RawTransaction>) {
        pin_mut!(activity_stream);
        let mut pending_total_balance = self.total_balance;
        let mut pending_held_balance = self.held_balance;
        let mut pending_available_balance = self.available_balance;
        while let Some(transaction) = activity_stream.next().await {
            if self.locked {
                break;
            }
            self.process_transaction(
                &mut pending_total_balance,
                &mut pending_held_balance,
                &mut pending_available_balance,
                transaction,
            );
        }
        self.available_balance = pending_available_balance;
        self.held_balance = pending_held_balance;
        self.total_balance = pending_total_balance;
        self.dispute_window.clear();
        self.processed_transactions.clear();
    }

    /// Processes a transaction and updates the client's pending balances.
    fn process_transaction(
        &mut self,
        pending_total_balance: &mut f64,
        pending_held_balance: &mut f64,
        pending_available_balance: &mut f64,
        transaction: RawTransaction,
    ) {
        if self.dispute_window.len() >= WINDOW_SIZE {
            self.finalize_transaction(pending_held_balance, pending_available_balance);
        }
        match transaction.variant {
            RawTransactionVariant::Deposit => {
                if let Ok(deposit) = transaction.try_into() {
                    self.process_deposit(pending_total_balance, pending_available_balance, deposit);
                } else {
                    eprintln!("Failed to parse deposit transaction");
                }
            }
            RawTransactionVariant::Withdrawal => {
                if let Ok(withdrawal) = transaction.try_into() {
                    self.process_withdrawal(
                        pending_total_balance,
                        pending_available_balance,
                        withdrawal,
                    );
                } else {
                    eprintln!("Failed to parse withdrawal transaction");
                }
            }
            RawTransactionVariant::Dispute => {
                if let Ok(dispute) = transaction.try_into() {
                    self.process_dispute(pending_held_balance, pending_available_balance, &dispute);
                } else {
                    eprintln!("Failed to parse dispute transaction");
                }
            }
            RawTransactionVariant::Resolve => {
                if let Ok(resolve) = transaction.try_into() {
                    self.process_resolve(
                        pending_held_balance,
                        pending_available_balance,
                        &resolve,
                        None,
                    );
                } else {
                    eprintln!("Failed to parse resolve transaction");
                }
            }
            RawTransactionVariant::Chargeback => {
                if let Ok(chargeback) = transaction.try_into() {
                    self.process_chargeback(
                        pending_held_balance,
                        pending_total_balance,
                        &chargeback,
                    );
                    if let Some(window_start) = self
                        .dispute_window
                        .iter()
                        .position(|&id| id == chargeback.tx_id)
                    {
                        let ids_to_check = self
                            .dispute_window
                            .clone()
                            .into_iter()
                            .skip(window_start)
                            .rev()
                            .collect::<Vec<_>>();
                        if *pending_total_balance < 0_f64 {
                            // Reverse withdrawals until the total balance is positive.
                            for id in ids_to_check {
                                if *pending_total_balance >= 0_f64 {
                                    break;
                                }
                                self.reverse_withdrawal(
                                    pending_available_balance,
                                    pending_total_balance,
                                    id,
                                );
                            }
                        }
                    }
                } else {
                    eprintln!("Failed to parse chargeback transaction");
                }
            }
        }
    }

    /// Finalizes a transaction by removing it from the dispute window.
    fn finalize_transaction(
        &mut self,
        pending_held_balance: &mut f64,
        pending_available_balance: &mut f64,
    ) {
        if let Some(id) = self.dispute_window.pop_front() {
            if let Some(mut old_tx) = self.processed_transactions.remove(&id) {
                match old_tx {
                    Transaction::Deposit(deposit) => {
                        if deposit.disputed && !deposit.resolved {
                            self.process_resolve(
                                pending_held_balance,
                                pending_available_balance,
                                &Resolve {
                                    client_id: deposit.client_id,
                                    tx_id: deposit.tx_id,
                                },
                                Some(&mut old_tx),
                            );
                        }
                    }
                    Transaction::Withdrawal(withdrawal) => {
                        if withdrawal.disputed && !withdrawal.resolved {
                            self.process_resolve(
                                pending_held_balance,
                                pending_available_balance,
                                &Resolve {
                                    client_id: withdrawal.client_id,
                                    tx_id: withdrawal.tx_id,
                                },
                                Some(&mut old_tx),
                            );
                        }
                    }
                    Transaction::Dispute(_)
                    | Transaction::Resolve(_)
                    | Transaction::Chargeback(_) => {
                        eprintln!("Failed to finalize transaction");
                    }
                }
            }
        }
    }

    /// Processes a deposit transaction.
    fn process_deposit(
        &mut self,
        pending_total_balance: &mut f64,
        pending_available_balance: &mut f64,
        deposit: Deposit,
    ) {
        if deposit.client_id != self.id {
            eprintln!("Received deposit from wrong client: {}", deposit.client_id);
            return;
        }
        *pending_total_balance += deposit.amount;
        *pending_available_balance += deposit.amount;
        self.dispute_window.push_back(deposit.tx_id);
        self.processed_transactions
            .insert(deposit.tx_id, Transaction::Deposit(deposit));
    }

    /// Processes a withdrawal transaction.
    fn process_withdrawal(
        &mut self,
        pending_total_balance: &mut f64,
        pending_available_balance: &mut f64,
        mut withdrawal: Withdrawal,
    ) {
        if withdrawal.client_id != self.id {
            eprintln!(
                "Received withdrawal from wrong client: {}",
                withdrawal.client_id
            );
            return;
        }
        if *pending_total_balance < withdrawal.amount {
            // No resolution of disputed transactions will enable this withdrawal to be processed.
            eprintln!("Insufficient funds to process withdrawal");
            withdrawal.failed = true;
            self.processed_transactions
                .insert(withdrawal.tx_id, Transaction::Withdrawal(withdrawal));
            return;
        }
        *pending_total_balance -= withdrawal.amount;
        *pending_available_balance -= withdrawal.amount;
        self.dispute_window.push_back(withdrawal.tx_id);
        self.processed_transactions
            .insert(withdrawal.tx_id, Transaction::Withdrawal(withdrawal));
    }

    /// Process a dispute
    fn process_dispute(
        &mut self,
        pending_held_balance: &mut f64,
        pending_available_balance: &mut f64,
        dispute: &Dispute,
    ) {
        if let Some(tx) = self.processed_transactions.get_mut(&dispute.tx_id) {
            match tx {
                Transaction::Deposit(deposit) => {
                    if deposit.disputed
                        || deposit.resolved
                        || dispute.client_id != deposit.client_id
                    {
                        // Transaction has already been disputed
                        eprintln!("Transaction has already been disputed: {}", dispute.tx_id);
                        return;
                    }
                    deposit.disputed = true;
                    *pending_held_balance += deposit.amount;
                    *pending_available_balance -= deposit.amount;
                }
                Transaction::Withdrawal(withdrawal) => {
                    if withdrawal.disputed
                        || withdrawal.resolved
                        || dispute.client_id != withdrawal.client_id
                    {
                        // Transaction has already been disputed
                        eprintln!("Transaction has already been disputed: {}", dispute.tx_id);
                        return;
                    }
                    withdrawal.disputed = true;
                    if !withdrawal.failed {
                        *pending_held_balance -= withdrawal.amount;
                        *pending_available_balance += withdrawal.amount;
                    }
                }
                Transaction::Dispute(_) | Transaction::Resolve(_) | Transaction::Chargeback(_) => {
                    // The transaction referenced is not a deposit or withdrawal
                    eprintln!(
                        "Transaction is not a deposit or withdrawal: {}",
                        dispute.tx_id
                    );
                }
            }
        }
    }

    /// Processes a `Resolve` transaction
    fn process_resolve(
        &mut self,
        pending_held_balance: &mut f64,
        pending_available_balance: &mut f64,
        resolve: &Resolve,
        tx: Option<&mut Transaction>,
    ) {
        let transaction = if tx.is_some() {
            tx
        } else {
            self.processed_transactions.get_mut(&resolve.tx_id)
        };
        if let Some(t) = transaction {
            match t {
                Transaction::Deposit(deposit) => {
                    if !deposit.disputed
                        || deposit.resolved
                        || resolve.client_id != deposit.client_id
                    {
                        // Transaction has not been disputed or has already been resolved
                        eprintln!(
                            "Transaction has not been disputed or has already been resolved: {}",
                            resolve.tx_id
                        );
                        return;
                    }
                    deposit.resolved = true;
                    *pending_held_balance -= deposit.amount;
                    *pending_available_balance += deposit.amount;
                }
                Transaction::Withdrawal(withdrawal) => {
                    if !withdrawal.disputed
                        || withdrawal.resolved
                        || resolve.client_id != withdrawal.client_id
                    {
                        // Transaction has not been disputed or has already been resolved
                        eprintln!(
                            "Transaction has not been disputed or has already been resolved: {}",
                            resolve.tx_id
                        );
                        return;
                    }
                    withdrawal.resolved = true;
                    if !withdrawal.failed {
                        *pending_held_balance += withdrawal.amount;
                        *pending_available_balance -= withdrawal.amount;
                    }
                }
                Transaction::Dispute(_) | Transaction::Resolve(_) | Transaction::Chargeback(_) => {
                    // The transaction is not a deposit or a withdrawal
                    eprintln!(
                        "Transaction is not a deposit or withdrawal: {}",
                        resolve.tx_id
                    );
                }
            }
        }
    }

    /// Processes a `Chargeback` transaction
    fn process_chargeback(
        &mut self,
        pending_held_balance: &mut f64,
        pending_total_balance: &mut f64,
        chargeback: &Chargeback,
    ) {
        if let Some(tx) = self.processed_transactions.get_mut(&chargeback.tx_id) {
            match tx {
                Transaction::Deposit(deposit) => {
                    if !deposit.disputed
                        || deposit.resolved
                        || chargeback.client_id != deposit.client_id
                    {
                        // Transaction has not been disputed or has already been resolved
                        eprintln!(
                            "Transaction has not been disputed or has already been resolved: {}",
                            chargeback.tx_id
                        );
                        return;
                    }
                    deposit.resolved = true;
                    *pending_held_balance -= deposit.amount;
                    *pending_total_balance -= deposit.amount;
                    self.locked = true;
                }
                Transaction::Withdrawal(withdrawal) => {
                    if !withdrawal.disputed
                        || withdrawal.resolved
                        || chargeback.client_id != withdrawal.client_id
                    {
                        // Transaction has not been disputed or has already been resolved
                        eprintln!(
                            "Transaction has not been disputed or has already been resolved: {}",
                            chargeback.tx_id
                        );
                        return;
                    }
                    withdrawal.resolved = true;
                    if !withdrawal.failed {
                        *pending_held_balance += withdrawal.amount;
                        *pending_total_balance += withdrawal.amount;
                    }

                    self.locked = true;
                }
                Transaction::Dispute(_) | Transaction::Resolve(_) | Transaction::Chargeback(_) => {
                    // The transaction is not a deposit or a withdrawal
                    eprintln!(
                        "Transaction is not a deposit or withdrawal: {}",
                        chargeback.tx_id
                    );
                }
            }
        }
    }

    /// Reverses a withdrawal transaction, marking it as failed.
    fn reverse_withdrawal(
        &mut self,
        pending_available_balance: &mut f64,
        pending_total_balance: &mut f64,
        id: u32,
    ) {
        if let Some(Transaction::Withdrawal(withdrawal)) = self.processed_transactions.get_mut(&id)
        {
            if !withdrawal.failed && !withdrawal.disputed || withdrawal.resolved {
                *pending_available_balance += withdrawal.amount;
                *pending_total_balance += withdrawal.amount;
                withdrawal.failed = true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use async_stream::stream;

    #[tokio::test]
    async fn it_processes_deposits() -> Result<()> {
        let stream = stream! {
            yield RawTransaction {
                tx_id: 1,
                client_id: 1,
                amount: Some(1_000.0_f64),
                variant: RawTransactionVariant::Deposit,
            };
            yield RawTransaction {
                tx_id: 2,
                client_id: 1,
                amount: Some(2_000.0_f64),
                variant: RawTransactionVariant::Deposit,
            };
        };

        let mut client = Client::new(1);

        client.process_activity(stream).await;

        assert!((client.available_balance - 3_000.0_f64).abs() < f64::EPSILON);
        assert!((client.total_balance - 3_000.0_f64).abs() < f64::EPSILON);
        assert!((client.available_balance - client.total_balance).abs() < f64::EPSILON);
        assert!(client.held_balance.abs() < f64::EPSILON);
        assert!(!client.locked);

        Ok(())
    }

    #[tokio::test]
    async fn it_fails_to_process_deposits_with_different_client_ids() -> Result<()> {
        let stream = stream! {
            yield RawTransaction {
                tx_id: 1,
                client_id: 1,
                amount: Some(1_000.0_f64),
                variant: RawTransactionVariant::Deposit,
            };
            yield RawTransaction {
                tx_id: 2,
                client_id: 2,
                amount: Some(2_000.0_f64),
                variant: RawTransactionVariant::Deposit,
            };
        };

        let mut client = Client::new(1);

        client.process_activity(stream).await;

        assert!((client.available_balance - 1_000.0_f64).abs() < f64::EPSILON);
        assert!((client.available_balance - client.total_balance).abs() < f64::EPSILON);
        assert!(!client.locked);

        Ok(())
    }

    #[tokio::test]
    async fn it_processes_withdrawals() -> Result<()> {
        let stream = stream! {
            yield RawTransaction {
                tx_id: 1,
                client_id: 1,
                amount: Some(1_500.0_f64),
                variant: RawTransactionVariant::Deposit,
            };
            yield RawTransaction {
                tx_id: 2,
                client_id: 1,
                amount: Some(1_000.0_f64),
                variant: RawTransactionVariant::Withdrawal,
            };
        };

        let mut client = Client::new(1);

        client.process_activity(stream).await;

        assert!((client.available_balance - 500.0).abs() < f64::EPSILON);
        assert!((client.total_balance - 500.0).abs() < f64::EPSILON);
        assert!((client.available_balance - client.total_balance).abs() < f64::EPSILON);
        assert!(client.held_balance.abs() < f64::EPSILON);
        assert!(!client.locked);

        Ok(())
    }

    #[tokio::test]
    async fn it_fails_to_process_withdrawals_with_different_client_ids() -> Result<()> {
        let stream = stream! {
            yield RawTransaction {
                tx_id: 1,
                client_id: 1,
                amount: Some(1_500.0_f64),
                variant: RawTransactionVariant::Deposit,
            };
            yield RawTransaction {
                tx_id: 2,
                client_id: 2,
                amount: Some(1_000.0_f64),
                variant: RawTransactionVariant::Withdrawal,
            };
        };

        let mut client = Client::new(1);

        client.process_activity(stream).await;

        assert!((client.available_balance - 1_500.0_f64).abs() < f64::EPSILON);
        assert!((client.available_balance - client.total_balance).abs() < f64::EPSILON);
        assert!(client.held_balance.abs() < f64::EPSILON);
        assert!(!client.locked);

        Ok(())
    }

    #[tokio::test]
    async fn it_handles_disputes_of_deposits() -> Result<()> {
        let stream = stream! {
        yield RawTransaction {
            tx_id: 1,
            client_id: 1,
            amount: Some(1_500.0_f64),
            variant: RawTransactionVariant::Deposit,
            };
        yield RawTransaction {
            tx_id: 1,
            client_id: 1,
            amount: None,
            variant: RawTransactionVariant::Dispute,
            };
        };

        let mut client = Client::new(1);

        client.process_activity(stream).await;

        assert!(client.available_balance.abs() < f64::EPSILON);
        assert!((client.total_balance - client.held_balance).abs() < f64::EPSILON);
        assert!((client.held_balance - 1500.0).abs() < f64::EPSILON);
        assert!(!client.locked);

        Ok(())
    }

    #[tokio::test]
    async fn it_handles_disputes_of_withdrawals() -> Result<()> {
        let stream = stream! {
        yield RawTransaction {
            tx_id: 1,
            client_id: 1,
            amount: Some(1_500.0_f64),
            variant: RawTransactionVariant::Deposit,
            };
        yield RawTransaction {
            tx_id: 2,
            client_id: 1,
            amount: Some(1_000.0_f64),
            variant: RawTransactionVariant::Withdrawal,
            };
        yield RawTransaction {
            tx_id: 2,
            client_id: 1,
            amount: None,
            variant: RawTransactionVariant::Dispute,
            };
        };

        let mut client = Client::new(1);

        client.process_activity(stream).await;

        assert!((client.available_balance - 1500.0).abs() < f64::EPSILON);
        assert!((client.total_balance - 500.0).abs() < f64::EPSILON);
        assert!((client.held_balance + 1000.0).abs() < f64::EPSILON);
        assert!(!client.locked);

        Ok(())
    }

    #[tokio::test]
    async fn it_fails_withdrawals_with_insufficient_balance() -> Result<()> {
        let stream = stream! {
            yield RawTransaction {
                tx_id: 1,
                client_id: 1,
                amount: Some(1_500.0_f64),
                variant: RawTransactionVariant::Deposit,
            };
            yield RawTransaction {
                tx_id: 2,
                client_id: 1,
                amount: Some(2_000.0_f64),
                variant: RawTransactionVariant::Withdrawal,
            };
        };

        let mut client = Client::new(1);

        client.process_activity(stream).await;

        assert!((client.available_balance - 1500.0).abs() < f64::EPSILON);
        assert!((client.total_balance - 1500.0).abs() < f64::EPSILON);
        assert!((client.available_balance - client.total_balance).abs() < f64::EPSILON);
        assert!(client.held_balance.abs() < f64::EPSILON);
        assert!(!client.locked);

        Ok(())
    }

    #[tokio::test]
    async fn it_handles_disputes_of_deposits_and_withdrawals() -> Result<()> {
        let stream = stream! {
        yield RawTransaction {
            tx_id: 1,
            client_id: 1,
            amount: Some(1_500.0_f64),
            variant: RawTransactionVariant::Deposit,
            };
        yield RawTransaction {
            tx_id: 2,
            client_id: 1,
            amount: Some(1_000.0_f64),
            variant: RawTransactionVariant::Withdrawal,
            };
        yield RawTransaction {
            tx_id: 1,
            client_id: 1,
            amount: None,
            variant: RawTransactionVariant::Dispute,
            };
        yield RawTransaction {
            tx_id: 2,
            client_id: 1,
            amount: None,
            variant: RawTransactionVariant::Dispute,
            };
        };

        let mut client = Client::new(1);

        client.process_activity(stream).await;

        assert!(client.available_balance.abs() < f64::EPSILON);
        assert!((client.total_balance - client.held_balance).abs() < f64::EPSILON);
        assert!((client.held_balance - 500.0).abs() < f64::EPSILON);
        assert!(!client.locked);

        Ok(())
    }

    #[tokio::test]
    async fn it_rejects_disputes_of_deposits_with_different_client_ids() -> Result<()> {
        let stream = stream! {
        yield RawTransaction {
            tx_id: 1,
            client_id: 1,
            amount: Some(1_500.0_f64),
            variant: RawTransactionVariant::Deposit,
            };
        yield RawTransaction {
            tx_id: 1,
            client_id: 2,
            amount: None,
            variant: RawTransactionVariant::Dispute,
            };
        };

        let mut client = Client::new(1);

        client.process_activity(stream).await;

        assert!((client.available_balance - 1500.0).abs() < f64::EPSILON);
        assert!((client.total_balance - client.available_balance).abs() < f64::EPSILON);
        assert!(client.held_balance.abs() < f64::EPSILON);
        assert!(!client.locked);

        Ok(())
    }

    #[tokio::test]
    async fn it_rejects_disputes_of_withdrawals_with_different_client_ids() -> Result<()> {
        let stream = stream! {
        yield RawTransaction {
            tx_id: 1,
            client_id: 1,
            amount: Some(1_500.0_f64),
            variant: RawTransactionVariant::Deposit,
            };
        yield RawTransaction {
            tx_id: 2,
            client_id: 1,
            amount: Some(1_000.0_f64),
            variant: RawTransactionVariant::Withdrawal,
            };
        yield RawTransaction {
            tx_id: 2,
            client_id: 2,
            amount: None,
            variant: RawTransactionVariant::Dispute,
            };
        };

        let mut client = Client::new(1);

        client.process_activity(stream).await;

        assert!((client.available_balance - 500.0).abs() < f64::EPSILON);
        assert!((client.total_balance - client.available_balance).abs() < f64::EPSILON);
        assert!(client.held_balance.abs() < f64::EPSILON);
        assert!(!client.locked);

        Ok(())
    }

    #[tokio::test]
    async fn it_handles_resolves_of_deposit_disputes() -> Result<()> {
        let stream = stream! {
        yield RawTransaction {
            tx_id: 1,
            client_id: 1,
            amount: Some(1_500.0_f64),
            variant: RawTransactionVariant::Deposit,
            };
        yield RawTransaction {
            tx_id: 1,
            client_id: 1,
            amount: None,
            variant: RawTransactionVariant::Dispute,
            };
        yield RawTransaction {
            tx_id: 1,
            client_id: 1,
            amount: None,
            variant: RawTransactionVariant::Resolve,
            };
        };

        let mut client = Client::new(1);

        client.process_activity(stream).await;

        assert!((client.available_balance - 1500.0).abs() < f64::EPSILON);
        assert!((client.total_balance - client.available_balance).abs() < f64::EPSILON);
        assert!(client.held_balance.abs() < f64::EPSILON);
        assert!(!client.locked);

        Ok(())
    }

    #[tokio::test]
    async fn it_handles_resolves_of_withdrawal_disputes() -> Result<()> {
        let stream = stream! {
        yield RawTransaction {
            tx_id: 1,
            client_id: 1,
            amount: Some(1_500.0_f64),
            variant: RawTransactionVariant::Deposit,
            };
        yield RawTransaction {
            tx_id: 2,
            client_id: 1,
            amount: Some(1_000.0_f64),
            variant: RawTransactionVariant::Withdrawal,
            };
        yield RawTransaction {
            tx_id: 2,
            client_id: 1,
            amount: None,
            variant: RawTransactionVariant::Dispute,
            };
        yield RawTransaction {
            tx_id: 2,
            client_id: 1,
            amount: None,
            variant: RawTransactionVariant::Resolve,
            };
        };

        let mut client = Client::new(1);

        client.process_activity(stream).await;

        assert!((client.available_balance - 500.0).abs() < f64::EPSILON);
        assert!((client.total_balance - client.available_balance).abs() < f64::EPSILON);
        assert!(client.held_balance.abs() < f64::EPSILON);
        assert!(!client.locked);

        Ok(())
    }

    #[tokio::test]
    async fn it_rejects_resolves_of_deposit_disputes_with_different_client_ids() -> Result<()> {
        let stream = stream! {
        yield RawTransaction {
            tx_id: 1,
            client_id: 1,
            amount: Some(1_500.0_f64),
            variant: RawTransactionVariant::Deposit,
            };
        yield RawTransaction {
            tx_id: 1,
            client_id: 1,
            amount: None,
            variant: RawTransactionVariant::Dispute,
            };
        yield RawTransaction {
            tx_id: 1,
            client_id: 2,
            amount: None,
            variant: RawTransactionVariant::Resolve,
            };
        };

        let mut client = Client::new(1);

        client.process_activity(stream).await;

        assert!(client.available_balance.abs() < f64::EPSILON);
        assert!((client.total_balance - client.held_balance).abs() < f64::EPSILON);
        assert!((client.held_balance - 1500.0).abs() < f64::EPSILON);
        assert!(!client.locked);

        Ok(())
    }

    #[tokio::test]
    async fn it_rejects_resolves_of_withdrawal_disputes_with_different_client_ids() -> Result<()> {
        let stream = stream! {
        yield RawTransaction {
            tx_id: 1,
            client_id: 1,
            amount: Some(1_500.0_f64),
            variant: RawTransactionVariant::Deposit,
            };
        yield RawTransaction {
            tx_id: 2,
            client_id: 1,
            amount: Some(1_000.0_f64),
            variant: RawTransactionVariant::Withdrawal,
            };
        yield RawTransaction {
            tx_id: 2,
            client_id: 1,
            amount: None,
            variant: RawTransactionVariant::Dispute,
            };
        yield RawTransaction {
            tx_id: 2,
            client_id: 2,
            amount: None,
            variant: RawTransactionVariant::Resolve,
            };
        };

        let mut client = Client::new(1);

        client.process_activity(stream).await;

        assert!((client.available_balance - 1500.0).abs() < f64::EPSILON);
        assert!((client.total_balance - 500.0).abs() < f64::EPSILON);
        assert!((client.held_balance + 1000.0).abs() < f64::EPSILON);
        assert!(!client.locked);

        Ok(())
    }

    #[tokio::test]
    async fn it_handles_chargebacks_of_deposit_disputes() -> Result<()> {
        let stream = stream! {
        yield RawTransaction {
            tx_id: 1,
            client_id: 1,
            amount: Some(1_500.0_f64),
            variant: RawTransactionVariant::Deposit,
            };
        yield RawTransaction {
            tx_id: 1,
            client_id: 1,
            amount: None,
            variant: RawTransactionVariant::Dispute,
            };
        yield RawTransaction {
            tx_id: 1,
            client_id: 1,
            amount: None,
            variant: RawTransactionVariant::Chargeback,
            };
        };

        let mut client = Client::new(1);

        client.process_activity(stream).await;

        println!("{:?}", client);
        assert!(client.available_balance.abs() < f64::EPSILON);
        assert!(client.held_balance.abs() < f64::EPSILON);
        assert!(client.total_balance.abs() < f64::EPSILON);
        assert!(client.locked);

        Ok(())
    }

    #[tokio::test]
    async fn it_handles_chargebacks_of_withdrawal_disputes() -> Result<()> {
        let stream = stream! {
        yield RawTransaction {
            tx_id: 1,
            client_id: 1,
            amount: Some(1_500.0_f64),
            variant: RawTransactionVariant::Deposit,
            };
        yield RawTransaction {
            tx_id: 2,
            client_id: 1,
            amount: Some(1_000.0_f64),
            variant: RawTransactionVariant::Withdrawal,
            };
        yield RawTransaction {
            tx_id: 2,
            client_id: 1,
            amount: None,
            variant: RawTransactionVariant::Dispute,
            };
        yield RawTransaction {
            tx_id: 2,
            client_id: 1,
            amount: None,
            variant: RawTransactionVariant::Chargeback,
            };
        };

        let mut client = Client::new(1);

        client.process_activity(stream).await;

        assert!((client.available_balance - 1500.0).abs() < f64::EPSILON);
        assert!((client.total_balance - 1500.0).abs() < f64::EPSILON);
        assert!(client.held_balance.abs() < f64::EPSILON);
        assert!(client.locked);

        Ok(())
    }

    #[tokio::test]
    async fn it_reverses_withdrawals_after_a_chargeback() -> Result<()> {
        let stream = stream! {
        yield RawTransaction {
            tx_id: 1,
            client_id: 1,
            amount: Some(1_500.0_f64),
            variant: RawTransactionVariant::Deposit,
            };
        yield RawTransaction {
            tx_id: 2,
            client_id: 1,
            amount: Some(1_000.0_f64),
            variant: RawTransactionVariant::Withdrawal,
            };
        yield RawTransaction {
            tx_id: 1,
            client_id: 1,
            amount: None,
            variant: RawTransactionVariant::Dispute,
            };
        yield RawTransaction {
            tx_id: 1,
            client_id: 1,
            amount: None,
            variant: RawTransactionVariant::Chargeback,
            };
        };

        let mut client = Client::new(1);

        client.process_activity(stream).await;

        assert!(client.available_balance.abs() < f64::EPSILON);
        assert!(client.held_balance.abs() < f64::EPSILON);
        assert!(client.total_balance.abs() < f64::EPSILON);
        assert!(client.locked);

        Ok(())
    }

    #[tokio::test]
    async fn it_stops_processing_transactions_when_a_client_is_locked() -> Result<()> {
        let stream = stream! {
        yield RawTransaction {
            tx_id: 1,
            client_id: 1,
            amount: Some(1_500.0_f64),
            variant: RawTransactionVariant::Deposit,
            };
        yield RawTransaction {
            tx_id: 1,
            client_id: 1,
            amount: None,
            variant: RawTransactionVariant::Dispute,
            };
        yield RawTransaction {
            tx_id: 1,
            client_id: 1,
            amount: None,
            variant: RawTransactionVariant::Chargeback,
            };
        yield RawTransaction {
            tx_id: 3,
            client_id: 1,
            amount: Some(1_000.0_f64),
            variant: RawTransactionVariant::Deposit,
            };
        };

        let mut client = Client::new(1);

        client.process_activity(stream).await;

        assert!(client.available_balance.abs() < f64::EPSILON);
        assert!(client.held_balance.abs() < f64::EPSILON);
        assert!(client.total_balance.abs() < f64::EPSILON);
        assert!(client.locked);

        Ok(())
    }

    #[tokio::test]
    async fn it_handles_a_large_volume_of_transactions() -> Result<()> {
        let stream = stream! {
            for i in 0..2000 {
                yield RawTransaction {
                    tx_id: i,
                    client_id: 1,
                    amount: Some(1.0_f64),
                    variant: RawTransactionVariant::Deposit,
                }
            }
        };

        let mut client = Client::new(1);

        client.process_activity(stream).await;

        assert!((client.available_balance - 2000.0).abs() < f64::EPSILON);
        assert!((client.total_balance - client.available_balance).abs() < f64::EPSILON);
        assert!(client.held_balance.abs() < f64::EPSILON);
        assert!(!client.locked);

        Ok(())
    }

    #[tokio::test]
    async fn it_handles_resolving_deposit_disputes_after_window_expired() -> Result<()> {
        let stream = stream! {
            yield RawTransaction {
                tx_id: 0,
                client_id: 1,
                amount: Some(1.0_f64),
                variant: RawTransactionVariant::Deposit,
            };
            yield RawTransaction {
                tx_id: 1,
                client_id: 1,
                amount: None,
                variant: RawTransactionVariant::Dispute,
            };
            for i in 0..1000 {
                yield RawTransaction {
                    tx_id: i + 1,
                    client_id: 1,
                    amount: Some(1.0_f64),
                    variant: RawTransactionVariant::Deposit,
                };
            }
        };

        let mut client = Client::new(1);

        client.process_activity(stream).await;

        assert!((client.available_balance - 1001.0).abs() < f64::EPSILON);
        assert!((client.total_balance - client.available_balance).abs() < f64::EPSILON);
        assert!(client.held_balance.abs() < f64::EPSILON);
        assert!(!client.locked);

        Ok(())
    }

    #[tokio::test]
    async fn it_handles_resolving_withdrawal_disputes_after_window_expired() -> Result<()> {
        let stream = stream! {
            yield RawTransaction {
                tx_id: 0,
                client_id: 1,
                amount: Some(1.0_f64),
                variant: RawTransactionVariant::Deposit,
            };
            yield RawTransaction {
                tx_id: 1,
                client_id: 1,
                amount: Some(1.0_f64),
                variant: RawTransactionVariant::Withdrawal,
            };
            yield RawTransaction {
                tx_id: 1,
                client_id: 1,
                amount: None,
                variant: RawTransactionVariant::Dispute,
            };
            for i in 0..1000 {
                yield RawTransaction {
                    tx_id: i + 2,
                    client_id: 1,
                    amount: Some(1.0_f64),
                    variant: RawTransactionVariant::Deposit,
                };
            }
        };

        let mut client = Client::new(1);

        client.process_activity(stream).await;

        assert!((client.available_balance - 1000.0).abs() < f64::EPSILON);
        assert!((client.total_balance - client.available_balance).abs() < f64::EPSILON);
        assert!(client.held_balance.abs() < f64::EPSILON);
        assert!(!client.locked);

        Ok(())
    }

    #[tokio::test]
    async fn it_handles_resolving_a_dispute_for_a_failed_withdrawal() -> Result<()> {
        let stream = stream! {
            yield RawTransaction {
                tx_id: 1,
                client_id: 1,
                amount: Some(1.0_f64),
                variant: RawTransactionVariant::Withdrawal,
            };
            yield RawTransaction {
                tx_id: 1,
                client_id: 1,
                amount: None,
                variant: RawTransactionVariant::Dispute,
            };
            yield RawTransaction {
                tx_id: 2,
                client_id: 1,
                amount: None,
                variant: RawTransactionVariant::Resolve,
            };
        };

        let mut client = Client::new(1);

        client.process_activity(stream).await;

        assert!(client.available_balance.abs() < f64::EPSILON);
        assert!(client.total_balance.abs() < f64::EPSILON);
        assert!(client.held_balance.abs() < f64::EPSILON);
        assert!(!client.locked);

        Ok(())
    }

    #[tokio::test]
    async fn it_handles_a_chargeback_for_a_failed_withdrawal() -> Result<()> {
        let stream = stream! {
            yield RawTransaction {
                tx_id: 1,
                client_id: 1,
                amount: Some(1.0_f64),
                variant: RawTransactionVariant::Withdrawal,
            };
            yield RawTransaction {
                tx_id: 1,
                client_id: 1,
                amount: None,
                variant: RawTransactionVariant::Dispute,
            };
            yield RawTransaction {
                tx_id: 1,
                client_id: 1,
                amount: None,
                variant: RawTransactionVariant::Chargeback,
            };
        };

        let mut client = Client::new(1);

        client.process_activity(stream).await;

        assert!(client.available_balance.abs() < f64::EPSILON);
        assert!(client.total_balance.abs() < f64::EPSILON);
        assert!(client.held_balance.abs() < f64::EPSILON);
        assert!(client.locked);

        Ok(())
    }

    #[tokio::test]
    async fn it_serializes_client_to_csv() -> Result<()> {
        let mut client = Client::new(1);
        client.available_balance = 1.0_f64;
        client.total_balance = 1.0_f64;
        client.held_balance = 1.0_f64;

        let mut writer = csv::Writer::from_writer(vec![]);
        writer.serialize(&client)?;
        let data = String::from_utf8(writer.into_inner()?)?;
        assert_eq!(
            data,
            "\
client,available,held,total,locked
1,1.0,1.0,1.0,false
"
        );

        Ok(())
    }

    #[tokio::test]
    async fn it_handles_deposits_withdrawals_disputes_and_resolves() -> Result<()> {
        let stream = stream! {
             yield RawTransaction {
                tx_id: 1,
                client_id: 1,
                amount: Some(1000.0_f64),
                variant: RawTransactionVariant::Deposit,
            };
            yield RawTransaction {
                tx_id: 2,
                client_id: 1,
                amount: Some(500.0_f64),
                variant: RawTransactionVariant::Withdrawal,
            };
            yield RawTransaction {
                tx_id: 1,
                client_id: 1,
                amount: None,
                variant: RawTransactionVariant::Dispute,
            };
            yield RawTransaction {
                tx_id: 2,
                client_id: 1,
                amount: None,
                variant: RawTransactionVariant::Dispute
            };
            yield RawTransaction {
                tx_id: 1,
                client_id: 1,
                amount: None,
                variant: RawTransactionVariant::Resolve
            };
            yield RawTransaction {
                tx_id: 2,
                client_id: 1,
                amount: None,
                variant: RawTransactionVariant::Resolve
            };
        };

        let mut client = Client::new(1);

        client.process_activity(stream).await;

        assert!((client.available_balance - 500.0).abs() < f64::EPSILON);
        assert!((client.total_balance - client.available_balance).abs() < f64::EPSILON);
        assert!(client.held_balance.abs() < f64::EPSILON);
        assert!(!client.locked);

        Ok(())
    }
}
