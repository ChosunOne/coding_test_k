//! The purpose of this module is to provide a way to construct a transaction from raw CSV data,
//! and to convert it into a well-formed variant of the transactions that can be used later in the
//! application.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Truncates a floating point number to the specified number of decimal places.
pub fn truncate_to_decimal_places(num: f64, places: i32) -> f64 {
    let ten = 10.0_f64.powi(places);
    // Need to check here because floats will become infinite if they are too large.  We are safe
    // to return `num` in this case because f64s cannot represent fractional values beyond 2^53.
    if num > f64::MAX / ten || num < f64::MIN / ten {
        return num;
    }
    (num * ten).floor() / ten
}

/// An error type for the transaction module.
#[derive(Debug, Error, PartialEq)]
#[non_exhaustive]
pub enum TransactionError {
    /// An error occurred while attempting to convert a `Transaction` to a `Deposit`.
    #[error("Invalid Deposit")]
    InvalidDeposit,
    /// An error occurred while attempting to convert a `Transaction` to a `Withdrawal`.
    #[error("Invalid Withdrawal")]
    InvalidWithdrawal,
    /// An error occurred while attempting to convert a `Transaction` to a `Dispute`.
    #[error("Invalid Dispute")]
    InvalidDispute,
    /// An error occurred while attempting to convert a `Transaction` to a `Resolve`.
    #[error("Invalid Resolve")]
    InvalidResolve,
    /// An error occurred while attempting to convert a `Transaction` to a `ChargeBack`.
    #[error("Invalid Chargeback")]
    InvalidChargeback,
}

/// Transactions have five variants:
/// * Deposit
/// * Withdrawal
/// * Dispute
/// * Resolve
/// * Chargeback
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RawTransactionVariant {
    /// A deposit is a credit to the client's asset account, meaning it should increase the available and
    /// total funds of the client account
    #[serde(rename = "deposit")]
    Deposit,
    /// A withdrawal is a debit to the client's asset account, meaning it should decrease the available and
    /// total funds of the client account.  If a client does not have sufficient available funds the
    /// withdrawal should fail and the total amount of funds should not change
    #[serde(rename = "withdrawal")]
    Withdrawal,
    /// A dispute represents a client's claim that a transaction was erroneous and should be reversed.
    /// The transaction shouldn't be reversed yet but the associated funds should be held. This means
    /// that the clients available funds should decrease by the amount disputed, their held funds should
    /// increase by the amount disputed, while their total funds should remain the same.
    #[serde(rename = "dispute")]
    Dispute,
    /// A resolve represents a resolution to a dispute, releasing the associated held funds. Funds that
    /// were previously disputed are no longer disputed. This means that the clients held funds should
    /// decrease by the amount no longer disputed, their available funds should increase by the
    /// amount no longer disputed, and their total funds should remain the same.
    #[serde(rename = "resolve")]
    Resolve,
    /// A chargeback is the final state of a dispute and represents the client reversing a transaction.
    /// Funds that were held have now been withdrawn. This means that the clients held funds and
    /// total funds should decrease by the amount previously disputed. If a chargeback occurs the
    /// client's account should be immediately frozen.
    #[serde(rename = "chargeback")]
    Chargeback,
}

/// Wrapper for collections of parsed transactions.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq)]
pub enum Transaction {
    Deposit(Deposit),
    Withdrawal(Withdrawal),
    Dispute(Dispute),
    Resolve(Resolve),
    Chargeback(Chargeback),
}
/// A deposit is a credit to the client's asset account, meaning it should increase the available and
/// total funds of the client account
#[non_exhaustive]
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct Deposit {
    /// The ID of the client
    pub client_id: u16,
    /// The ID of the transaction
    pub tx_id: u32,
    /// The amount of the transaction
    pub amount: f64,
    /// Whether or not the transaction was disputed
    pub disputed: bool,
    /// Whether or not the dispute was resolved
    pub resolved: bool,
}

impl TryFrom<RawTransaction> for Deposit {
    type Error = TransactionError;

    #[inline]
    fn try_from(value: RawTransaction) -> Result<Self, Self::Error> {
        if value.variant == RawTransactionVariant::Deposit {
            if let Some(amount) = value.amount {
                if amount < 0.0_f64 {
                    return Err(TransactionError::InvalidDeposit);
                }
                return Ok(Self {
                    client_id: value.client_id,
                    tx_id: value.tx_id,
                    amount: truncate_to_decimal_places(amount, 4),
                    disputed: false,
                    resolved: false,
                });
            }
        }
        Err(TransactionError::InvalidDeposit)
    }
}

/// A withdrawal is a debit to the client's asset account, meaning it should decrease the available and
/// total funds of the client account.  If a client does not have sufficient available funds the
/// withdrawal should fail and the total amount of funds should not change
#[non_exhaustive]
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct Withdrawal {
    /// The ID of the client
    pub client_id: u16,
    /// The ID of the transaction
    pub tx_id: u32,
    /// The amount of the transaction
    pub amount: f64,
    /// Whether the transaction has been disputed
    pub disputed: bool,
    /// Whether or not the dispute was resolved
    pub resolved: bool,
    /// Whether the transaction failed
    pub failed: bool,
}

impl TryFrom<RawTransaction> for Withdrawal {
    type Error = TransactionError;

    #[inline]
    fn try_from(value: RawTransaction) -> Result<Self, Self::Error> {
        if value.variant != RawTransactionVariant::Withdrawal {
            return Err(TransactionError::InvalidWithdrawal);
        }
        if let Some(amount) = value.amount {
            if amount < 0.0_f64 {
                return Err(TransactionError::InvalidWithdrawal);
            }
            return Ok(Self {
                client_id: value.client_id,
                tx_id: value.tx_id,
                amount: truncate_to_decimal_places(amount, 4),
                disputed: false,
                resolved: false,
                failed: false,
            });
        }
        Err(TransactionError::InvalidWithdrawal)
    }
}

/// A dispute represents a client's claim that a transaction was erroneous and should be reversed.
/// The transaction shouldn't be reversed yet but the associated funds should be held. This means
/// that the clients available funds should decrease by the amount disputed, their held funds should
/// increase by the amount disputed, while their total funds should remain the same.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct Dispute {
    /// The ID of the client
    pub client_id: u16,
    /// The ID of the transaction
    pub tx_id: u32,
}

impl TryFrom<RawTransaction> for Dispute {
    type Error = TransactionError;

    #[inline]
    fn try_from(value: RawTransaction) -> Result<Self, Self::Error> {
        if value.variant != RawTransactionVariant::Dispute {
            return Err(TransactionError::InvalidDispute);
        }
        if value.amount.is_some() {
            return Err(TransactionError::InvalidDispute);
        }
        Ok(Self {
            client_id: value.client_id,
            tx_id: value.tx_id,
        })
    }
}

/// A resolve represents a resolution to a dispute, releasing the associated held funds. Funds that
/// were previously disputed are no longer disputed. This means that the clients held funds should
/// decrease by the amount no longer disputed, their available funds should increase by the
/// amount no longer disputed, and their total funds should remain the same.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct Resolve {
    /// The ID of the client
    pub client_id: u16,
    /// The ID of the transaction
    pub tx_id: u32,
}

impl TryFrom<RawTransaction> for Resolve {
    type Error = TransactionError;

    #[inline]
    fn try_from(value: RawTransaction) -> Result<Self, Self::Error> {
        if value.variant != RawTransactionVariant::Resolve {
            return Err(TransactionError::InvalidResolve);
        }
        if value.amount.is_some() {
            return Err(TransactionError::InvalidResolve);
        }
        Ok(Self {
            client_id: value.client_id,
            tx_id: value.tx_id,
        })
    }
}

/// A chargeback is the final state of a dispute and represents the client reversing a transaction.
/// Funds that were held have now been withdrawn. This means that the clients held funds and
/// total funds should decrease by the amount previously disputed. If a chargeback occurs the
/// client's account should be immediately frozen.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct Chargeback {
    /// The ID of the client
    pub client_id: u16,
    /// The ID of the transaction
    pub tx_id: u32,
}

impl TryFrom<RawTransaction> for Chargeback {
    type Error = TransactionError;

    #[inline]
    fn try_from(value: RawTransaction) -> Result<Self, Self::Error> {
        if value.variant != RawTransactionVariant::Chargeback {
            return Err(TransactionError::InvalidChargeback);
        }
        if value.amount.is_some() {
            return Err(TransactionError::InvalidChargeback);
        }
        Ok(Self {
            client_id: value.client_id,
            tx_id: value.tx_id,
        })
    }
}

/// A wrapper type around the possible transaction variants.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RawTransaction {
    /// The type of the transaction.
    #[serde(rename = "type")]
    pub variant: RawTransactionVariant,
    /// The ID of the client
    #[serde(rename = "client")]
    pub client_id: u16,
    /// The ID of the transaction
    #[serde(rename = "tx")]
    pub tx_id: u32,
    /// The amount of the transaction
    pub amount: Option<f64>,
}

unsafe impl Send for RawTransaction {}
unsafe impl Sync for RawTransaction {}

#[cfg(test)]
#[allow(clippy::panic_in_result_fn)]
mod tests {
    use super::*;
    use anyhow::{anyhow, Result};

    #[test]
    fn it_truncates_decimal_numbers_to_4_decimal_places_1() {
        let amount = 0.123_456_789_f64;
        let truncated_amount = truncate_to_decimal_places(amount, 4);
        println!("{}", truncated_amount);
        assert!((truncated_amount - 0.1234_f64).abs() < f64::EPSILON);
    }

    #[test]
    fn it_truncates_decimal_numbers_to_4_decimal_places_2() {
        let amount = 0.123_443_210_f64;
        let truncated_amount = truncate_to_decimal_places(amount, 4);
        println!("{}", truncated_amount);
        assert!((truncated_amount - 0.1234_f64).abs() < f64::EPSILON);
    }

    #[test]
    fn it_returns_large_numbers_without_modification() {
        let amount = f64::MAX / 2.0_f64;
        let rounded_amount = truncate_to_decimal_places(amount, 4);
        println!("{}", rounded_amount);
        assert!((rounded_amount - amount).abs() < f64::EPSILON);
    }

    #[test]
    fn it_deserializes_csv_rows_into_transactions() -> Result<()> {
        let mut csv_rows = r#"type, client, tx, amount
        deposit,1,1,1.00
        withdrawal,2,2,2.00
        dispute,3,3,
        resolve,4,4,
        chargeback,5,5,
        "#
        .trim()
        .to_owned();
        csv_rows.retain(|c| c == '\n' || !c.is_whitespace());
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(csv_rows.as_bytes());
        let mut iter = rdr.deserialize::<RawTransaction>();
        let deposit = iter.next().unwrap()?;
        assert_eq!(
            deposit,
            RawTransaction {
                variant: RawTransactionVariant::Deposit,
                client_id: 1,
                tx_id: 1,
                amount: Some(1.00_f64)
            }
        );
        let withdrawal = iter.next().unwrap()?;
        assert_eq!(
            withdrawal,
            RawTransaction {
                variant: RawTransactionVariant::Withdrawal,
                client_id: 2,
                tx_id: 2,
                amount: Some(2.00_f64)
            }
        );
        let dispute = iter.next().unwrap()?;
        assert_eq!(
            dispute,
            RawTransaction {
                variant: RawTransactionVariant::Dispute,
                client_id: 3,
                tx_id: 3,
                amount: None
            }
        );
        let resolve = iter.next().unwrap()?;
        assert_eq!(
            resolve,
            RawTransaction {
                variant: RawTransactionVariant::Resolve,
                client_id: 4,
                tx_id: 4,
                amount: None
            }
        );
        let chargeback = iter.next().unwrap()?;
        assert_eq!(
            chargeback,
            RawTransaction {
                variant: RawTransactionVariant::Chargeback,
                client_id: 5,
                tx_id: 5,
                amount: None
            }
        );
        Ok(())
    }

    #[test]
    fn it_deserializes_csv_rows_into_transactions_with_invalid_rows() -> Result<()> {
        let mut csv_rows = r#"type, client, tx, amount
        deposit,1,1,1.00
        withdrawal,2,2,2.00
        dispute,3,3,abc
        this is a garbage line
        chargeback,5,5,3.00
        "#
        .trim()
        .to_owned();
        csv_rows.retain(|c| c == '\n' || !c.is_whitespace());
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(csv_rows.as_bytes());
        let mut iter = rdr.deserialize::<RawTransaction>();
        let deposit = iter.next().unwrap()?;
        assert_eq!(
            deposit,
            RawTransaction {
                variant: RawTransactionVariant::Deposit,
                client_id: 1,
                tx_id: 1,
                amount: Some(1.00_f64)
            }
        );
        let withdrawal = iter.next().unwrap()?;
        assert_eq!(
            withdrawal,
            RawTransaction {
                variant: RawTransactionVariant::Withdrawal,
                client_id: 2,
                tx_id: 2,
                amount: Some(2.00_f64)
            }
        );
        let dispute = iter.next().unwrap();
        assert!(dispute.is_err());
        let resolve = iter.next().unwrap();
        assert!(resolve.is_err());
        let chargeback_transaction = iter.next().unwrap()?;
        assert_eq!(
            chargeback_transaction,
            RawTransaction {
                variant: RawTransactionVariant::Chargeback,
                client_id: 5,
                tx_id: 5,
                amount: Some(3.00_f64)
            }
        );
        let chargeback: Result<Chargeback, TransactionError> = chargeback_transaction.try_into();
        assert_eq!(chargeback.unwrap_err(), TransactionError::InvalidChargeback);
        Ok(())
    }

    #[test]
    fn it_deserializes_csv_rows_into_transactions_without_headers() -> Result<()> {
        let mut csv_rows = r#"deposit,1,1,1.00
        withdrawal,2,2,2.00
        dispute,3,3,
        resolve,4,4,
        chargeback,5,5,
        "#
        .trim()
        .to_owned();
        csv_rows.retain(|c| c == '\n' || !c.is_whitespace());
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(csv_rows.as_bytes());
        let mut iter = rdr.deserialize::<RawTransaction>();
        let deposit = iter.next().unwrap()?;
        assert_eq!(
            deposit,
            RawTransaction {
                variant: RawTransactionVariant::Deposit,
                client_id: 1,
                tx_id: 1,
                amount: Some(1.00_f64)
            }
        );
        let withdrawal = iter.next().unwrap()?;
        assert_eq!(
            withdrawal,
            RawTransaction {
                variant: RawTransactionVariant::Withdrawal,
                client_id: 2,
                tx_id: 2,
                amount: Some(2.00_f64)
            }
        );
        let dispute = iter.next().unwrap()?;
        assert_eq!(
            dispute,
            RawTransaction {
                variant: RawTransactionVariant::Dispute,
                client_id: 3,
                tx_id: 3,
                amount: None
            }
        );
        let resolve = iter.next().unwrap()?;
        assert_eq!(
            resolve,
            RawTransaction {
                variant: RawTransactionVariant::Resolve,
                client_id: 4,
                tx_id: 4,
                amount: None
            }
        );
        let chargeback = iter.next().unwrap()?;
        assert_eq!(
            chargeback,
            RawTransaction {
                variant: RawTransactionVariant::Chargeback,
                client_id: 5,
                tx_id: 5,
                amount: None
            }
        );
        Ok(())
    }
    #[test]
    fn it_converts_a_transaction_to_a_deposit() -> Result<()> {
        let tx = RawTransaction {
            variant: RawTransactionVariant::Deposit,
            client_id: 1,
            tx_id: 1,
            amount: Some(1.00_f64),
        };
        let deposit: Deposit = tx.try_into()?;
        assert_eq!(
            deposit,
            Deposit {
                client_id: 1,
                tx_id: 1,
                amount: 1.00_f64,
                disputed: false,
                resolved: false,
            }
        );
        Ok(())
    }

    #[test]
    fn it_fails_to_convert_a_transaction_to_a_deposit_with_negative_amount() -> Result<()> {
        let tx = RawTransaction {
            variant: RawTransactionVariant::Deposit,
            client_id: 1,
            tx_id: 1,
            amount: Some(-1.00_f64),
        };
        let deposit: Result<Deposit, TransactionError> = tx.try_into();
        if deposit == Err(TransactionError::InvalidDeposit) {
            Ok(())
        } else {
            Err(anyhow!(
                "Should have failed to convert a transaction with negative amount to a deposit!"
            ))
        }
    }

    #[test]
    fn it_fails_to_convert_a_withdrawal_into_a_deposit() -> Result<()> {
        let tx = RawTransaction {
            variant: RawTransactionVariant::Withdrawal,
            client_id: 1,
            tx_id: 1,
            amount: Some(1.00_f64),
        };
        let deposit: Result<Deposit, TransactionError> = tx.try_into();
        if deposit == Err(TransactionError::InvalidDeposit) {
            Ok(())
        } else {
            Err(anyhow!(
                "Should have failed to convert a withdrawal to a deposit!"
            ))
        }
    }

    #[test]
    fn it_fails_to_convert_an_invalid_transaction_into_a_deposit() -> Result<()> {
        let tx = RawTransaction {
            variant: RawTransactionVariant::Deposit,
            client_id: 1,
            tx_id: 1,
            amount: None,
        };
        let deposit: Result<Deposit, TransactionError> = tx.try_into();
        if deposit == Err(TransactionError::InvalidDeposit) {
            Ok(())
        } else {
            Err(anyhow!(
                "Should have failed to convert an invalid transaction to a deposit!"
            ))
        }
    }

    #[test]
    fn it_converts_a_transaction_to_a_withdrawal() -> Result<()> {
        let tx = RawTransaction {
            variant: RawTransactionVariant::Withdrawal,
            client_id: 1,
            tx_id: 1,
            amount: Some(1.00_f64),
        };
        let withdrawal: Withdrawal = tx.try_into()?;
        assert_eq!(
            withdrawal,
            Withdrawal {
                client_id: 1,
                tx_id: 1,
                amount: 1.00_f64,
                disputed: false,
                resolved: false,
                failed: false
            }
        );
        Ok(())
    }

    #[test]
    fn it_fails_to_convert_a_transaction_with_negative_amount_to_a_withdrawal() -> Result<()> {
        let tx = RawTransaction {
            variant: RawTransactionVariant::Withdrawal,
            client_id: 1,
            tx_id: 1,
            amount: Some(-1.00_f64),
        };
        let withdrawal: Result<Withdrawal, TransactionError> = tx.try_into();
        if withdrawal == Err(TransactionError::InvalidWithdrawal) {
            Ok(())
        } else {
            Err(anyhow!(
                "Should have failed to convert a transaction with negative amount to a withdrawal!"
            ))
        }
    }

    #[test]
    fn it_fails_to_convert_a_deposit_into_a_withdrawal() -> Result<()> {
        let tx = RawTransaction {
            variant: RawTransactionVariant::Deposit,
            client_id: 1,
            tx_id: 1,
            amount: Some(1.00_f64),
        };
        let withdrawal: Result<Withdrawal, TransactionError> = tx.try_into();
        if withdrawal == Err(TransactionError::InvalidWithdrawal) {
            Ok(())
        } else {
            Err(anyhow!(
                "Should have failed to convert a deposit to a withdrawal!"
            ))
        }
    }

    #[test]
    fn it_fails_to_convert_an_invalid_transaction_into_a_withdrawal() -> Result<()> {
        let tx = RawTransaction {
            variant: RawTransactionVariant::Withdrawal,
            client_id: 1,
            tx_id: 1,
            amount: None,
        };
        let withdrawal: Result<Withdrawal, TransactionError> = tx.try_into();
        if withdrawal == Err(TransactionError::InvalidWithdrawal) {
            Ok(())
        } else {
            Err(anyhow!(
                "Should have failed to convert an invalid transaction to a withdrawal!"
            ))
        }
    }

    #[test]
    fn it_converts_a_transaction_to_a_dispute() -> Result<()> {
        let tx = RawTransaction {
            variant: RawTransactionVariant::Dispute,
            client_id: 1,
            tx_id: 1,
            amount: None,
        };
        let dispute: Dispute = tx.try_into()?;
        assert_eq!(
            dispute,
            Dispute {
                client_id: 1,
                tx_id: 1
            }
        );
        Ok(())
    }

    #[test]
    fn it_fails_to_convert_a_withdrawal_into_a_dispute() -> Result<()> {
        let tx = RawTransaction {
            variant: RawTransactionVariant::Withdrawal,
            client_id: 1,
            tx_id: 1,
            amount: Some(1.00_f64),
        };
        let dispute: Result<Dispute, TransactionError> = tx.try_into();
        if dispute == Err(TransactionError::InvalidDispute) {
            Ok(())
        } else {
            Err(anyhow!(
                "Should have failed to convert a withdrawal to a dispute!"
            ))
        }
    }

    #[test]
    fn it_fails_to_convert_an_invalid_transaction_into_a_dispute() -> Result<()> {
        let tx = RawTransaction {
            variant: RawTransactionVariant::Dispute,
            client_id: 1,
            tx_id: 1,
            amount: Some(1.00_f64),
        };
        let dispute: Result<Dispute, TransactionError> = tx.try_into();
        if dispute == Err(TransactionError::InvalidDispute) {
            Ok(())
        } else {
            Err(anyhow!(
                "Should have failed to convert an invalid transaction to a dispute!"
            ))
        }
    }

    #[test]
    fn it_converts_a_transaction_to_a_resolve() -> Result<()> {
        let tx = RawTransaction {
            variant: RawTransactionVariant::Resolve,
            client_id: 1,
            tx_id: 1,
            amount: None,
        };
        let resolve: Resolve = tx.try_into()?;
        assert_eq!(
            resolve,
            Resolve {
                client_id: 1,
                tx_id: 1
            }
        );
        Ok(())
    }

    #[test]
    fn it_fails_to_convert_a_dispute_into_a_resolve() -> Result<()> {
        let tx = RawTransaction {
            variant: RawTransactionVariant::Dispute,
            client_id: 1,
            tx_id: 1,
            amount: None,
        };
        let resolve: Result<Resolve, TransactionError> = tx.try_into();
        if resolve == Err(TransactionError::InvalidResolve) {
            Ok(())
        } else {
            Err(anyhow!(
                "Should have failed to convert a dispute to a resolve!"
            ))
        }
    }

    #[test]
    fn it_fails_to_convert_an_invalid_transaction_into_a_resolve() -> Result<()> {
        let tx = RawTransaction {
            variant: RawTransactionVariant::Resolve,
            client_id: 1,
            tx_id: 1,
            amount: Some(1.00_f64),
        };
        let resolve: Result<Resolve, TransactionError> = tx.try_into();
        if resolve == Err(TransactionError::InvalidResolve) {
            Ok(())
        } else {
            Err(anyhow!(
                "Should have failed to convert an invalid transaction to a resolve!"
            ))
        }
    }

    #[test]
    fn it_converts_a_transaction_to_a_chargeback() -> Result<()> {
        let tx = RawTransaction {
            variant: RawTransactionVariant::Chargeback,
            client_id: 1,
            tx_id: 1,
            amount: None,
        };
        let chargeback: Chargeback = tx.try_into()?;
        assert_eq!(
            chargeback,
            Chargeback {
                client_id: 1,
                tx_id: 1
            }
        );
        Ok(())
    }

    #[test]
    fn it_fails_to_convert_a_resolve_into_a_chargeback() -> Result<()> {
        let tx = RawTransaction {
            variant: RawTransactionVariant::Resolve,
            client_id: 1,
            tx_id: 1,
            amount: None,
        };
        let chargeback: Result<Chargeback, TransactionError> = tx.try_into();
        if chargeback == Err(TransactionError::InvalidChargeback) {
            Ok(())
        } else {
            Err(anyhow!(
                "Should have failed to convert a resolve to a chargeback!"
            ))
        }
    }

    #[test]
    fn it_fails_to_convert_an_invalid_transaction_into_a_chargeback() -> Result<()> {
        let tx = RawTransaction {
            variant: RawTransactionVariant::Chargeback,
            client_id: 1,
            tx_id: 1,
            amount: Some(1.00_f64),
        };
        let chargeback: Result<Chargeback, TransactionError> = tx.try_into();
        if chargeback == Err(TransactionError::InvalidChargeback) {
            Ok(())
        } else {
            Err(anyhow!(
                "Should have failed to convert an invalid transaction to a chargeback!"
            ))
        }
    }
}
