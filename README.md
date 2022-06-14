# Building
Build with `cargo build`
# Running
Run with `cargo run -- <csv-file-path>`
# Testing
Run tests with `cargo test`.  To generate a coverage report, run `cargo make coverage`.
# Notes
The main functions needed to implement are the following:
* Read series of transactions from CSV
* Update client accounts
* Handle disputes and chargebacks
* Generate report of client states as CSV

Questions:
* Is there a window for a client to dispute a transaction?
  * **ASSUMPTION**: The window will be assumed to be of size 1000.  That is, if a dispute is initiated within 1000 txs after a transaction, the funds will be held and subsequent transactions may fail.  
  * Realistically there should be some definite time period defined by either the partner or the company.  This challenge
  did not provide any time information in transactions, so the number of transactions following was used as a proxy.
* Is there a window for a client to resolve a dispute?
  * **ASSUMPTION**: The window will be assumed to be a maximum of 1000 transactions per client.  That is, if a dispute is resolved at any 
  point after a transaction, the funds will either be released or the account will be locked.  Subsequent transactions may
  fail or succeed based on the outcome of the dispute.  If a transaction is disputed but an outcome is not decided within 
  the dispute window, it will default to resolved.  If the client's final total balance is greater than or equal to zero after
  calculating the outcome of a dispute, then the subsequent transactions will be allowed to succeed.
  * Example:
    * Consider the following activity:
    ```csv 
    type, client, tx, amount
    deposit, 1, 1, 1000
    withdrawal, 1, 2, 1000
    dispute, 1, 1,
    dispute, 1, 2,
    resolve, 1, 1,
    resolve, 1, 2,
    ```
    Here the intermediate client balances would look like the following:
    ```csv
    held, available, total
    0, 1000, 1000
    0, 0, 0,
    1000, -1000, 0
    0, 0, 0
    -1000, 1000, 0
    0, 0, 0
    ```
    The final balance for the client in the dispute window is 0, so no transactions need to be rejected.  
    * Now consider an alternate scenario:
    ```csv
    type, client, tx, amount
    deposit, 1, 1, 1000
    withdrawal, 1, 2, 1000
    dispute, 1, 1,
    chargeback, 1, 1,
    ```
    The intermediate client balances are as follows:
    ```csv
    held, available, total
    0, 1000, 1000
    0, 0, 0,
    1000, -1000, 0
    0, -1000, -1000
    ```
    Here the final balance for the client in the dispute window is -1000, so transactions past the deposit need to be
    rejected as the deposit was not resolved in the input window.
  * Realistically there should be some time period defined by either 
  the partner or the company, as well as some default resolution outcome.
* How should the application handle invalid input data?
  * **ASSUMPTION**: The application will generate an error internally but will continue to run, attempting to process future
    transactions.  These errors will be logged to stderr in the case of a malformed or invalid input.
* Are floating point numbers really appropriate for this application?
  * I don't believe they are.  Floating point numbers are generally inappropriate for financial applications.  Ideally 
  you should use integers, denominated in the smallest unit of currency.  In this case, the smallest unit of currency is
  the cent.  For example, $1.00 is represented as 100 cents.  Similarly, $10.00 is represented as 1000 cents.  For more 
  fractional precision, such as denoted in the requirements, the smallest unit of currency is 1/100th of a unit, so the ideal
  internal representation of `1.0` is `10000`.
  * For the sake of simplicity in this code example I won't go through the trouble of implementing this, but in a real environment
  I would insist on doing it like above.
* How should the application report failed transactions?
  * **ASSUMPTION**: Transaction failures will be reported to stderr.
* ~~How should a situation where a dispute would make a client's balance negative be handled?~~
  * Given the above assumption regarding the dispute window, a client's balance will never be allowed to be negative.
* ~~How should a dispute handle a situation where a client does not yet exist?~~ 
  * Create a new client
* How should a dispute be handled when the client ID does not match the referenced tx ID's client ID?
  * **ASSUMPTION**: This should never happen since each transaction is given to the appropriate client object, but 
  the program is robust to handle this scenario in case of developer error. As such these types of transactions will be 
  considered invalid and reported to stderr.
* How should a resolve be handled when the client ID does not match the referenced tx ID's client ID?
  * See above
* How should a chargeback be handled when the client ID does not match the referenced tx ID's client ID?
  * See above
* What if there are multiple disputes for the same transaction ID?
  * **ASSUMPTION**: The first dispute will be considered valid, but subsequent disputes for the same transaction will 
  be ignored, however the incident will be reported to stderr.  
* What if there are multiple resolves for the same transaction ID?
  * Will be handled the same way as multiple disputes.
* What if there are multiple chargebacks for the same transaction ID?
  * Will be handled the same way as multiple disputes.
* ~~What if there are disputes for resolves or chargebacks (or disputes)?~~
  * This situation is impossible since a dispute, resolve, or chargeback does not have an ID.
* What implications follow from an account being locked?
  * **ASSUMPTION**: All transactions after a chargeback has been declared will fail.  For transactions that happened between
  the indicated transaction on the chargeback and the chargeback transaction itself, the transactions will fail only if the balance
  of the client is unable to cover the transaction.  See the discussion on the dispute window.
* What should happen if a dispute is initiated for a failed transaction?
  * **ASSUMPTION**: The account may still be locked if the transaction resolves in a chargeback, but transactions can only 
  fail if it was impossible for them to succeed.  A failed transaction will not impact the balances of a client.