-- Business validation matrix for ecom_poc.
-- Validation runs before deduplication. All failed records are written to quarantine tables.

-- Order rules
-- order_id:       mandatory; later used as a unique accepted key
-- customer_id:    mandatory
-- order_date:     YYYY-MM-DD, not NULL, not in the future
-- category:       mandatory; normalized to lower-case
-- quantity:       integer in [1, 100]
-- unit_price:     decimal > 0 and <= 100000
-- order_status:   completed, cancelled, pending, or refunded
-- country:        mandatory; normalized to upper-case

-- Transaction rules
-- txn_id:         mandatory; later used as a unique accepted key
-- order_id:       mandatory and present in accepted silver_orders
-- payment_method: credit_card, debit_card, paypal, bank_transfer, or cash
-- amount:         decimal > 0 and <= 100000
-- currency:       USD, EUR, GBP, JPY, or CAD
-- txn_status:     success, failed, pending, or refund
-- txn_ts:         valid timestamp and not in the future

