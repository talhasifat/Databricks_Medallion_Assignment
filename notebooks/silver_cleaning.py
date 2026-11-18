# Databricks notebook source

dbutils.widgets.text("CATALOG", "workspace", "Unity Catalog catalog")
dbutils.widgets.text("SCHEMA", "ecom_poc", "Schema")

from pyspark.sql import functions as F
from pyspark.sql.window import Window

catalog = dbutils.widgets.get("CATALOG")
schema = dbutils.widgets.get("SCHEMA")
bronze_orders = f"`{catalog}`.`{schema}`.`bronze_orders`"
bronze_transactions = f"`{catalog}`.`{schema}`.`bronze_transactions`"
out = f"`{catalog}`.`{schema}`"

# Orders: validation precedes deterministic deduplication.
orders = spark.table(bronze_orders)
orders_typed = orders.select(
    "order_id", "customer_id", "category", "order_status", "country",
    F.col("order_id").alias("order_id_raw"),
    F.col("customer_id").alias("customer_id_raw"),
    F.col("order_date").alias("order_date_raw"),
    F.col("category").alias("category_raw"),
    F.col("quantity").alias("quantity_raw"),
    F.col("unit_price").alias("unit_price_raw"),
    F.col("order_status").alias("order_status_raw"),
    F.col("country").alias("country_raw"),
    "_ingested_at", "_source_file", "_batch_id", "_row_hash",
    F.trim(F.col("order_id")).alias("order_id_trim"),
    F.trim(F.col("customer_id")).alias("customer_id_trim"),
    F.expr("try_cast(order_date AS DATE)").alias("order_date"),
    F.lower(F.trim(F.col("category"))).alias("category_norm"),
    F.expr("try_cast(quantity AS INT)").alias("quantity"),
    F.expr("try_cast(unit_price AS DECIMAL(18,2))").alias("unit_price"),
    F.lower(F.trim(F.col("order_status"))).alias("order_status_norm"),
    F.upper(F.trim(F.col("country"))).alias("country_norm"),
)

# Validation reason matrix. Invalid rows are quarantined before deduplication.
orders_validated = orders_typed.withColumn(
    "rejection_reason",
    F.concat_ws("|",
        F.when(F.col("order_id_trim").isNull() | (F.col("order_id_trim") == ""), F.lit("missing_order_id")),
        F.when(F.col("customer_id_trim").isNull() | (F.col("customer_id_trim") == ""), F.lit("missing_customer_id")),
        F.when(F.col("order_date").isNull(), F.lit("invalid_order_date")),
        F.when(F.col("order_date") > F.current_date(), F.lit("future_order_date")),
        F.when(F.col("category_norm").isNull() | (F.col("category_norm") == ""), F.lit("missing_category")),
        F.when(F.col("quantity").isNull() | (F.col("quantity") < 1) | (F.col("quantity") > 100), F.lit("invalid_quantity")),
        F.when(F.col("unit_price").isNull() | (F.col("unit_price") <= 0) | (F.col("unit_price") > 100000), F.lit("invalid_unit_price")),
        F.when(~F.col("order_status_norm").isin("completed", "cancelled", "pending", "refunded"), F.lit("invalid_order_status")),
        F.when(F.col("country_norm").isNull() | (F.col("country_norm") == ""), F.lit("missing_country")),
    )
)

orders_validated = orders_validated.withColumn(
    "is_valid",
    (F.col("rejection_reason") == "") | F.col("rejection_reason").isNull()
)

quarantine_orders = (
    orders_validated.filter(~F.col("is_valid"))
    .select(
        "_row_hash", "_source_file", "_batch_id", "_ingested_at",
        "order_id_raw", "customer_id_raw", "order_date_raw", "category_raw", "quantity_raw",
        "unit_price_raw", "order_status_raw", "country_raw",
        "rejection_reason",
    )
)

valid_orders = orders_validated.filter(F.col("is_valid")).withColumn("order_id", F.col("order_id_trim"))
order_window = Window.partitionBy("order_id").orderBy(F.asc("_row_hash"), F.asc("_source_file"), F.asc("_batch_id"))
ranked_orders = valid_orders.withColumn("_dedupe_rank", F.row_number().over(order_window))

duplicate_orders = ranked_orders.filter(F.col("_dedupe_rank") > 1).select(
    "_row_hash", "_source_file", "_batch_id", "_ingested_at", "order_id",
    F.lit("duplicate_order_id").alias("rejection_reason"),
)
silver_orders = ranked_orders.filter(F.col("_dedupe_rank") == 1).select(
    F.col("order_id"), F.col("customer_id_trim").alias("customer_id"),
    F.col("order_date").cast("date").alias("order_date"),
    F.col("category_norm").alias("category"),
    F.col("quantity").cast("int").alias("quantity"),
    F.col("unit_price").cast("decimal(18,2)").alias("unit_price"),
    F.col("order_status_norm").alias("order_status"),
    F.col("country_norm").alias("country"),
    "_ingested_at", "_source_file", "_batch_id", "_row_hash",
)

# Transactions: validation then deduplication.
transactions = spark.table(bronze_transactions)
transactions_typed = transactions.select(
    "_ingested_at", "_source_file", "_batch_id", "_row_hash",
    F.col("txn_id").alias("txn_id_raw"),
    F.col("order_id").alias("order_id_raw"),
    F.col("payment_method").alias("payment_method_raw"),
    F.col("amount").alias("amount_raw"),
    F.col("currency").alias("currency_raw"),
    F.col("txn_status").alias("txn_status_raw"),
    F.col("txn_ts").alias("txn_ts_raw"),
    F.trim(F.col("txn_id")).alias("txn_id"),
    F.trim(F.col("order_id")).alias("order_id"),
    F.lower(F.trim(F.col("payment_method"))).alias("payment_method"),
    F.expr("try_cast(amount AS DECIMAL(18,2))").alias("amount"),
    F.upper(F.trim(F.col("currency"))).alias("currency"),
    F.lower(F.trim(F.col("txn_status"))).alias("txn_status"),
    F.expr("try_to_timestamp(txn_ts, 'yyyy-MM-dd HH:mm:ss')").alias("txn_ts"),
)

accepted_order_ids = silver_orders.select(F.col("order_id").alias("valid_order_id"))
transactions_typed = transactions_typed.join(
    accepted_order_ids, transactions_typed.order_id == accepted_order_ids.valid_order_id, "left"
).withColumn(
    "rejection_reason",
    F.concat_ws("|",
        F.when(F.col("txn_id").isNull() | (F.col("txn_id") == ""), F.lit("missing_txn_id")),
        F.when(F.col("order_id").isNull() | (F.col("order_id") == ""), F.lit("missing_order_id")),
        F.when(F.col("valid_order_id").isNull(), F.lit("orphan_order_id")),
        F.when(F.col("payment_method").isNull() | ~F.col("payment_method").isin("credit_card", "debit_card", "paypal", "bank_transfer", "cash"), F.lit("invalid_payment_method")),
        F.when(F.col("amount").isNull() | (F.col("amount") <= 0) | (F.col("amount") > 100000), F.lit("invalid_amount")),
        F.when(~F.col("currency").isin("USD", "EUR", "GBP", "JPY", "CAD"), F.lit("unsupported_currency")),
        F.when(~F.col("txn_status").isin("success", "failed", "pending", "refund"), F.lit("invalid_txn_status")),
        F.when(F.col("txn_ts").isNull(), F.lit("invalid_txn_timestamp")),
        F.when(F.col("txn_ts") > F.current_timestamp(), F.lit("future_txn_timestamp")),
    )
).withColumn("is_valid", (F.col("rejection_reason") == "") | F.col("rejection_reason").isNull()).drop("valid_order_id")

quarantine_transactions = (
    transactions_typed.filter(~F.col("is_valid"))
    .select(
        "_row_hash", "_source_file", "_batch_id", "_ingested_at",
        "txn_id_raw", "order_id_raw", "payment_method_raw", "amount_raw", "currency_raw", "txn_status_raw", "txn_ts_raw",
        "txn_id", "order_id", "payment_method", "amount", "currency", "txn_status", "txn_ts",
        "rejection_reason",
    )
)

valid_transactions = transactions_typed.filter(F.col("is_valid"))
txn_window = Window.partitionBy("txn_id").orderBy(F.asc("_row_hash"), F.asc("_source_file"), F.asc("_batch_id"))
ranked_transactions = valid_transactions.withColumn("_dedupe_rank", F.row_number().over(txn_window))

duplicate_transactions = ranked_transactions.filter(F.col("_dedupe_rank") > 1).select(
    "_row_hash", "_source_file", "_batch_id", "_ingested_at", "txn_id",
    F.lit("duplicate_txn_id").alias("rejection_reason"),
)
silver_transactions = ranked_transactions.filter(F.col("_dedupe_rank") == 1).select(
    "txn_id", "order_id", "payment_method",
    F.col("amount").cast("decimal(18,2)").alias("amount"),
    "currency", "txn_status", "txn_ts",
    "_ingested_at", "_source_file", "_batch_id", "_row_hash",
)

silver_orders.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{out}.silver_orders")
quarantine_orders.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{out}.quarantine_orders")
duplicate_orders.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{out}.duplicate_orders")
silver_transactions.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{out}.silver_transactions")
quarantine_transactions.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{out}.quarantine_transactions")
duplicate_transactions.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{out}.duplicate_transactions")
