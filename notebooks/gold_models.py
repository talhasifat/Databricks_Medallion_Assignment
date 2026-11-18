# Databricks notebook source

dbutils.widgets.text("CATALOG", "workspace", "Unity Catalog catalog")
dbutils.widgets.text("SCHEMA", "ecom_poc", "Schema")

from pyspark.sql import functions as F

catalog = dbutils.widgets.get("CATALOG")
schema = dbutils.widgets.get("SCHEMA")
out = f"`{catalog}`.`{schema}`"

orders = spark.table(f"{out}.silver_orders")
transactions = spark.table(f"{out}.silver_transactions")

# Revenue is only successful USD transactions joined once to a unique Silver order.
successful_usd = (
    transactions.join(orders, "order_id", "inner")
    .filter((F.col("txn_status") == "success") & (F.col("currency") == "USD") & (F.col("order_status") == "completed"))
)

# Daily sales: one row per order date.
daily_sales = (
    orders.groupBy(F.col("order_date").alias("sales_date"))
    .agg(
        F.countDistinct("order_id").alias("order_count"),
        F.sum(F.when(F.col("order_status") == "cancelled", 1).otherwise(0)).cast("long").alias("cancelled_order_count"),
    )
    .join(
        successful_usd.groupBy(F.col("order_date").alias("sales_date"))
        .agg(F.round(F.sum("amount"), 2).alias("total_revenue")),
        "sales_date",
        "left",
    )
    .fillna(0)
    .withColumn("cancellation_rate", F.round(F.col("cancelled_order_count") / F.col("order_count"), 6))
    .select("sales_date", "total_revenue", "order_count", "cancelled_order_count", "cancellation_rate")
)

# Revenue breakdown: category and country.
revenue_breakdown = (
    successful_usd.groupBy("category", "country")
    .agg(
        F.round(F.sum("amount"), 2).alias("total_revenue"),
        F.countDistinct("txn_id").alias("successful_payment_count"),
    )
)

# Payment success: one row per payment method.
payment_success = (
    transactions.groupBy("payment_method")
    .agg(
        F.count("*").alias("payment_count"),
        F.sum(F.when(F.col("txn_status") == "success", 1).otherwise(0)).cast("long").alias("successful_count"),
        F.sum(F.when(F.col("txn_status") == "failed", 1).otherwise(0)).cast("long").alias("failed_count"),
    )
    .withColumn("success_rate", F.round(F.col("successful_count") / F.col("payment_count"), 6))
)

# Top customers: successful completed USD payments, joined once to unique accepted orders.
top_customers = (
    successful_usd.groupBy("customer_id")
    .agg(
        F.round(F.sum("amount"), 2).alias("total_spend"),
        F.countDistinct("txn_id").alias("successful_payment_count"),
        F.countDistinct("order_id").alias("order_count"),
    )
    .orderBy(F.desc("total_spend"), F.asc("customer_id"))
)

daily_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{out}.gold_daily_sales")
revenue_breakdown.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{out}.gold_revenue_breakdown")
payment_success.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{out}.gold_payment_success")
top_customers.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{out}.gold_top_customers")
