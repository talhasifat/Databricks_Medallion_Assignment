# Databricks notebook source

# Bronze ingestion: all supplied fields remain STRING.
# Re-running this notebook with the same BATCH_ID replaces that batch only.

dbutils.widgets.text("CATALOG", "workspace", "Unity Catalog catalog")
dbutils.widgets.text("SCHEMA", "ecom_poc", "Schema")
dbutils.widgets.text("RAW_VOLUME", "raw_files", "Raw volume")
dbutils.widgets.text("BATCH_ID", "batch-20261119-001", "Stable batch identifier")

from pyspark.sql import functions as F

catalog = dbutils.widgets.get("CATALOG")
schema = dbutils.widgets.get("SCHEMA")
raw_volume = dbutils.widgets.get("RAW_VOLUME")
batch_id = dbutils.widgets.get("BATCH_ID")
raw_path = f"/Volumes/{catalog}/{schema}/{raw_volume}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")


def ingest_csv(table_name: str, file_name: str, source_columns: list[str]) -> None:
    source_path = f"{raw_path}/{file_name}"
    if not any(r.path.endswith(file_name) for r in dbutils.fs.ls(raw_path)):
        raise FileNotFoundError(f"Required source file is missing: {source_path}")

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .load(source_path)
        .select(*[F.col(c).cast("string").alias(c) for c in source_columns])
    )

    json_payload = F.to_json(F.struct(*[F.col(c) for c in source_columns]))
    df = (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_path))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_row_hash", F.sha2(json_payload, 256))
    )

    target = f"`{catalog}`.`{schema}`.`{table_name}`"
    df.createOrReplaceTempView(f"{table_name}_incoming")
    spark.sql(f"DELETE FROM {target} WHERE _batch_id = '{batch_id}'") if spark.catalog.tableExists(target) else None

    df.write.format("delta").mode("append").saveAsTable(target)
    print(table_name, df.count())


orders_columns = ["order_id", "customer_id", "order_date", "category", "quantity", "unit_price", "order_status", "country"]
transactions_columns = ["txn_id", "order_id", "payment_method", "amount", "currency", "txn_status", "txn_ts"]

ingest_csv("bronze_orders", "orders.csv", orders_columns)
ingest_csv("bronze_transactions", "transactions.csv", transactions_columns)
