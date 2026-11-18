# Databricks notebook source

dbutils.widgets.text("CATALOG", "workspace", "Unity Catalog catalog")
dbutils.widgets.text("SCHEMA", "ecom_poc", "Schema")
dbutils.widgets.text("RAW_VOLUME", "raw_files", "Raw volume")
dbutils.widgets.text("BATCH_ID", "batch-20261119-001", "Stable batch identifier")

catalog = dbutils.widgets.get("CATALOG")
schema = dbutils.widgets.get("SCHEMA")
raw_volume = dbutils.widgets.get("RAW_VOLUME")
batch_id = dbutils.widgets.get("BATCH_ID")

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{raw_volume}`")

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

raw_path = f"/Volumes/{catalog}/{schema}/{raw_volume}"
print(f"catalog={catalog}")
print(f"schema={schema}")
print(f"raw_path={raw_path}")
print(f"batch_id={batch_id}")
