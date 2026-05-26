# Databricks notebook source
# MAGIC %md
# MAGIC # 30 - Silver: data_quality_log
# MAGIC
# MAGIC **Role:** Audit table written after every silver run. Records row counts,
# MAGIC null rates on key columns, latest data timestamp, and a pass/fail flag.
# MAGIC Run this notebook last in the silver pipeline.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

CATALOG       = "workspace"
SILVER_SCHEMA = "silver"

# Tables to audit with their key freshness column.
SILVER_TABLES = {
    "dim_geography":        "_updated_at",
    "dim_metro":            "_updated_at",
    "dim_date":             "_updated_at",
    "home_prices":          "date_key",
    "market_activity":      "date_key",
    "rentals":              "date_key",
    "demographics":         "_ingested_at",
    "economic_indicators":  "date_key",
    "building_permits":     "date_key",
    "parcel_sales":         "sale_date",
}

# COMMAND ----------

rows = []
checked_at = datetime.utcnow()

for table, ts_col in SILVER_TABLES.items():
    full_name = f"{CATALOG}.{SILVER_SCHEMA}.{table}"
    try:
        df = spark.table(full_name)
        row_count = df.count()
        latest_ts = df.agg(F.max(F.col(ts_col).cast("timestamp"))).collect()[0][0]
        # Null rate on first non-metadata column (index 0).
        data_cols = [c for c in df.columns if not c.startswith("_")]
        null_rate = (df.filter(F.col(data_cols[0]).isNull()).count() / max(row_count, 1))
        passed = row_count > 0
        error_msg = None
    except Exception as e:
        row_count, latest_ts, null_rate = 0, None, None
        passed = False
        error_msg = str(e)[:500]

    rows.append((table, row_count, float(null_rate) if null_rate is not None else None,
                 latest_ts, passed, error_msg, checked_at))

# COMMAND ----------

schema = ["table_name", "row_count", "null_rate_key_col",
          "latest_data_ts", "passed", "error_msg", "checked_at"]

log_df = spark.createDataFrame(rows, schema)

(log_df.write
 .format("delta")
 .mode("append")        # append so history is preserved across runs
 .option("mergeSchema", "true")
 .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.data_quality_log"))

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.data_quality_log")
    .orderBy(F.col("checked_at").desc(), "table_name")
    .limit(20))
