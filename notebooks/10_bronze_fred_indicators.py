# Databricks notebook source
# MAGIC %md
# MAGIC # 10 - Bronze: FRED Economic Indicators
# MAGIC
# MAGIC **Layer:** Bronze
# MAGIC **Source:** Federal Reserve Economic Data (FRED) — additional series
# MAGIC beyond the 30-year mortgage rate already in notebook 03.
# MAGIC
# MAGIC Series ingested:
# MAGIC - MORTGAGE15US — 15-year fixed mortgage rate (weekly)
# MAGIC - HOUST         — Total housing starts, national (monthly)
# MAGIC - HOUST1F       — Single-family housing starts, national (monthly)
# MAGIC - NCBPPRIVSA    — NC building permits authorized (monthly, SA)
# MAGIC - NCUR           — NC unemployment rate (monthly)
# MAGIC - MEDLISPRINC   — NC median listing price (monthly)

# COMMAND ----------

import json, os
from pyspark.sql import functions as F

CATALOG = "workspace"
BRONZE_SCHEMA = "bronze"
VOLUME_BASE = "/Volumes/workspace/landing/raw/fred"

SERIES_FILES = {
    "MORTGAGE15US": f"{VOLUME_BASE}/mortgage_15yr.json",
    "HOUST":        f"{VOLUME_BASE}/housing_starts.json",
    "HOUST1F":      f"{VOLUME_BASE}/housing_starts_sfr.json",
    "NCBPPRIVSA":   f"{VOLUME_BASE}/nc_building_permits.json",
    "NCUR":         f"{VOLUME_BASE}/nc_unemployment.json",
    "MEDLISPRINC":  f"{VOLUME_BASE}/nc_median_listing_price.json",
}

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# Parse all FRED series into one unified observations table.
all_dfs = []
for series_id, path in SERIES_FILES.items():
    with open(path, "r") as f:
        payload = json.load(f)
    rows = [
        (series_id, obs["date"], obs["value"])
        for obs in payload.get("observations", [])
    ]
    df = spark.createDataFrame(rows, ["series_id", "observation_date", "value_raw"])
    all_dfs.append(df)

combined = all_dfs[0]
for df in all_dfs[1:]:
    combined = combined.union(df)

bronze = (combined
          .withColumn("observation_date", F.col("observation_date").cast("date"))
          .withColumn("value",
                      F.when(F.col("value_raw") == ".", None)
                       .otherwise(F.col("value_raw").cast("double")))
          .drop("value_raw")
          .withColumn("_ingested_at", F.current_timestamp()))

# COMMAND ----------

(bronze.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.fred_indicators"))

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.fred_indicators")
    .orderBy("series_id", F.col("observation_date").desc())
    .limit(30))
