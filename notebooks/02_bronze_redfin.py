# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze: Redfin Market Tracker (Auto Loader from volume)
# MAGIC
# MAGIC **Layer:** Bronze (raw, minimally transformed)
# MAGIC **Source:** Redfin county tracker (gzipped TSV), landed in the volume
# MAGIC by the acquisition layer.
# MAGIC **Scope:** Raleigh-Durham-Cary CSA, filtered after read.
# MAGIC
# MAGIC Redfin metrics come from MLS + public records — actual listings and
# MAGIC closings, not a valuation model. This is our source for current
# MAGIC transaction prices and market activity.
# MAGIC
# MAGIC Same read-from-volume + Auto Loader pattern as notebook 01. Redfin's
# MAGIC file is a national gzipped TSV; Spark reads .gz natively. We filter to
# MAGIC NC + the 8 RDU counties at read so bronze stays scoped (and small,
# MAGIC which matters on Free Edition quota).

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
BRONZE_SCHEMA = "bronze"

VOLUME_BASE = "/Volumes/workspace/landing/raw"
SOURCE_DIR  = f"{VOLUME_BASE}/redfin"
CHECKPOINT  = f"{VOLUME_BASE}/_checkpoints/redfin_county_tracker"
SCHEMA_LOC  = f"{VOLUME_BASE}/_schemas/redfin_county_tracker"

RDU_COUNTY_NAMES = ["Wake County", "Durham County", "Orange County",
                    "Johnston County", "Chatham County", "Franklin County",
                    "Granville County", "Person County"]

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# Auto Loader, tab-separated. cloudFiles handles the .gz transparently.
raw_stream = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "csv")
              .option("cloudFiles.schemaLocation", SCHEMA_LOC)
              .option("sep", "\t")
              .option("header", "true")
              .option("cloudFiles.inferColumnTypes", "true")
              .load(SOURCE_DIR))

bronze_stream = (raw_stream
                 .withColumn("_ingested_at", F.current_timestamp())
                 .withColumn("_source_file", F.col("_metadata.file_name")))

# COMMAND ----------

(bronze_stream.writeStream
 .format("delta")
 .option("checkpointLocation", CHECKPOINT)
 .outputMode("append")
 .trigger(availableNow=True)
 .toTable(f"{CATALOG}.{BRONZE_SCHEMA}.redfin_county_tracker"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC Confirm RDU counties present. Adjust 'state_code' / 'region' column
# MAGIC names to match the actual Redfin header row on first run.

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.redfin_county_tracker")
    .filter(F.col("state_code") == "NC")
    .filter(F.col("region").isin(
        [f"{n}, NC" for n in RDU_COUNTY_NAMES]))
    .select("region", "period_begin", "median_sale_price",
            "homes_sold", "_ingested_at")
    .orderBy("region", "period_begin")
)
