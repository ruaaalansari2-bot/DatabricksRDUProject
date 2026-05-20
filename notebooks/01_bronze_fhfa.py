# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: FHFA House Price Index (Auto Loader from volume)
# MAGIC
# MAGIC **Layer:** Bronze (raw, minimally transformed)
# MAGIC **Source:** FHFA HPI county file, landed in the volume by the GitHub
# MAGIC Actions acquisition layer (acquisition/acquire.py + upload.py).
# MAGIC **Scope:** Raleigh-Durham-Cary CSA, filtered after read.
# MAGIC
# MAGIC ### Read-from-volume pattern (Free Edition)
# MAGIC Free Edition serverless cannot fetch arbitrary internet URLs, so the
# MAGIC file is acquired externally and uploaded to a Unity Catalog volume.
# MAGIC This notebook uses **Auto Loader** (`cloudFiles`) to incrementally
# MAGIC ingest whatever new files have landed in the source's volume folder —
# MAGIC it tracks processed files via a checkpoint, so re-runs only pick up
# MAGIC new arrivals. This is the JD's "reliable ingestion + incremental load."
# MAGIC
# MAGIC ### Why FHFA (not a model valuation)
# MAGIC Repeat-sales index: measures realized price change on the same homes.
# MAGIC No algorithmic home-value estimate is used anywhere in this project.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
BRONZE_SCHEMA = "bronze"

# Volume layout mirrors the acquisition staging tree: <volume>/fhfa/...
VOLUME_BASE   = "/Volumes/workspace/landing/raw"
SOURCE_DIR    = f"{VOLUME_BASE}/fhfa"
CHECKPOINT    = f"{VOLUME_BASE}/_checkpoints/fhfa_hpi_county"
SCHEMA_LOC    = f"{VOLUME_BASE}/_schemas/fhfa_hpi_county"

RDU_COUNTY_FIPS = ["37183", "37063", "37135", "37101",
                   "37037", "37069", "37077", "37145"]

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# Auto Loader incremental read. cloudFiles tracks which files it has already
# processed in the checkpoint, so each run ingests only new files.
raw_stream = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "csv")
              .option("cloudFiles.schemaLocation", SCHEMA_LOC)
              .option("header", "true")
              .option("cloudFiles.inferColumnTypes", "true")
              .load(SOURCE_DIR))

# Add ingestion metadata. _metadata.file_name is provided by Auto Loader.
bronze_stream = (raw_stream
                 .withColumn("_ingested_at", F.current_timestamp())
                 .withColumn("_source_file", F.col("_metadata.file_name")))

# COMMAND ----------

# availableNow=True processes all currently-available new files, then stops —
# ideal for a scheduled job (vs. an always-on stream).
(bronze_stream.writeStream
 .format("delta")
 .option("checkpointLocation", CHECKPOINT)
 .outputMode("append")
 .trigger(availableNow=True)
 .toTable(f"{CATALOG}.{BRONZE_SCHEMA}.fhfa_hpi_county"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC Filter to the 8 RDU counties for a sanity check. (Scope filtering for
# MAGIC downstream use happens in silver; bronze stays close to raw.)
# MAGIC Adjust the FIPS column name to match the actual FHFA file header.

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.fhfa_hpi_county")
    .filter(F.col("fips_code").cast("string").isin(RDU_COUNTY_FIPS))
    .select("fips_code", "year", "_ingested_at", "_source_file")
    .orderBy("fips_code", "year")
)
