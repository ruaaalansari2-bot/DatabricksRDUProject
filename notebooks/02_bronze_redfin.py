# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze: Redfin Market Tracker (batch read from volume)
# MAGIC
# MAGIC **Layer:** Bronze (raw, minimally transformed)
# MAGIC **Source:** Redfin county tracker (gzipped TSV), landed in the volume
# MAGIC by the acquisition layer.
# MAGIC **Scope:** Raleigh-Durham-Cary CSA, filtered after read.
# MAGIC
# MAGIC Redfin publishes one full national file replaced weekly.
# MAGIC Batch overwrite is correct here — no duplicates, no checkpoint needed.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
BRONZE_SCHEMA = "bronze"
SOURCE_FILE = "/Volumes/workspace/landing/raw/redfin/county_market_tracker.tsv000.gz"

RDU_COUNTY_NAMES = ["Wake County", "Durham County", "Orange County",
                    "Johnston County", "Chatham County", "Franklin County",
                    "Granville County", "Person County"]

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# Spark reads .gz natively. Filter to NC + RDU counties at read to keep
# bronze small — the national file is ~200 MB uncompressed.
raw = (spark.read
       .option("header", "true")
       .option("sep", "\t")
       .option("inferSchema", "true")
       .csv(SOURCE_FILE))

bronze = (raw
          .filter(F.col("state_code") == "NC")
          .filter(F.col("region").isin(
              [f"{n}, NC" for n in RDU_COUNTY_NAMES]))
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_source_file", F.lit(SOURCE_FILE)))

# COMMAND ----------

(bronze.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.redfin_county_tracker"))

# COMMAND ----------

# Validation — most recent periods for each RDU county.
display(
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.redfin_county_tracker")
    .select("region", "period_begin", "median_sale_price",
            "homes_sold", "_ingested_at")
    .orderBy("region", "period_begin")
)
