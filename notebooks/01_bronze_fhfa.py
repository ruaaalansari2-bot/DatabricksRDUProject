# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: FHFA House Price Index (batch read from volume)
# MAGIC
# MAGIC **Layer:** Bronze (raw, minimally transformed)
# MAGIC **Source:** FHFA HPI county file, landed in the volume by the GitHub
# MAGIC Actions acquisition layer (acquisition/acquire.py + upload.py).
# MAGIC **Scope:** Syracuse, NY Metropolitan Statistical Area, filtered after read.
# MAGIC
# MAGIC ### Why batch overwrite (not Auto Loader)
# MAGIC FHFA publishes one full-history file that is replaced each quarter.
# MAGIC Overwriting the bronze table on each run is correct and avoids duplicates.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
BRONZE_SCHEMA = "bronze"
SOURCE_FILE = "/Volumes/workspace/landing/raw/fhfa/hpi_county.csv"

# Syracuse MSA County FIPS codes (New York state code 36 + county codes)
SYRACUSE_COUNTY_FIPS = ["36067",  # Onondaga County (Syracuse city)
                        "36053",  # Madison County
                        "36075"]  # Oswego County

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

raw = (spark.read
       .option("header", "true")
       .option("inferSchema", "true")
       .csv(SOURCE_FILE))

bronze = (raw
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_source_file", F.lit(SOURCE_FILE)))

# COMMAND ----------

(bronze.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.fhfa_hpi_county"))

# COMMAND ----------

# Validation — Syracuse MSA counties only.
display(
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.fhfa_hpi_county")
    .filter(F.col("fips_code").cast("string").isin(SYRACUSE_COUNTY_FIPS))
    .select("fips_code", "year", "hpi", "annual_change_pct", "_ingested_at")
    .orderBy("fips_code", "year")
)
