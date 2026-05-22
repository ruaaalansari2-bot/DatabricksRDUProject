# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Bronze: FRED 30-Year Mortgage Rate (JSON from volume)
# MAGIC Reads the FRED API JSON that the acquisition layer landed in the volume,
# MAGIC parses the observations array into rows, writes a bronze Delta table.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

CATALOG = "workspace"
BRONZE_SCHEMA = "bronze"
SOURCE_FILE = "/Volumes/workspace/landing/raw/fred/mortgage_30yr.json"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# FRED returns a JSON object with an "observations" array of {date, value, ...}.
# Read the whole file as one JSON object (multiLine), then explode the array.
raw = (spark.read
       .option("multiLine", "true")
       .json(SOURCE_FILE))

# COMMAND ----------

# Explode observations into one row per date.
obs = (raw
       .select(F.explode("observations").alias("o"))
       .select(
           F.col("o.date").alias("observation_date"),
           F.col("o.value").alias("rate_value"))
       # FRED uses "." for missing values; null them out.
       .withColumn("rate_value",
                   F.when(F.col("rate_value") == ".", None)
                    .otherwise(F.col("rate_value").cast("double")))
       .withColumn("observation_date", F.col("observation_date").cast("date"))
       .withColumn("series_id", F.lit("MORTGAGE30US"))
       .withColumn("_ingested_at", F.current_timestamp())
       .withColumn("_source_file", F.lit(SOURCE_FILE)))

# COMMAND ----------

(obs.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.fred_mortgage_30yr"))

# COMMAND ----------

# Validation — most recent rates.
display(
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.fred_mortgage_30yr")
    .orderBy(F.col("observation_date").desc())
    .limit(10))
