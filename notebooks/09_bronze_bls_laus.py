# Databricks notebook source
# MAGIC %md
# MAGIC # 09 - Bronze: BLS LAUS — County Unemployment Rates
# MAGIC
# MAGIC **Layer:** Bronze
# MAGIC **Source:** Bureau of Labor Statistics Local Area Unemployment Statistics.
# MAGIC Unemployment rate (not seasonally adjusted) for all 8 RDU CSA counties.
# MAGIC **Series:** LAUCN{state_fips}{county_fips}0000000003 (measure 03 = rate)

# COMMAND ----------

import json
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

CATALOG = "workspace"
BRONZE_SCHEMA = "bronze"
SOURCE_FILE = "/Volumes/workspace/landing/raw/bls/laus_rdu_counties.json"

SERIES_TO_FIPS = {
    "LAUCN371830000000003": "37183",  # Wake
    "LAUCN370630000000003": "37063",  # Durham
    "LAUCN371350000000003": "37135",  # Orange
    "LAUCN371010000000003": "37101",  # Johnston
    "LAUCN370370000000003": "37037",  # Chatham
    "LAUCN370690000000003": "37069",  # Franklin
    "LAUCN370770000000003": "37077",  # Granville
    "LAUCN371450000000003": "37145",  # Person
}

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

with open(SOURCE_FILE, "r") as f:
    payload = json.load(f)

# BLS returns one Results.series entry per series ID, each with a data array.
rows = []
for series in payload.get("Results", {}).get("series", []):
    series_id = series["seriesID"]
    fips = SERIES_TO_FIPS.get(series_id, "unknown")
    for obs in series.get("data", []):
        rows.append({
            "series_id":          series_id,
            "county_fips":        fips,
            "year":               int(obs["year"]),
            "period":             obs["period"],       # e.g. M01..M12
            "period_name":        obs["periodName"],
            "unemployment_rate":  float(obs["value"]) if obs["value"] != "-" else None,
            "footnote":           obs.get("footnotes", [{}])[0].get("text", None),
        })

print(f"Total observations: {len(rows)}")

# COMMAND ----------

df = spark.createDataFrame(rows)

bronze = (df
          .withColumn("period_month", F.regexp_extract(F.col("period"), r"M(\d+)", 1).cast("int"))
          .withColumn("observation_date",
                      F.to_date(F.concat_ws("-", F.col("year"), F.col("period_month"), F.lit("01"))))
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_source_file", F.lit(SOURCE_FILE)))

# COMMAND ----------

(bronze.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.bls_laus_rdu"))

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.bls_laus_rdu")
    .select("county_fips", "observation_date", "unemployment_rate")
    .orderBy("county_fips", F.col("observation_date").desc())
    .limit(20))
