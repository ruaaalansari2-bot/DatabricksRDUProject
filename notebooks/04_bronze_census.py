# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Bronze: Census ACS Median Household Income (JSON from volume)
# MAGIC
# MAGIC **Layer:** Bronze
# MAGIC **Source:** Census ACS 5-year, median household income by NC county,
# MAGIC landed in the volume by the acquisition layer.
# MAGIC **Scope:** filtered to the 8 RDU counties.
# MAGIC
# MAGIC ### Census JSON shape (important — differs from FRED)
# MAGIC The Census API returns an **array of arrays**, where the FIRST inner
# MAGIC array is the header and the rest are data rows:
# MAGIC ```
# MAGIC [["NAME","B19013_001E","state","county"],
# MAGIC  ["Wake County, North Carolina","104819","37","183"],
# MAGIC  ...]
# MAGIC ```
# MAGIC So we read it as raw text, parse with Python's json, split header from
# MAGIC rows, and build a DataFrame. (Spark's .json() expects objects, not this
# MAGIC headerless array-of-arrays, so we parse it ourselves.)

# COMMAND ----------

import json
from pyspark.sql import functions as F

CATALOG = "workspace"
BRONZE_SCHEMA = "bronze"
SOURCE_FILE = "/Volumes/workspace/landing/raw/census/acs_income_nc.json"

# RDU county FIPS (3-digit county part; Census returns state=37 separately).
RDU_COUNTY_FIPS_3 = ["183", "063", "135", "101", "037", "069", "077", "145"]

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# Read the raw file content. On serverless, read the volume file directly.
with open(SOURCE_FILE, "r") as f:
    payload = json.load(f)

header, *rows = payload          # first element is the header row
print("Columns returned by Census:", header)
print("Total rows:", len(rows))

# COMMAND ----------

# Build a Spark DataFrame from the parsed rows using the header as column names.
df = spark.createDataFrame(rows, schema=header)

# Census numeric fields come back as strings; cast the income measure.
# B19013_001E = median household income.
df = (df
      .withColumnRenamed("B19013_001E", "median_household_income")
      .withColumn("median_household_income",
                  F.col("median_household_income").cast("double"))
      .withColumn("_ingested_at", F.current_timestamp())
      .withColumn("_source_file", F.lit(SOURCE_FILE)))

# COMMAND ----------

# Filter to the 8 RDU counties. Census returns a 'county' column (3-digit).
bronze = df.filter(F.col("county").isin(RDU_COUNTY_FIPS_3))

(bronze.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.census_acs_income"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC Should show 8 rows — one per RDU county — with a plausible income value
# MAGIC (NC county medians are roughly $50k–$110k).

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.census_acs_income")
    .select("NAME", "state", "county", "median_household_income")
    .orderBy(F.col("median_household_income").desc()))
