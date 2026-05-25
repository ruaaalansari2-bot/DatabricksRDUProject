# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - Bronze: Census ACS Housing Variables
# MAGIC
# MAGIC **Layer:** Bronze
# MAGIC **Source:** Census ACS 5-year estimates for NC counties.
# MAGIC **Variables:**
# MAGIC - B25077 — Median home value (owner-occupied)
# MAGIC - B25064 — Median gross rent
# MAGIC - B25003 — Tenure (total, owner, renter occupied units)
# MAGIC - B01003 — Total population

# COMMAND ----------

import json
from pyspark.sql import functions as F

CATALOG = "workspace"
BRONZE_SCHEMA = "bronze"
SOURCE_FILE = "/Volumes/workspace/landing/raw/census/acs_housing_nc.json"

RDU_COUNTY_FIPS_3 = ["183", "063", "135", "101", "037", "069", "077", "145"]

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

with open(SOURCE_FILE, "r") as f:
    payload = json.load(f)

header, *rows = payload
print("Columns:", header)
print("Total NC counties:", len(rows))

# COMMAND ----------

df = spark.createDataFrame(rows, schema=header)

bronze = (df
          .withColumnRenamed("B25077_001E", "median_home_value")
          .withColumnRenamed("B25064_001E", "median_gross_rent")
          .withColumnRenamed("B25003_001E", "total_occupied_units")
          .withColumnRenamed("B25003_002E", "owner_occupied_units")
          .withColumnRenamed("B25003_003E", "renter_occupied_units")
          .withColumnRenamed("B01003_001E", "total_population")
          .withColumn("median_home_value",     F.col("median_home_value").cast("double"))
          .withColumn("median_gross_rent",     F.col("median_gross_rent").cast("double"))
          .withColumn("total_occupied_units",  F.col("total_occupied_units").cast("long"))
          .withColumn("owner_occupied_units",  F.col("owner_occupied_units").cast("long"))
          .withColumn("renter_occupied_units", F.col("renter_occupied_units").cast("long"))
          .withColumn("total_population",      F.col("total_population").cast("long"))
          .withColumn("fips_code", F.concat(F.col("state"), F.col("county")))
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_source_file", F.lit(SOURCE_FILE)))

# COMMAND ----------

(bronze.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.census_acs_housing"))

# COMMAND ----------

# Validation — RDU counties ranked by median home value.
display(
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.census_acs_housing")
    .filter(F.col("county").isin(RDU_COUNTY_FIPS_3))
    .select("NAME", "fips_code", "median_home_value", "median_gross_rent",
            "owner_occupied_units", "renter_occupied_units", "total_population")
    .orderBy(F.col("median_home_value").desc()))
