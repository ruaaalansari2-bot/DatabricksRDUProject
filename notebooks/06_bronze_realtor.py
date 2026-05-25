# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Bronze: Realtor.com Inventory Metrics (County + ZIP)
# MAGIC
# MAGIC **Layer:** Bronze
# MAGIC **Source:** Realtor.com Research public S3 CSVs — county and ZIP level
# MAGIC inventory core metrics (median listing price, active listings, days on
# MAGIC market, price per sqft, new listings, price reductions).
# MAGIC **Scope:** Filtered to NC after read. Full history in one file.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
BRONZE_SCHEMA = "bronze"

SOURCE_COUNTY = "/Volumes/workspace/landing/raw/realtor/inventory_county.csv"
SOURCE_ZIP    = "/Volumes/workspace/landing/raw/realtor/inventory_zip.csv"

NC_FIPS_PREFIX = "37"   # all NC county FIPS start with 37

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# County table — filter to NC via county_fips prefix.
county_raw = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(SOURCE_COUNTY))

county_bronze = (county_raw
                 .filter(F.col("county_fips").cast("string").startswith(NC_FIPS_PREFIX))
                 .withColumn("_ingested_at", F.current_timestamp())
                 .withColumn("_source_file", F.lit(SOURCE_COUNTY)))

(county_bronze.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.realtor_inventory_county"))

# COMMAND ----------

# ZIP table — filter to NC via state field.
zip_raw = (spark.read
           .option("header", "true")
           .option("inferSchema", "true")
           .csv(SOURCE_ZIP))

zip_bronze = (zip_raw
              .filter(F.col("state_id") == "NC")
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.lit(SOURCE_ZIP)))

(zip_bronze.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.realtor_inventory_zip"))

# COMMAND ----------

# Validation
display(
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.realtor_inventory_county")
    .select("county_fips", "county_name", "month_date_yyyymm",
            "median_listing_price", "active_listing_count", "median_days_on_market")
    .orderBy("county_fips", "month_date_yyyymm")
    .limit(20))
