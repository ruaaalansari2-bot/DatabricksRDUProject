# Databricks notebook source
# MAGIC %md
# MAGIC # 29 - Silver: parcel_sales
# MAGIC
# MAGIC **Role:** Wake County qualified (arms-length) sales — transaction ground truth.
# MAGIC Cleaned here: deduplicated, typed, outliers flagged.
# MAGIC
# MAGIC **Cleaning rules applied:**
# MAGIC - Deduplicate on (parcel_id, sale_date, sale_price) — Wake file can repeat rows.
# MAGIC - Cast heated_area to double; null if non-numeric.
# MAGIC - Compute price_per_sqft where both sale_price and heated_area > 0.
# MAGIC - Flag is_outlier = true where sale_price <= 0, sale_price > 10M,
# MAGIC   or price_per_sqft > 2000 (implausible for residential Wake County).
# MAGIC
# MAGIC **Note:** assessed_value is NOT present in the Wake qualified sales file.
# MAGIC The assessor_vs_market gold mart will need the separate Wake property file
# MAGIC added as a bronze source before that column can be populated.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG       = "workspace"
SILVER_SCHEMA = "silver"
BRONZE_SCHEMA = "bronze"

PRICE_MIN     =       1
PRICE_MAX     = 10_000_000
PPSF_MAX      =     2_000

# COMMAND ----------

raw = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.wake_qualified_sales")

cleaned = (raw
           .dropDuplicates(["parcel_id", "sale_date", "sale_price"])
           .filter(F.col("sale_price").isNotNull())
           .filter(F.col("sale_date").isNotNull())
           .withColumn("heated_area_sqft",
                       F.col("heated_area").cast("double"))
           .withColumn("price_per_sqft",
                       F.when(
                           (F.col("heated_area_sqft") > 0) & (F.col("sale_price") > 0),
                           F.round(F.col("sale_price") / F.col("heated_area_sqft"), 2)
                       ).otherwise(None))
           .withColumn("is_outlier",
                       (F.col("sale_price") <= PRICE_MIN) |
                       (F.col("sale_price") > PRICE_MAX) |
                       (F.col("price_per_sqft") > PPSF_MAX))
           .withColumn("assessed_vs_sale_gap",
                       F.when(
                           F.col("assessed_value").isNotNull() & (F.col("sale_price") > 0),
                           F.round(F.col("assessed_value") - F.col("sale_price"), 2)
                       ).otherwise(None))
           .withColumn("_ingested_at", F.current_timestamp())
           .select(
               F.col("parcel_id").cast("string"),
               "county_fips",
               "sale_date",
               "sale_price",
               F.col("assessed_value").cast("double"),
               "assessed_vs_sale_gap",
               "heated_area_sqft",
               "price_per_sqft",
               "address",
               F.col("sale_type").cast("string"),
               F.col("year_built").cast("integer"),
               F.col("bldg_use").cast("string"),
               F.col("zip").cast("string"),
               "is_outlier",
               "_ingested_at"))

# COMMAND ----------

(cleaned.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.parcel_sales"))

# COMMAND ----------

total    = cleaned.count()
outliers = cleaned.filter(F.col("is_outlier")).count()
pct      = (100 * outliers / total) if total > 0 else 0.0
print(f"Total rows: {total:,}  |  Outliers flagged: {outliers:,}  ({pct:.1f}%)")

display(
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.parcel_sales")
    .filter(~F.col("is_outlier"))
    .orderBy(F.col("sale_date").desc())
    .limit(15))
