# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Bronze: Wake County Qualified Sales (CSV from volume)
# MAGIC
# MAGIC **Layer:** Bronze
# MAGIC **Source:** Wake County qualified (arms-length) sales file. Acquired as
# MAGIC xlsx, converted to CSV by the acquisition layer, landed in the volume.
# MAGIC **Scope:** Wake County only (FIPS 37183) — this is the parcel-level
# MAGIC transaction ground-truth source.
# MAGIC
# MAGIC ### Why this source matters
# MAGIC Wake pre-filters to "qualified" sales — arms-length transactions, with
# MAGIC foreclosures, related-party, and partial-interest sales removed. That is
# MAGIC clean transaction-price ground truth, and the basis for the
# MAGIC assessor-vs-market gold mart.
# MAGIC
# MAGIC ### Column names are UNKNOWN until you look
# MAGIC Wake's file column names are not guessable. **Run the inspection cell
# MAGIC FIRST**, read the real column names, then set the COLUMN MAP below to
# MAGIC match before running the write cell.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
BRONZE_SCHEMA = "bronze"
SOURCE_FILE = "/Volumes/workspace/landing/raw/wake/qualified_sales.csv"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP 1 — Inspect (run this, read output, do not skip)

# COMMAND ----------

raw = (spark.read
       .option("header", "true")
       .option("inferSchema", "true")
       .csv(SOURCE_FILE))

print("=== Wake file columns ===")
for c in raw.columns:
    print(" ", c)
print(f"\nRow count: {raw.count()}")
display(raw.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP 2 — Map the real columns
# MAGIC
# MAGIC From the inspection above, fill in the actual Wake column names for the
# MAGIC fields we care about. Leave a value as None if the file does not have
# MAGIC it. Common Wake fields include a parcel/PIN id, a sale price, a sale
# MAGIC date, and an address — but use the EXACT names printed above.

# COMMAND ----------

COLUMN_MAP = {
    # our_name        : "Exact Wake column name from STEP 1"
    "parcel_id":        "PIN_NUM",        # <-- adjust
    "sale_price":       "TOTSALPRICE",    # <-- adjust
    "sale_date":        "SALEDATE",       # <-- adjust
    "address":          "SITE_ADDRESS",   # <-- adjust (or None)
    "heated_area":      "HEATEDAREA",     # <-- adjust (or None, for $/sqft)
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP 3 — Build the bronze table
# MAGIC
# MAGIC Selects and renames only the mapped columns, casts types, adds metadata.
# MAGIC Bronze stays close to raw: light typing only, no business logic.

# COMMAND ----------

select_exprs = []
for our_name, wake_name in COLUMN_MAP.items():
    if wake_name is not None and wake_name in raw.columns:
        select_exprs.append(F.col(wake_name).alias(our_name))
    else:
        # Keep the column present but null if not found, so downstream code
        # has a stable schema regardless of file variations.
        select_exprs.append(F.lit(None).cast("string").alias(our_name))

bronze = raw.select(*select_exprs)

# Light typing. Sale price often has $ and commas if it came through as text.
bronze = (bronze
          .withColumn("sale_price",
                      F.regexp_replace(F.col("sale_price").cast("string"),
                                       "[^0-9.]", "").cast("double"))
          .withColumn("sale_date", F.to_date(F.col("sale_date")))
          .withColumn("county_fips", F.lit("37183"))
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_source_file", F.lit(SOURCE_FILE)))

(bronze.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.wake_qualified_sales"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC Should show recent qualified sales with plausible prices. Sanity check:
# MAGIC prices should mostly fall in a believable range for Wake County homes.

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.wake_qualified_sales")
    .filter(F.col("sale_price").isNotNull())
    .orderBy(F.col("sale_date").desc())
    .select("parcel_id", "sale_date", "sale_price", "address")
    .limit(20))

# COMMAND ----------

# Quick distribution check — median and range of sale prices.
display(
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.wake_qualified_sales")
    .filter(F.col("sale_price").isNotNull())
    .selectExpr(
        "count(*) as n_sales",
        "percentile_approx(sale_price, 0.5) as median_price",
        "min(sale_price) as min_price",
        "max(sale_price) as max_price"))
