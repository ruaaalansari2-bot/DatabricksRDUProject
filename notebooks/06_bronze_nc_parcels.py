# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Bronze: NC OneMap Parcel Sales (non-Wake RDU counties)
# MAGIC
# MAGIC **Layer:** Bronze
# MAGIC **Source:** NC Integrated Cadastral Data Exchange via NC OneMap FeatureServer.
# MAGIC Covers Durham (37063), Orange (37135), Johnston (37101), Chatham (37037),
# MAGIC Franklin (37069), Granville (37077), Person (37145).
# MAGIC
# MAGIC **Why not Wake?** Wake County publishes its own qualified-sales extract
# MAGIC (notebook 05). NC OneMap is used only for the remaining RDU counties.
# MAGIC
# MAGIC ### Column names must be verified first
# MAGIC NC OneMap field names are published by NCCGIA but may differ between
# MAGIC dataset vintages. **Run the inspection cell first**, confirm field names,
# MAGIC then fill in COLUMN_MAP before running the write cell.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG       = "workspace"
BRONZE_SCHEMA = "bronze"
SOURCE_FILE   = "/Volumes/workspace/landing/raw/nc_parcels/rdu_non_wake.geojson"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP 1 — Inspect (run this, read output, do not skip)

# COMMAND ----------

raw = (spark.read
       .option("multiline", "true")
       .format("json")
       .load(SOURCE_FILE))

# GeoJSON FeatureServer response — properties are nested under "features.properties"
from pyspark.sql.functions import col, explode

features = raw.select(explode(col("features")).alias("feat"))
props = features.select(col("feat.properties.*"))

print("=== NC OneMap parcel columns ===")
for c in props.columns:
    print(" ", c)
print(f"\nRow count: {props.count()}")
display(props.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP 2 — Map the real columns
# MAGIC
# MAGIC From the inspection above, fill in the actual NC OneMap field names.
# MAGIC Common field names in the ICDE dataset (but VERIFY against inspection output):
# MAGIC   FIPS, PARCELPK, REID, SITEADDR, SITECITY, SITEZIP,
# MAGIC   SALEPRICE, SALEDATE, TOTVAL, LANDVAL, BLDGVAL,
# MAGIC   HEATAREA, BLDGUSE, YEARBUILT, OWNER

# COMMAND ----------

COLUMN_MAP = {
    # our_name          : "Exact NC OneMap field name"
    "parcel_id":          "REID",          # or PARCELPK — check inspection
    "county_fips":        "FIPS",          # 5-digit: 37063 etc.
    "sale_price":         "SALEPRICE",
    "sale_date":          "SALEDATE",
    "assessed_value":     "TOTVAL",
    "land_assessed_value":"LANDVAL",
    "heated_area":        "HEATAREA",
    "bldg_use":           "BLDGUSE",
    "year_built":         "YEARBUILT",
    "address":            "SITEADDR",
    "zip":                "SITEZIP",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP 3 — Build the bronze table

# COMMAND ----------

select_exprs = []
for our_name, nc_name in COLUMN_MAP.items():
    if nc_name is not None and nc_name in props.columns:
        select_exprs.append(F.col(nc_name).alias(our_name))
    else:
        select_exprs.append(F.lit(None).cast("string").alias(our_name))

bronze = props.select(*select_exprs)

bronze = (bronze
          .withColumn("sale_price",
                      F.regexp_replace(F.col("sale_price").cast("string"),
                                       "[^0-9.]", "").cast("double"))
          .withColumn("assessed_value",
                      F.regexp_replace(F.col("assessed_value").cast("string"),
                                       "[^0-9.]", "").cast("double"))
          .withColumn("land_assessed_value",
                      F.regexp_replace(F.col("land_assessed_value").cast("string"),
                                       "[^0-9.]", "").cast("double"))
          .withColumn("heated_area",    F.col("heated_area").cast("double"))
          .withColumn("year_built",     F.col("year_built").cast("integer"))
          .withColumn("sale_date",      F.to_date(F.col("sale_date")))
          .withColumn("_ingested_at",   F.current_timestamp())
          .withColumn("_source_file",   F.lit(SOURCE_FILE)))

(bronze.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.nc_parcel_sales"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.nc_parcel_sales")
    .filter(F.col("sale_price").isNotNull())
    .orderBy(F.col("sale_date").desc())
    .select("county_fips", "parcel_id", "sale_date", "sale_price",
            "assessed_value", "address")
    .limit(20))

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.nc_parcel_sales")
    .filter(F.col("sale_price").isNotNull())
    .groupBy("county_fips")
    .agg(
        F.count("*").alias("n_sales"),
        F.round(F.expr("percentile_approx(sale_price, 0.5)"), 0).alias("median_price"),
        F.min("sale_date").alias("earliest_sale"),
        F.max("sale_date").alias("latest_sale"),
    )
    .orderBy("county_fips"))
