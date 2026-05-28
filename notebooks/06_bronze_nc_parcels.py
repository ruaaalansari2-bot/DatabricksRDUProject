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
# MAGIC ## STEP 2 — Column map (confirmed from NC OneMap field inspection)
# MAGIC
# MAGIC NC OneMap ICDE schema confirmed fields:
# MAGIC   stcntyfips = 5-digit county FIPS (e.g. 37063)
# MAGIC   parno      = parcel number (parcel ID)
# MAGIC   parval     = total assessed value  ← this is assessed_value
# MAGIC   landval    = land assessed value
# MAGIC   improvval  = improvement (building) assessed value
# MAGIC   saledate   = last recorded sale date
# MAGIC   parusedesc = parcel use description
# MAGIC   structyear = year built
# MAGIC   siteadd    = site address
# MAGIC   szip       = site ZIP code
# MAGIC
# MAGIC NOTE: There is NO sale price field in NC OneMap.
# MAGIC sale_price will be NULL for all non-Wake records.
# MAGIC These records contribute parcel_median_assessed to the gold layer only.

# COMMAND ----------

COLUMN_MAP = {
    # our_name            : "Exact NC OneMap field name"
    "parcel_id":            "parno",
    "county_fips":          "stcntyfips",   # 5-digit: 37063, 37135, etc.
    "sale_price":           None,           # NOT available in NC OneMap
    "sale_date":            "saledate",
    "assessed_value":       "parval",       # total assessed value
    "land_assessed_value":  "landval",
    "heated_area":          "recareano",    # recorded area (sq ft or acres — verify)
    "bldg_use":             "parusedesc",
    "year_built":           "structyear",
    "address":              "siteadd",
    "zip":                  "szip",
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
