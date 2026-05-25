# Databricks notebook source
# MAGIC %md
# MAGIC # 24 - Silver: market_activity
# MAGIC
# MAGIC **Role:** Inventory and velocity facts from Redfin and Realtor.com,
# MAGIC conformed to county + monthly grain, source-tagged.
# MAGIC
# MAGIC **Schema:** county_fips, date_key, source, active_listings, new_listings,
# MAGIC             pending_listings, median_dom, sale_to_list_ratio,
# MAGIC             sold_above_list_pct, price_per_sqft, months_of_supply

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG       = "workspace"
SILVER_SCHEMA = "silver"
BRONZE_SCHEMA = "bronze"

RDU_FIPS = ["37183","37063","37135","37101","37037","37069","37077","37145"]

REDFIN_REGION_MAP = {
    "Wake County, NC":      "37183",
    "Durham County, NC":    "37063",
    "Orange County, NC":    "37135",
    "Johnston County, NC":  "37101",
    "Chatham County, NC":   "37037",
    "Franklin County, NC":  "37069",
    "Granville County, NC": "37077",
    "Person County, NC":    "37145",
}

# COMMAND ----------

region_map_expr = F.create_map(
    *[x for pair in [(F.lit(k), F.lit(v)) for k, v in REDFIN_REGION_MAP.items()] for x in pair]
)

# ── 1. Redfin (weekly → monthly) ─────────────────────────────────────────────
redfin = (spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.redfin_county_tracker")
          .filter(F.col("STATE_CODE") == "NC")
          .filter(F.col("PROPERTY_TYPE") == "All Residential")
          .withColumn("county_fips", region_map_expr[F.col("REGION")])
          .filter(F.col("county_fips").isNotNull())
          .withColumn("date_key", F.date_trunc("month", F.col("PERIOD_BEGIN")).cast("date"))
          .groupBy("county_fips", "date_key")
          .agg(
              F.avg(F.col("INVENTORY").cast("double")).alias("active_listings"),
              F.avg(F.col("NEW_LISTINGS").cast("double")).alias("new_listings"),
              F.avg(F.col("PENDING_SALES").cast("double")).alias("pending_listings"),
              F.avg(F.col("MEDIAN_DOM").cast("double")).alias("median_dom"),
              F.avg(F.col("AVG_SALE_TO_LIST").cast("double")).alias("sale_to_list_ratio"),
              F.avg(F.col("SOLD_ABOVE_LIST").cast("double")).alias("sold_above_list_pct"),
              F.avg(F.col("MEDIAN_PPSF").cast("double")).alias("price_per_sqft"),
              F.avg(F.col("MONTHS_OF_SUPPLY").cast("double")).alias("months_of_supply"),
          )
          .withColumn("source", F.lit("redfin"))
          .select("county_fips", "date_key", "source",
                  "active_listings", "new_listings", "pending_listings",
                  "median_dom", "sale_to_list_ratio", "sold_above_list_pct",
                  "price_per_sqft", "months_of_supply"))

# ── 2. Realtor.com (native monthly) ──────────────────────────────────────────
realtor = (spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.realtor_inventory_county")
           .filter(F.lpad(F.col("county_fips").cast("string"), 5, "0").isin(RDU_FIPS))
           .withColumn("county_fips",
                       F.lpad(F.col("county_fips").cast("string"), 5, "0"))
           .withColumn("date_key",
                       F.to_date(F.concat(
                           F.substring(F.col("month_date_yyyymm").cast("string"), 1, 4),
                           F.lit("-"),
                           F.substring(F.col("month_date_yyyymm").cast("string"), 5, 2),
                           F.lit("-01"))))
           .withColumn("source",            F.lit("realtor"))
           .withColumn("pending_listings",  F.col("pending_listing_count"))
           .withColumn("sale_to_list_ratio",F.lit(None).cast("double"))
           .withColumn("sold_above_list_pct",F.lit(None).cast("double"))
           .withColumn("months_of_supply",  F.lit(None).cast("double"))
           .select("county_fips", "date_key", "source",
                   F.col("active_listing_count").alias("active_listings"),
                   F.col("new_listing_count").alias("new_listings"),
                   "pending_listings",
                   F.col("median_days_on_market").alias("median_dom"),
                   "sale_to_list_ratio", "sold_above_list_pct",
                   F.col("median_listing_price_per_square_foot").alias("price_per_sqft"),
                   "months_of_supply"))

# ── Union ─────────────────────────────────────────────────────────────────────
market_activity = (redfin.union(realtor)
                   .withColumn("_ingested_at", F.current_timestamp()))

# COMMAND ----------

(market_activity.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.market_activity"))

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.market_activity")
    .filter(F.col("county_fips") == "37183")
    .orderBy("source", F.col("date_key").desc())
    .limit(12))
