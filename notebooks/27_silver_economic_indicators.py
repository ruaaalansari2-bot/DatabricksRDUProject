# Databricks notebook source
# MAGIC %md
# MAGIC # 27 - Silver: economic_indicators
# MAGIC
# MAGIC **Role:** Macro and regional economic series conformed to monthly grain,
# MAGIC stored in long format (one row per indicator per month).
# MAGIC
# MAGIC **geography_scope:** 'national' for FRED series; county_fips for BLS LAUS.
# MAGIC
# MAGIC **Monthly rule for weekly FRED rates:** average of all weekly observations
# MAGIC whose date falls within that calendar month.
# MAGIC
# MAGIC **Indicators:**
# MAGIC   mortgage_30yr, mortgage_15yr — national weekly rates, averaged to month
# MAGIC   housing_starts, housing_starts_sfr — national monthly (HOUST, HOUST1F)
# MAGIC   nc_building_permits — NC state monthly (NCBPPRIVSA)
# MAGIC   nc_unemployment — NC state monthly (NCUR)
# MAGIC   nc_median_listing_price — NC monthly (MEDLISPRINC)
# MAGIC   county_unemployment — BLS LAUS, per county monthly

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG       = "workspace"
SILVER_SCHEMA = "silver"
BRONZE_SCHEMA = "bronze"

FRED_SERIES_TO_INDICATOR = {
    "MORTGAGE30US": "mortgage_30yr",
    "MORTGAGE15US": "mortgage_15yr",
    "HOUST":        "housing_starts",
    "HOUST1F":      "housing_starts_sfr",
    "NCBPPRIVSA":   "nc_building_permits",
    "NCUR":         "nc_unemployment",
    "MEDLISPRINC":  "nc_median_listing_price",
}

# COMMAND ----------

# ── 1. FRED 30yr (weekly → monthly avg from fred_mortgage_30yr table) ─────────
fred_30yr = (spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.fred_mortgage_30yr")
             .filter(F.col("rate_value").isNotNull())
             .withColumn("date_key", F.date_trunc("month", F.col("observation_date")).cast("date"))
             .groupBy("date_key")
             .agg(F.avg("rate_value").alias("value"))
             .withColumn("indicator",        F.lit("mortgage_30yr"))
             .withColumn("geography_scope",  F.lit("national")))

# ── 2. All other FRED series (from fred_indicators table) ─────────────────────
indicator_map_expr = F.create_map(
    *[x for pair in [(F.lit(k), F.lit(v)) for k, v in FRED_SERIES_TO_INDICATOR.items()] for x in pair]
)

fred_others = (spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.fred_indicators")
               .filter(F.col("value").isNotNull())
               .filter(F.col("series_id") != "MORTGAGE30US")  # already in fred_30yr above
               .withColumn("date_key",
                           F.date_trunc("month", F.col("observation_date")).cast("date"))
               .groupBy("series_id", "date_key")
               .agg(F.avg("value").alias("value"))
               .withColumn("indicator",       indicator_map_expr[F.col("series_id")])
               .withColumn("geography_scope", F.lit("national"))
               .select("date_key", "indicator", "geography_scope", "value"))

# ── 3. BLS LAUS — county unemployment (already monthly) ───────────────────────
bls = (spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.bls_laus_rdu")
       .filter(F.col("unemployment_rate").isNotNull())
       .withColumn("date_key",
                   F.date_trunc("month", F.col("observation_date")).cast("date"))
       .withColumn("indicator",       F.lit("county_unemployment"))
       .withColumn("geography_scope", F.col("county_fips"))
       .select("date_key", "indicator", "geography_scope",
               F.col("unemployment_rate").alias("value")))

# ── Union all ─────────────────────────────────────────────────────────────────
fred_30yr_sel = fred_30yr.select("date_key", "indicator", "geography_scope", "value")

indicators = (fred_30yr_sel
              .union(fred_others)
              .union(bls)
              .filter(F.col("indicator").isNotNull())
              .withColumn("_ingested_at", F.current_timestamp()))

# COMMAND ----------

(indicators.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.economic_indicators"))

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.economic_indicators")
    .groupBy("indicator", "geography_scope")
    .agg(F.count("*").alias("months"),
         F.max("date_key").alias("latest_date"),
         F.round(F.avg("value"), 2).alias("avg_value"))
    .orderBy("indicator", "geography_scope"))
