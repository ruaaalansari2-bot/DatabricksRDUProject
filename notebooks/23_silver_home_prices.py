# Databricks notebook source
# MAGIC %md
# MAGIC # 23 - Silver: home_prices
# MAGIC
# MAGIC **Role:** Conformed price facts from three sources in one table, source-tagged.
# MAGIC Keeping all price signals together lets gold compare list vs sale vs index
# MAGIC side-by-side on the same county + month key.
# MAGIC
# MAGIC **Sources & grain decisions:**
# MAGIC - FHFA: annual HPI → forward-filled to 12 monthly rows per year/county.
# MAGIC - Redfin: weekly median sale price → averaged to monthly.
# MAGIC - Realtor.com: monthly median listing price (native monthly, no change needed).
# MAGIC
# MAGIC **Schema:** county_fips, date_key, source, median_sale_price,
# MAGIC             median_list_price, hpi_index, hpi_annual_change_pct

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG       = "workspace"
SILVER_SCHEMA = "silver"
BRONZE_SCHEMA = "bronze"

# Syracuse MSA County FIPS codes (New York state code 36 + county codes)
SYRACUSE_FIPS = ["36067",  # Onondaga County (Syracuse city)
                 "36053",  # Madison County
                 "36075"]  # Oswego County

# county_fips crosswalk: Redfin REGION string → 5-digit FIPS
REDFIN_REGION_MAP = {
    "Onondaga County, NY": "36067",
    "Madison County, NY":  "36053",
    "Oswego County, NY":   "36075",
}

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

# ── 1. FHFA ──────────────────────────────────────────────────────────────────
# Annual index → expand to one row per month (forward-fill within each year).
# date_key = first of each month in that year.
fhfa_raw = (spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.fhfa_hpi_county")
            .filter(F.lpad(F.col("fips_code").cast("string"), 5, "0").isin(SYRACUSE_FIPS))
            .select(
                F.lpad(F.col("fips_code").cast("string"), 5, "0").alias("county_fips"),
                F.col("year").cast("int"),
                F.col("hpi").alias("hpi_index"),
                F.col("annual_change_pct").alias("hpi_annual_change_pct")))

# Cross-join each year-county row with months 1-12.
months = spark.range(1, 13).select(F.col("id").cast("int").alias("month"))

fhfa = (fhfa_raw.crossJoin(months)
        .withColumn("date_key",
                    F.to_date(F.concat_ws("-", F.col("year"), F.col("month"), F.lit("1"))))
        .withColumn("source",            F.lit("fhfa"))
        .withColumn("median_sale_price", F.lit(None).cast("double"))
        .withColumn("median_list_price", F.lit(None).cast("double"))
        .select("county_fips", "date_key", "source",
                "median_sale_price", "median_list_price",
                "hpi_index", "hpi_annual_change_pct"))

# ── 2. Redfin ─────────────────────────────────────────────────────────────────
# Weekly → monthly average. Map REGION string → county_fips via broadcast map.
region_map_expr = F.create_map(
    *[x for pair in [(F.lit(k), F.lit(v)) for k, v in REDFIN_REGION_MAP.items()] for x in pair]
)

redfin = (spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.redfin_county_tracker")
          .filter(F.col("STATE_CODE") == "NY")
          .filter(F.col("PROPERTY_TYPE") == "All Residential")
          .withColumn("county_fips", region_map_expr[F.col("REGION")])
          .filter(F.col("county_fips").isNotNull())
          .withColumn("date_key", F.date_trunc("month", F.col("PERIOD_BEGIN")).cast("date"))
          .groupBy("county_fips", "date_key")
          .agg(F.avg(F.col("MEDIAN_SALE_PRICE").cast("double")).alias("median_sale_price"),
               F.avg(F.col("MEDIAN_LIST_PRICE").cast("double")).alias("median_list_price"))
          .withColumn("source",               F.lit("redfin"))
          .withColumn("hpi_index",            F.lit(None).cast("double"))
          .withColumn("hpi_annual_change_pct",F.lit(None).cast("double"))
          .select("county_fips", "date_key", "source",
                  "median_sale_price", "median_list_price",
                  "hpi_index", "hpi_annual_change_pct"))

# ── 3. Realtor.com ────────────────────────────────────────────────────────────
# Native monthly. Convert YYYYMM int → date_key. Filter to Syracuse FIPS.
realtor = (spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.realtor_inventory_county")
           .filter(F.lpad(F.col("county_fips").cast("string"), 5, "0").isin(SYRACUSE_FIPS))
           .withColumn("county_fips",
                       F.lpad(F.col("county_fips").cast("string"), 5, "0"))
           .withColumn("date_key",
                       F.to_date(F.concat(
                           F.substring(F.col("month_date_yyyymm").cast("string"), 1, 4),
                           F.lit("-"),
                           F.substring(F.col("month_date_yyyymm").cast("string"), 5, 2),
                           F.lit("-01"))))
           .withColumn("source",               F.lit("realtor"))
           .withColumn("median_sale_price",     F.lit(None).cast("double"))
           .withColumn("hpi_index",             F.lit(None).cast("double"))
           .withColumn("hpi_annual_change_pct", F.lit(None).cast("double"))
           .select("county_fips", "date_key", "source",
                   "median_sale_price",
                   F.col("median_listing_price").alias("median_list_price"),
                   "hpi_index", "hpi_annual_change_pct"))

# ── Union all three ───────────────────────────────────────────────────────────
home_prices = (fhfa.union(redfin).union(realtor)
               .withColumn("_ingested_at", F.current_timestamp()))

# COMMAND ----------

(home_prices.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.home_prices"))

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.home_prices")
    .filter(F.col("county_fips") == "36067")  # Onondaga County (Syracuse)
    .orderBy("source", F.col("date_key").desc())
    .limit(15))
