# Databricks notebook source
# MAGIC %md
# MAGIC # 40 - Gold: county_market_monthly
# MAGIC
# MAGIC **Role:** Primary analytical mart. One row per (county, month).
# MAGIC Combines home_prices + market_activity into a single wide table.
# MAGIC
# MAGIC **Source priority for prices:**
# MAGIC - median_sale_price: Redfin > Realtor > FHFA
# MAGIC - median_list_price: Realtor > Redfin
# MAGIC - hpi_index: FHFA only
# MAGIC
# MAGIC **Derived columns:**
# MAGIC - list_vs_sale_gap: list - sale price
# MAGIC - months_of_supply: prefer Redfin-reported; fallback = active / new listings
# MAGIC - price_mom: month-over-month % change in median_sale_price
# MAGIC - price_yoy: year-over-year % change in median_sale_price

# COMMAND ----------

from pyspark.sql import functions as F, Window

CATALOG     = "workspace"
GOLD_SCHEMA = "gold"
SILVER      = f"{CATALOG}.silver"
TARGET      = f"{CATALOG}.{GOLD_SCHEMA}.county_market_monthly"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------

base = spark.sql(f"""
WITH prices AS (
    SELECT
        county_fips,
        date_key,
        MAX(CASE WHEN source = 'redfin'  THEN median_sale_price END) AS sale_redfin,
        MAX(CASE WHEN source = 'realtor' THEN median_sale_price END) AS sale_realtor,
        MAX(CASE WHEN source = 'fhfa'    THEN median_sale_price END) AS sale_fhfa,
        MAX(CASE WHEN source = 'redfin'  THEN median_list_price END) AS list_redfin,
        MAX(CASE WHEN source = 'realtor' THEN median_list_price END) AS list_realtor,
        MAX(CASE WHEN source = 'fhfa'    THEN hpi_index          END) AS hpi_index
    FROM {SILVER}.home_prices
    GROUP BY county_fips, date_key
),
activity AS (
    SELECT
        county_fips,
        date_key,
        AVG(active_listings)     AS active_listings,
        AVG(new_listings)        AS new_listings,
        AVG(pending_listings)    AS pending_listings,
        AVG(median_dom)          AS median_dom,
        AVG(sale_to_list_ratio)  AS sale_to_list_ratio,
        AVG(sold_above_list_pct) AS sold_above_list_pct,
        AVG(price_per_sqft)      AS price_per_sqft,
        AVG(months_of_supply)    AS months_of_supply_src
    FROM {SILVER}.market_activity
    GROUP BY county_fips, date_key
)
SELECT
    p.county_fips,
    p.date_key,
    COALESCE(p.sale_redfin,  p.sale_realtor,  p.sale_fhfa)  AS median_sale_price,
    COALESCE(p.list_realtor, p.list_redfin)                  AS median_list_price,
    p.hpi_index,
    ROUND(
        COALESCE(p.list_realtor, p.list_redfin)
        - COALESCE(p.sale_redfin, p.sale_realtor), 2
    )                                                        AS list_vs_sale_gap,
    CAST(a.active_listings  AS BIGINT)                       AS active_listings,
    CAST(a.new_listings     AS BIGINT)                       AS new_listings,
    CAST(a.pending_listings AS BIGINT)                       AS pending_listings,
    ROUND(
        COALESCE(a.months_of_supply_src,
            CASE WHEN a.new_listings > 0
                 THEN a.active_listings / a.new_listings
            END), 2
    )                                                        AS months_of_supply,
    ROUND(a.median_dom,            1)                        AS median_dom,
    ROUND(a.sale_to_list_ratio,    4)                        AS sale_to_list_ratio,
    ROUND(a.sold_above_list_pct,   2)                        AS sold_above_list_pct,
    ROUND(a.price_per_sqft,        2)                        AS price_per_sqft
FROM prices p
LEFT JOIN activity a USING (county_fips, date_key)
WHERE p.county_fips IS NOT NULL
  AND p.date_key    IS NOT NULL
""")

# COMMAND ----------

# Window-based price change metrics — requires ordered history per county
w = Window.partitionBy("county_fips").orderBy("date_key")

mart = (base
    .withColumn("price_mom",
        F.round(
            (F.col("median_sale_price") - F.lag("median_sale_price", 1).over(w))
            / F.lag("median_sale_price", 1).over(w) * 100, 2))
    .withColumn("price_yoy",
        F.round(
            (F.col("median_sale_price") - F.lag("median_sale_price", 12).over(w))
            / F.lag("median_sale_price", 12).over(w) * 100, 2))
    .withColumn("_updated_at", F.current_timestamp()))

# COMMAND ----------

(mart.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(TARGET))

count = spark.table(TARGET).count()
print(f"Rows written: {count:,}")
display(spark.table(TARGET).orderBy("county_fips", "date_key").limit(24))
