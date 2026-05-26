# Databricks notebook source
# MAGIC %md
# MAGIC # 45 - Gold: assessor_vs_market
# MAGIC
# MAGIC **Role:** Compare actual transaction prices (Wake qualified sales) against
# MAGIC the broader market indices from Redfin/Realtor. Surfaces gaps between
# MAGIC assessor-reported activity and listed/indexed market values.
# MAGIC
# MAGIC **Grain:** county x month (parcel_sales aggregated to county-month median)
# MAGIC
# MAGIC **Columns:**
# MAGIC - parcel_sale_count    — number of qualifying (non-outlier) transactions
# MAGIC - parcel_median_price  — median sale price from Wake qualified sales file
# MAGIC - parcel_median_ppsf   — median price per sqft from Wake sales
# MAGIC - market_median_price  — median sale price from Redfin/Realtor (silver.home_prices)
# MAGIC - hpi_index            — FHFA HPI index for same county+month
# MAGIC - parcel_vs_market_gap — parcel_median − market_median ($ diff)
# MAGIC - parcel_vs_market_gap_pct — same as % of market price
# MAGIC - assessed_vs_sale_gap — NULL until Wake property file (assessed_value) added
# MAGIC
# MAGIC **Note:** Wake County is county_fips 37183. Other counties have no parcel
# MAGIC data yet; rows for them will show NULL on all parcel_ columns.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
GOLD    = f"{CATALOG}.gold"
SILVER  = f"{CATALOG}.silver"
TARGET  = f"{GOLD}.assessor_vs_market"

# COMMAND ----------

avm = spark.sql(f"""
WITH parcel_agg AS (
    SELECT
        county_fips,
        DATE_TRUNC('month', CAST(sale_date AS DATE))         AS date_key,
        COUNT(*)                                             AS parcel_sale_count,
        ROUND(PERCENTILE_APPROX(sale_price,    0.5), 0)     AS parcel_median_price,
        ROUND(PERCENTILE_APPROX(price_per_sqft,0.5), 2)     AS parcel_median_ppsf,
        SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END)         AS outlier_count
    FROM {SILVER}.parcel_sales
    WHERE NOT is_outlier
      AND sale_price > 0
    GROUP BY county_fips, DATE_TRUNC('month', CAST(sale_date AS DATE))
),
market AS (
    SELECT
        county_fips,
        date_key,
        COALESCE(
            MAX(CASE WHEN source = 'redfin'  THEN median_sale_price END),
            MAX(CASE WHEN source = 'realtor' THEN median_sale_price END)
        )                                                    AS market_median_price,
        MAX(CASE WHEN source = 'fhfa' THEN hpi_index END)   AS hpi_index
    FROM {SILVER}.home_prices
    GROUP BY county_fips, date_key
)
SELECT
    m.county_fips,
    m.date_key,
    p.parcel_sale_count,
    p.parcel_median_price,
    p.parcel_median_ppsf,
    p.outlier_count,
    m.market_median_price,
    m.hpi_index,
    ROUND(p.parcel_median_price - m.market_median_price, 2) AS parcel_vs_market_gap,
    CASE WHEN m.market_median_price > 0 THEN
        ROUND(
            (p.parcel_median_price - m.market_median_price)
            / m.market_median_price * 100, 2)
    END                                                     AS parcel_vs_market_gap_pct,
    -- populated once Wake property file is added as a bronze source
    CAST(NULL AS DOUBLE)                                    AS assessed_vs_sale_gap
FROM market m
LEFT JOIN parcel_agg p USING (county_fips, date_key)
""")

# COMMAND ----------

(avm
 .withColumn("_updated_at", F.current_timestamp())
 .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(TARGET))

count         = spark.table(TARGET).count()
with_parcel   = spark.table(TARGET).filter(F.col("parcel_sale_count").isNotNull()).count()
print(f"Rows written: {count:,}  |  Rows with parcel data: {with_parcel:,}")

display(
    spark.table(TARGET)
    .filter(F.col("parcel_median_price").isNotNull())
    .orderBy(F.col("date_key").desc())
    .limit(20))
