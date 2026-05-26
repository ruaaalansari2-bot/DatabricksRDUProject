# Databricks notebook source
# MAGIC %md
# MAGIC # 41 - Gold: affordability_monthly
# MAGIC
# MAGIC **Role:** Housing affordability by county x month.
# MAGIC
# MAGIC **Mortgage payment model:**
# MAGIC - 20% down, 30-year fixed, monthly compounding
# MAGIC - est_monthly_payment = (price × 0.80) × r(1+r)^360 / ((1+r)^360 − 1)
# MAGIC   where r = mortgage_rate / 100 / 12
# MAGIC
# MAGIC **affordability_index (NAR-style):**
# MAGIC - qualifying_income = est_monthly_payment × 12 / 0.28
# MAGIC - affordability_index = (median_income / qualifying_income) × 100
# MAGIC - > 100: median household CAN afford the median home at 28% housing DTI
# MAGIC - < 100: median household CANNOT (underffordable market)
# MAGIC
# MAGIC **Depends on:** gold.county_market_monthly (run notebook 40 first)

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG     = "workspace"
GOLD        = f"{CATALOG}.gold"
SILVER      = f"{CATALOG}.silver"
TARGET      = f"{GOLD}.affordability_monthly"

# COMMAND ----------

afford = spark.sql(f"""
WITH base AS (
    SELECT
        m.county_fips,
        m.date_key,
        m.median_sale_price,
        r.value                   AS mortgage_rate,
        d.median_household_income AS median_income
    FROM {GOLD}.county_market_monthly m
    LEFT JOIN {SILVER}.economic_indicators r
           ON r.date_key        = m.date_key
          AND r.indicator       = 'mortgage_30yr'
          AND r.geography_scope = 'national'
    LEFT JOIN {SILVER}.demographics d
           ON d.county_fips = m.county_fips
          AND d.acs_year    = YEAR(m.date_key)
    WHERE m.median_sale_price IS NOT NULL
),
with_payment AS (
    SELECT
        county_fips,
        date_key,
        median_sale_price,
        mortgage_rate,
        median_income,
        CASE
            WHEN mortgage_rate > 0 THEN
                ROUND(
                    (median_sale_price * 0.80)
                    * (mortgage_rate / 100.0 / 12)
                    * POWER(1 + mortgage_rate / 100.0 / 12, 360)
                    / (POWER(1 + mortgage_rate / 100.0 / 12, 360) - 1),
                2)
            ELSE
                ROUND(median_sale_price * 0.80 / 360.0, 2)
        END AS est_monthly_payment
    FROM base
)
SELECT
    county_fips,
    date_key,
    median_sale_price,
    ROUND(mortgage_rate, 3)                            AS mortgage_rate,
    median_income,
    est_monthly_payment,
    -- affordability_index: >100 means median HH can afford at 28% housing DTI
    CASE WHEN est_monthly_payment > 0 AND median_income > 0 THEN
        ROUND((median_income * 0.28 / 12.0) / est_monthly_payment * 100, 1)
    END                                                AS affordability_index,
    CASE WHEN median_income > 0 THEN
        ROUND(median_sale_price / median_income, 2)
    END                                                AS price_to_income_ratio
FROM with_payment
""")

# COMMAND ----------

(afford
 .withColumn("_updated_at", F.current_timestamp())
 .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(TARGET))

count = spark.table(TARGET).count()
print(f"Rows written: {count:,}")
display(
    spark.table(TARGET)
    .orderBy("county_fips", "date_key")
    .limit(20))
