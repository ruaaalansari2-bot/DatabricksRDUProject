# Databricks notebook source
# MAGIC %md
# MAGIC # 42 - Gold: rent_vs_buy
# MAGIC
# MAGIC **Role:** Rent vs. buy decision matrix by county x month.
# MAGIC
# MAGIC **Columns:**
# MAGIC - rent_vs_buy_gap: est_monthly_payment − median_gross_rent
# MAGIC   Positive = buying costs more per month than renting
# MAGIC - rent_burden_pct: rent / (median_income / 12) × 100
# MAGIC   Standard threshold: >30% = cost-burdened
# MAGIC - rent_to_price_ratio: annualised rent / median_sale_price
# MAGIC   Rule of thumb: > 0.05 (5%) favors buying long-term
# MAGIC
# MAGIC **Depends on:** gold.affordability_monthly (run notebook 41 first)

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
GOLD    = f"{CATALOG}.gold"
SILVER  = f"{CATALOG}.silver"
TARGET  = f"{GOLD}.rent_vs_buy"

# COMMAND ----------

rvb = spark.sql(f"""
SELECT
    a.county_fips,
    a.date_key,
    a.median_sale_price,
    a.est_monthly_payment,
    a.mortgage_rate,
    a.median_income,
    r.median_gross_rent,
    ROUND(a.est_monthly_payment - r.median_gross_rent, 2)         AS rent_vs_buy_gap,
    CASE WHEN a.median_income > 0 THEN
        ROUND(r.median_gross_rent / (a.median_income / 12.0) * 100, 1)
    END                                                            AS rent_burden_pct,
    CASE WHEN a.median_sale_price > 0 THEN
        ROUND((r.median_gross_rent * 12.0) / a.median_sale_price, 4)
    END                                                            AS rent_to_price_ratio
FROM {GOLD}.affordability_monthly a
LEFT JOIN {SILVER}.rentals r
       ON r.county_fips = a.county_fips
      AND r.date_key    = a.date_key
WHERE a.est_monthly_payment IS NOT NULL
""")

# COMMAND ----------

(rvb
 .withColumn("_updated_at", F.current_timestamp())
 .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(TARGET))

count = spark.table(TARGET).count()
print(f"Rows written: {count:,}")
display(
    spark.table(TARGET)
    .filter(F.col("median_gross_rent").isNotNull())
    .orderBy("county_fips", "date_key")
    .limit(20))
