# Databricks notebook source
# MAGIC %md
# MAGIC # 43 - Gold: market_health_score
# MAGIC
# MAGIC **Role:** Composite 0–100 market activity score by county x month.
# MAGIC
# MAGIC **Component scores** (each 0–100 via PERCENT_RANK across all county-months):
# MAGIC - dom_score:           low median_dom  → high score  (fast sales)
# MAGIC - sale_to_list_score:  high ratio      → high score  (strong demand)
# MAGIC - supply_score:        low months_of_supply → high score (tight inventory)
# MAGIC - momentum_score:      high price_yoy  → high score  (price appreciation)
# MAGIC - affordability_score: high affordability_index → high score (buyer-friendly)
# MAGIC
# MAGIC **Weights:**
# MAGIC - dom_score          15%
# MAGIC - sale_to_list_score 15%
# MAGIC - supply_score       20%
# MAGIC - momentum_score     30%
# MAGIC - affordability_score 20%
# MAGIC
# MAGIC Interpretation: high score = fast, supply-tight, price-appreciating, relatively
# MAGIC affordable market. A very hot seller's market will score high on velocity and
# MAGIC momentum but lower on affordability, moderating the composite.
# MAGIC
# MAGIC **Depends on:** gold.county_market_monthly + gold.affordability_monthly

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
GOLD    = f"{CATALOG}.gold"
TARGET  = f"{GOLD}.market_health_score"

# COMMAND ----------

health = spark.sql(f"""
WITH base AS (
    SELECT
        m.county_fips,
        m.date_key,
        m.median_dom,
        m.sale_to_list_ratio,
        m.months_of_supply,
        m.price_yoy,
        a.affordability_index
    FROM {GOLD}.county_market_monthly m
    LEFT JOIN {GOLD}.affordability_monthly a USING (county_fips, date_key)
),
scored AS (
    SELECT
        county_fips,
        date_key,
        median_dom,
        sale_to_list_ratio,
        months_of_supply,
        price_yoy,
        affordability_index,
        -- low DOM = fast sales = higher score
        ROUND(COALESCE(
            (1 - PERCENT_RANK() OVER (ORDER BY median_dom ASC)) * 100,
            50.0), 1)                                           AS dom_score,
        -- high sale_to_list = strong offers = higher score
        ROUND(COALESCE(
            PERCENT_RANK() OVER (ORDER BY sale_to_list_ratio ASC) * 100,
            50.0), 1)                                           AS sale_to_list_score,
        -- low months_of_supply = tight inventory = higher score
        ROUND(COALESCE(
            (1 - PERCENT_RANK() OVER (ORDER BY months_of_supply ASC)) * 100,
            50.0), 1)                                           AS supply_score,
        -- high price_yoy = strong appreciation = higher score
        ROUND(COALESCE(
            PERCENT_RANK() OVER (ORDER BY price_yoy ASC) * 100,
            50.0), 1)                                           AS momentum_score,
        -- high affordability_index = more affordable = higher score
        ROUND(COALESCE(
            PERCENT_RANK() OVER (ORDER BY affordability_index ASC) * 100,
            50.0), 1)                                           AS affordability_score
    FROM base
)
SELECT
    county_fips,
    date_key,
    median_dom,
    sale_to_list_ratio,
    months_of_supply,
    price_yoy,
    affordability_index,
    dom_score,
    sale_to_list_score,
    supply_score,
    momentum_score,
    affordability_score,
    ROUND(
        dom_score            * 0.15
        + sale_to_list_score * 0.15
        + supply_score       * 0.20
        + momentum_score     * 0.30
        + affordability_score * 0.20,
    1) AS market_health_score
FROM scored
""")

# COMMAND ----------

(health
 .withColumn("_updated_at", F.current_timestamp())
 .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(TARGET))

count = spark.table(TARGET).count()
print(f"Rows written: {count:,}")
display(
    spark.table(TARGET)
    .orderBy(F.col("date_key").desc(), F.col("market_health_score").desc())
    .limit(20))
