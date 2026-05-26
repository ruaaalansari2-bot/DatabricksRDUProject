# Databricks notebook source
# MAGIC %md
# MAGIC # 44 - Gold: supply_demand_signals
# MAGIC
# MAGIC **Role:** Supply pipeline and demand-pressure context by county x month.
# MAGIC Joins building permits (new supply entering the market) with unemployment
# MAGIC (demand-side affordability pressure) and market price/inventory context.
# MAGIC
# MAGIC **Columns:**
# MAGIC - single_family_units, multi_family_units, total_units_permitted — new supply
# MAGIC - county_unemployment_rate — BLS LAUS monthly rate
# MAGIC - median_sale_price, active_listings, months_of_supply — market context
# MAGIC - price_yoy — price trend
# MAGIC
# MAGIC **Depends on:** gold.county_market_monthly (run notebook 40 first)

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
GOLD    = f"{CATALOG}.gold"
SILVER  = f"{CATALOG}.silver"
TARGET  = f"{GOLD}.supply_demand_signals"

# COMMAND ----------

sds = spark.sql(f"""
WITH unemployment AS (
    SELECT
        date_key,
        geography_scope AS county_fips,
        value           AS county_unemployment_rate
    FROM {SILVER}.economic_indicators
    WHERE indicator = 'county_unemployment'
),
national_macro AS (
    SELECT
        date_key,
        MAX(CASE WHEN indicator = 'mortgage_30yr'       THEN value END) AS mortgage_rate,
        MAX(CASE WHEN indicator = 'housing_starts'      THEN value END) AS national_housing_starts,
        MAX(CASE WHEN indicator = 'nc_building_permits' THEN value END) AS nc_building_permits
    FROM {SILVER}.economic_indicators
    WHERE geography_scope = 'national'
    GROUP BY date_key
)
SELECT
    m.county_fips,
    m.date_key,
    -- supply pipeline
    p.single_family_units,
    p.multi_family_units,
    p.total_units_permitted,
    -- demand pressure
    u.county_unemployment_rate,
    n.mortgage_rate,
    -- market context
    m.median_sale_price,
    m.active_listings,
    m.months_of_supply,
    m.price_yoy,
    -- macro context
    n.national_housing_starts,
    n.nc_building_permits
FROM {GOLD}.county_market_monthly m
LEFT JOIN {SILVER}.building_permits p USING (county_fips, date_key)
LEFT JOIN unemployment              u USING (county_fips, date_key)
LEFT JOIN national_macro            n ON n.date_key = m.date_key
WHERE m.county_fips IS NOT NULL
""")

# COMMAND ----------

(sds
 .withColumn("_updated_at", F.current_timestamp())
 .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(TARGET))

count = spark.table(TARGET).count()
print(f"Rows written: {count:,}")
display(spark.table(TARGET).orderBy("county_fips", "date_key").limit(20))
