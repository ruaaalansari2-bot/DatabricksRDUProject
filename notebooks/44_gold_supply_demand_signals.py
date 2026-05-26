# Databricks notebook source
# MAGIC %md
# MAGIC # 44 - Gold: supply_demand_signals (SCD Type 2)
# MAGIC
# MAGIC **Role:** Supply pipeline and demand-pressure context by county x month.
# MAGIC
# MAGIC **SCD Type 2:** natural key (county_fips, date_key).
# MAGIC Revision triggers: BPS permit revisions, BLS unemployment revisions,
# MAGIC FRED rate updates, or upstream county_market_monthly changes.
# MAGIC
# MAGIC **Depends on:** gold.county_market_monthly (notebook 40)

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
GOLD    = f"{CATALOG}.gold"
SILVER  = f"{CATALOG}.silver"
TARGET  = f"{GOLD}.supply_demand_signals"

METRIC_COLS = [
    "single_family_units", "multi_family_units", "total_units_permitted",
    "county_unemployment_rate", "mortgage_rate",
    "median_sale_price", "active_listings", "months_of_supply", "price_yoy",
    "national_housing_starts", "nc_building_permits",
]

# COMMAND ----------

# ── Compute incoming data ─────────────────────────────────────────────────────
new_data = spark.sql(f"""
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
    p.single_family_units,
    p.multi_family_units,
    p.total_units_permitted,
    u.county_unemployment_rate,
    n.mortgage_rate,
    m.median_sale_price,
    m.active_listings,
    m.months_of_supply,
    m.price_yoy,
    n.national_housing_starts,
    n.nc_building_permits
FROM {GOLD}.county_market_monthly m
LEFT JOIN {SILVER}.building_permits p USING (county_fips, date_key)
LEFT JOIN unemployment              u USING (county_fips, date_key)
LEFT JOIN national_macro            n ON n.date_key = m.date_key
WHERE m.is_current = true
""")

new_data = new_data.withColumn("_row_hash",
    F.md5(F.concat_ws("|", *[
        F.coalesce(F.col(c).cast("string"), F.lit("")) for c in METRIC_COLS
    ])))

new_data.createOrReplaceTempView("new_sds")

# COMMAND ----------

# ── Migration guard + CREATE TABLE ────────────────────────────────────────────
existing_cols = []
try:
    existing_cols = [c.name for c in spark.table(TARGET).schema]
except Exception:
    pass
if "is_current" not in existing_cols:
    spark.sql(f"DROP TABLE IF EXISTS {TARGET}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    county_fips               STRING  NOT NULL,
    date_key                  DATE    NOT NULL,
    single_family_units       BIGINT,
    multi_family_units        BIGINT,
    total_units_permitted     BIGINT,
    county_unemployment_rate  DOUBLE,
    mortgage_rate             DOUBLE,
    median_sale_price         DOUBLE,
    active_listings           BIGINT,
    months_of_supply          DOUBLE,
    price_yoy                 DOUBLE,
    national_housing_starts   DOUBLE,
    nc_building_permits       DOUBLE,
    _row_hash                 STRING,
    effective_start_date      DATE    NOT NULL,
    effective_end_date        DATE,
    is_current                BOOLEAN NOT NULL,
    _updated_at               TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

# ── Pass 1: Close changed rows ────────────────────────────────────────────────
spark.sql(f"""
MERGE INTO {TARGET} AS t
USING new_sds AS s
  ON  t.county_fips = s.county_fips
 AND  t.date_key    = s.date_key
 AND  t.is_current  = true
WHEN MATCHED AND t._row_hash != s._row_hash
THEN UPDATE SET
    t.effective_end_date = current_date(),
    t.is_current         = false,
    t._updated_at        = current_timestamp()
""")

# COMMAND ----------

# ── Pass 2: Insert new versions ───────────────────────────────────────────────
metric_select = ",\n    ".join([f"s.{c}" for c in METRIC_COLS])
spark.sql(f"""
INSERT INTO {TARGET}
SELECT
    s.county_fips,
    s.date_key,
    {metric_select},
    s._row_hash,
    current_date()      AS effective_start_date,
    NULL                AS effective_end_date,
    true                AS is_current,
    current_timestamp() AS _updated_at
FROM new_sds s
LEFT JOIN {TARGET} t
       ON t.county_fips = s.county_fips
      AND t.date_key    = s.date_key
      AND t.is_current  = true
WHERE t.county_fips IS NULL
""")

# COMMAND ----------

total   = spark.sql(f"SELECT COUNT(*) FROM {TARGET}").collect()[0][0]
current = spark.sql(f"SELECT COUNT(*) FROM {TARGET} WHERE is_current = true").collect()[0][0]
print(f"Total rows: {total:,}  |  Current: {current:,}  |  Historical: {total - current:,}")

display(spark.table(TARGET).filter("is_current = true").orderBy("county_fips", "date_key").limit(20))
