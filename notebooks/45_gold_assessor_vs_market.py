# Databricks notebook source
# MAGIC %md
# MAGIC # 45 - Gold: assessor_vs_market (SCD Type 2)
# MAGIC
# MAGIC **Role:** Compare Wake parcel transaction prices against Redfin/Realtor
# MAGIC market indices. Surfaces gaps between assessor-recorded activity and
# MAGIC listed/indexed market values.
# MAGIC
# MAGIC **SCD Type 2:** natural key (county_fips, date_key).
# MAGIC Revision triggers: Wake qualified sales file updated, or Redfin/Realtor
# MAGIC revises historical county medians.
# MAGIC
# MAGIC **Note:** assessed_vs_sale_gap is NULL until the Wake property file
# MAGIC (assessed_value) is added as a separate bronze source.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
GOLD    = f"{CATALOG}.gold"
SILVER  = f"{CATALOG}.silver"
TARGET  = f"{GOLD}.assessor_vs_market"

METRIC_COLS = [
    "parcel_sale_count", "parcel_median_price", "parcel_median_ppsf",
    "parcel_median_assessed", "median_assessed_vs_sale_gap", "outlier_count",
    "market_median_price", "hpi_index",
    "parcel_vs_market_gap", "parcel_vs_market_gap_pct",
]

# COMMAND ----------

# ── Compute incoming data ─────────────────────────────────────────────────────
new_data = spark.sql(f"""
WITH parcel_agg AS (
    SELECT
        county_fips,
        DATE_TRUNC('month', CAST(sale_date AS DATE))          AS date_key,
        COUNT(*)                                                   AS parcel_sale_count,
        ROUND(PERCENTILE_APPROX(sale_price,           0.5), 0)    AS parcel_median_price,
        ROUND(PERCENTILE_APPROX(price_per_sqft,       0.5), 2)    AS parcel_median_ppsf,
        ROUND(PERCENTILE_APPROX(assessed_value,       0.5), 0)    AS parcel_median_assessed,
        ROUND(PERCENTILE_APPROX(assessed_vs_sale_gap, 0.5), 0)    AS median_assessed_vs_sale_gap,
        SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END)               AS outlier_count
    FROM {SILVER}.parcel_sales
    WHERE NOT is_outlier AND sale_price > 0
    GROUP BY county_fips, DATE_TRUNC('month', CAST(sale_date AS DATE))
),
market AS (
    SELECT
        county_fips,
        date_key,
        COALESCE(
            MAX(CASE WHEN source = 'redfin'  THEN median_sale_price END),
            MAX(CASE WHEN source = 'realtor' THEN median_sale_price END)
        )                                                     AS market_median_price,
        MAX(CASE WHEN source = 'fhfa' THEN hpi_index END)    AS hpi_index
    FROM {SILVER}.home_prices
    GROUP BY county_fips, date_key
)
SELECT
    m.county_fips,
    m.date_key,
    p.parcel_sale_count,
    p.parcel_median_price,
    p.parcel_median_ppsf,
    p.parcel_median_assessed,
    p.median_assessed_vs_sale_gap,
    p.outlier_count,
    m.market_median_price,
    m.hpi_index,
    ROUND(p.parcel_median_price - m.market_median_price, 2)  AS parcel_vs_market_gap,
    CASE WHEN m.market_median_price > 0 THEN
        ROUND(
            (p.parcel_median_price - m.market_median_price)
            / m.market_median_price * 100, 2)
    END                                                      AS parcel_vs_market_gap_pct
FROM market m
LEFT JOIN parcel_agg p USING (county_fips, date_key)
""")

new_data = new_data.withColumn("_row_hash",
    F.md5(F.concat_ws("|", *[
        F.coalesce(F.col(c).cast("string"), F.lit("")) for c in METRIC_COLS
    ])))

new_data.createOrReplaceTempView("new_avm")

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
    parcel_sale_count            BIGINT,
    parcel_median_price          DOUBLE,
    parcel_median_ppsf           DOUBLE,
    parcel_median_assessed       DOUBLE,
    median_assessed_vs_sale_gap  DOUBLE,
    outlier_count                BIGINT,
    market_median_price          DOUBLE,
    hpi_index                    DOUBLE,
    parcel_vs_market_gap         DOUBLE,
    parcel_vs_market_gap_pct     DOUBLE,
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
USING new_avm AS s
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
FROM new_avm s
LEFT JOIN {TARGET} t
       ON t.county_fips = s.county_fips
      AND t.date_key    = s.date_key
      AND t.is_current  = true
WHERE t.county_fips IS NULL
""")

# COMMAND ----------

total       = spark.sql(f"SELECT COUNT(*) FROM {TARGET}").collect()[0][0]
current     = spark.sql(f"SELECT COUNT(*) FROM {TARGET} WHERE is_current = true").collect()[0][0]
with_parcel = spark.sql(f"SELECT COUNT(*) FROM {TARGET} WHERE is_current = true AND parcel_sale_count IS NOT NULL").collect()[0][0]
print(f"Total rows: {total:,}  |  Current: {current:,}  |  Historical: {total - current:,}  |  With parcel data: {with_parcel:,}")

display(
    spark.table(TARGET)
    .filter("is_current = true AND parcel_median_price IS NOT NULL")
    .orderBy(F.col("date_key").desc())
    .limit(20))
