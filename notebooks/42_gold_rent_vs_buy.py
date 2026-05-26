# Databricks notebook source
# MAGIC %md
# MAGIC # 42 - Gold: rent_vs_buy (SCD Type 2)
# MAGIC
# MAGIC **Role:** Rent vs. buy decision matrix by county x month.
# MAGIC
# MAGIC **SCD Type 2:** natural key (county_fips, date_key).
# MAGIC Revision triggers: any upstream change to affordability_monthly or rentals.
# MAGIC
# MAGIC **Key metrics:**
# MAGIC - rent_vs_buy_gap: est_monthly_payment − median_gross_rent (+ = buying costs more)
# MAGIC - rent_burden_pct: rent / (monthly income) × 100  (>30% = cost-burdened)
# MAGIC - rent_to_price_ratio: annualised rent / price (>5% historically favours buying)
# MAGIC
# MAGIC **Depends on:** gold.affordability_monthly (notebook 41)

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
GOLD    = f"{CATALOG}.gold"
SILVER  = f"{CATALOG}.silver"
TARGET  = f"{GOLD}.rent_vs_buy"

METRIC_COLS = [
    "median_sale_price", "est_monthly_payment", "mortgage_rate", "median_income",
    "median_gross_rent", "rent_vs_buy_gap", "rent_burden_pct", "rent_to_price_ratio",
]

# COMMAND ----------

# ── Compute incoming data ─────────────────────────────────────────────────────
new_data = spark.sql(f"""
SELECT
    a.county_fips,
    a.date_key,
    a.median_sale_price,
    a.est_monthly_payment,
    a.mortgage_rate,
    a.median_income,
    r.median_gross_rent,
    ROUND(a.est_monthly_payment - r.median_gross_rent, 2)        AS rent_vs_buy_gap,
    CASE WHEN a.median_income > 0 THEN
        ROUND(r.median_gross_rent / (a.median_income / 12.0) * 100, 1)
    END                                                           AS rent_burden_pct,
    CASE WHEN a.median_sale_price > 0 THEN
        ROUND((r.median_gross_rent * 12.0) / a.median_sale_price, 4)
    END                                                           AS rent_to_price_ratio
FROM {GOLD}.affordability_monthly a
LEFT JOIN {SILVER}.rentals r
       ON r.county_fips = a.county_fips
      AND r.date_key    = a.date_key
WHERE a.is_current = true
  AND a.est_monthly_payment IS NOT NULL
""")

new_data = new_data.withColumn("_row_hash",
    F.md5(F.concat_ws("|", *[
        F.coalesce(F.col(c).cast("string"), F.lit("")) for c in METRIC_COLS
    ])))

new_data.createOrReplaceTempView("new_rvb")

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
    county_fips          STRING  NOT NULL,
    date_key             DATE    NOT NULL,
    median_sale_price    DOUBLE,
    est_monthly_payment  DOUBLE,
    mortgage_rate        DOUBLE,
    median_income        DOUBLE,
    median_gross_rent    DOUBLE,
    rent_vs_buy_gap      DOUBLE,
    rent_burden_pct      DOUBLE,
    rent_to_price_ratio  DOUBLE,
    _row_hash            STRING,
    effective_start_date DATE    NOT NULL,
    effective_end_date   DATE,
    is_current           BOOLEAN NOT NULL,
    _updated_at          TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

# ── Pass 1: Close changed rows ────────────────────────────────────────────────
spark.sql(f"""
MERGE INTO {TARGET} AS t
USING new_rvb AS s
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
FROM new_rvb s
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

display(
    spark.table(TARGET)
    .filter("is_current = true AND median_gross_rent IS NOT NULL")
    .orderBy("county_fips", "date_key")
    .limit(20))
