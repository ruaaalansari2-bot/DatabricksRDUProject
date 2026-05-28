# Databricks notebook source
# MAGIC %md
# MAGIC # 41 - Gold: affordability_monthly (SCD Type 2)
# MAGIC
# MAGIC **Role:** Housing affordability by county x month.
# MAGIC
# MAGIC **SCD Type 2:** natural key (county_fips, date_key).
# MAGIC Revision triggers: mortgage_rate revised by FRED, income updated by Census,
# MAGIC or sale_price history corrected by Redfin/Realtor.
# MAGIC
# MAGIC **Mortgage model:** 20% down, 30yr fixed, standard amortisation formula.
# MAGIC **affordability_index:** NAR-style — >100 median HH can afford at 28% DTI.
# MAGIC
# MAGIC **Depends on:** gold.county_market_monthly (notebook 40)

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
GOLD    = f"{CATALOG}.gold"
SILVER  = f"{CATALOG}.silver"
TARGET  = f"{GOLD}.affordability_monthly"

METRIC_COLS = [
    "median_sale_price", "mortgage_rate", "median_income",
    "est_monthly_payment", "affordability_index", "price_to_income_ratio",
]

# COMMAND ----------

# ── Compute incoming data ─────────────────────────────────────────────────────
new_data = spark.sql(f"""
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
          AND d.acs_year    = (
              SELECT MAX(d2.acs_year)
              FROM {SILVER}.demographics d2
              WHERE d2.county_fips = m.county_fips
                AND d2.acs_year   <= YEAR(m.date_key)
          )
    WHERE m.median_sale_price IS NOT NULL
      AND m.is_current = true
),
with_payment AS (
    SELECT
        county_fips,
        date_key,
        median_sale_price,
        ROUND(mortgage_rate, 3) AS mortgage_rate,
        median_income,
        CASE
            WHEN mortgage_rate > 0 THEN
                ROUND(
                    (median_sale_price * 0.80)
                    * (mortgage_rate / 100.0 / 12)
                    * POWER(1 + mortgage_rate / 100.0 / 12, 360)
                    / (POWER(1 + mortgage_rate / 100.0 / 12, 360) - 1),
                2)
            ELSE ROUND(median_sale_price * 0.80 / 360.0, 2)
        END AS est_monthly_payment
    FROM base
)
SELECT
    county_fips,
    date_key,
    median_sale_price,
    mortgage_rate,
    median_income,
    est_monthly_payment,
    CASE WHEN est_monthly_payment > 0 AND median_income > 0 THEN
        ROUND((median_income * 0.28 / 12.0) / est_monthly_payment * 100, 1)
    END AS affordability_index,
    CASE WHEN median_income > 0 THEN
        ROUND(median_sale_price / median_income, 2)
    END AS price_to_income_ratio
FROM with_payment
""")

new_data = new_data.withColumn("_row_hash",
    F.md5(F.concat_ws("|", *[
        F.coalesce(F.col(c).cast("string"), F.lit("")) for c in METRIC_COLS
    ])))

new_data.createOrReplaceTempView("new_afford")

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
    mortgage_rate        DOUBLE,
    median_income        DOUBLE,
    est_monthly_payment  DOUBLE,
    affordability_index  DOUBLE,
    price_to_income_ratio DOUBLE,
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
USING new_afford AS s
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
FROM new_afford s
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
