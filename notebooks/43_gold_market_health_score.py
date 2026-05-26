# Databricks notebook source
# MAGIC %md
# MAGIC # 43 - Gold: market_health_score (SCD Type 2)
# MAGIC
# MAGIC **Role:** Composite 0–100 market activity score by county x month.
# MAGIC
# MAGIC **SCD Type 2:** natural key (county_fips, date_key).
# MAGIC Revision triggers: any new month of data shifts PERCENT_RANK scores for
# MAGIC all existing rows — SCD captures each revision of the relative ranking.
# MAGIC
# MAGIC **Component scores** (each 0–100, PERCENT_RANK across ALL county-months):
# MAGIC - dom_score:           low median_dom  → high score  (fast sales)
# MAGIC - sale_to_list_score:  high ratio      → high score  (strong demand)
# MAGIC - supply_score:        low months_of_supply → high score (tight inventory)
# MAGIC - momentum_score:      high price_yoy  → high score  (appreciation)
# MAGIC - affordability_score: high affordability_index → high score (buyer-friendly)
# MAGIC
# MAGIC **Weights:** dom 15% | sale_to_list 15% | supply 20% | momentum 30% | affordability 20%
# MAGIC
# MAGIC **Depends on:** gold.county_market_monthly + gold.affordability_monthly

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
GOLD    = f"{CATALOG}.gold"
TARGET  = f"{GOLD}.market_health_score"

METRIC_COLS = [
    "median_dom", "sale_to_list_ratio", "months_of_supply", "price_yoy",
    "affordability_index", "dom_score", "sale_to_list_score", "supply_score",
    "momentum_score", "affordability_score", "market_health_score",
]

# COMMAND ----------

# ── Compute incoming data ─────────────────────────────────────────────────────
new_data = spark.sql(f"""
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
    LEFT JOIN {GOLD}.affordability_monthly a
           ON a.county_fips = m.county_fips
          AND a.date_key    = m.date_key
          AND a.is_current  = true
    WHERE m.is_current = true
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
        ROUND(COALESCE((1 - PERCENT_RANK() OVER (ORDER BY median_dom ASC))       * 100, 50.0), 1) AS dom_score,
        ROUND(COALESCE(     PERCENT_RANK() OVER (ORDER BY sale_to_list_ratio ASC) * 100, 50.0), 1) AS sale_to_list_score,
        ROUND(COALESCE((1 - PERCENT_RANK() OVER (ORDER BY months_of_supply ASC)) * 100, 50.0), 1) AS supply_score,
        ROUND(COALESCE(     PERCENT_RANK() OVER (ORDER BY price_yoy ASC)          * 100, 50.0), 1) AS momentum_score,
        ROUND(COALESCE(     PERCENT_RANK() OVER (ORDER BY affordability_index ASC) * 100, 50.0), 1) AS affordability_score
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
        dom_score             * 0.15
        + sale_to_list_score  * 0.15
        + supply_score        * 0.20
        + momentum_score      * 0.30
        + affordability_score * 0.20,
    1) AS market_health_score
FROM scored
""")

new_data = new_data.withColumn("_row_hash",
    F.md5(F.concat_ws("|", *[
        F.coalesce(F.col(c).cast("string"), F.lit("")) for c in METRIC_COLS
    ])))

new_data.createOrReplaceTempView("new_health")

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
    median_dom           DOUBLE,
    sale_to_list_ratio   DOUBLE,
    months_of_supply     DOUBLE,
    price_yoy            DOUBLE,
    affordability_index  DOUBLE,
    dom_score            DOUBLE,
    sale_to_list_score   DOUBLE,
    supply_score         DOUBLE,
    momentum_score       DOUBLE,
    affordability_score  DOUBLE,
    market_health_score  DOUBLE,
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
USING new_health AS s
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
FROM new_health s
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
    .filter("is_current = true")
    .orderBy(F.col("date_key").desc(), F.col("market_health_score").desc())
    .limit(20))
