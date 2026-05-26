# Databricks notebook source
# MAGIC %md
# MAGIC # 40 - Gold: county_market_monthly (SCD Type 2)
# MAGIC
# MAGIC **Role:** Primary analytical mart. One row per (county, month).
# MAGIC Combines home_prices + market_activity into a single wide table.
# MAGIC
# MAGIC **SCD Type 2 implementation:**
# MAGIC Natural key: (county_fips, date_key).
# MAGIC Change detection via MD5 hash of all metric columns — if any metric is
# MAGIC revised by a source update, the old row is closed and a new version inserted.
# MAGIC
# MAGIC **Source priority for prices:**
# MAGIC - median_sale_price: Redfin > Realtor > FHFA
# MAGIC - median_list_price: Realtor > Redfin
# MAGIC - hpi_index: FHFA only

# COMMAND ----------

from pyspark.sql import functions as F, Window

CATALOG     = "workspace"
GOLD_SCHEMA = "gold"
SILVER      = f"{CATALOG}.silver"
TARGET      = f"{CATALOG}.{GOLD_SCHEMA}.county_market_monthly"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

# Metric columns tracked for change detection (natural key columns excluded)
METRIC_COLS = [
    "median_sale_price", "median_list_price", "hpi_index", "list_vs_sale_gap",
    "active_listings", "new_listings", "pending_listings", "months_of_supply",
    "median_dom", "sale_to_list_ratio", "sold_above_list_pct", "price_per_sqft",
    "price_mom", "price_yoy",
]

# COMMAND ----------

# ── Compute incoming data ─────────────────────────────────────────────────────
base = spark.sql(f"""
WITH prices AS (
    SELECT
        county_fips,
        date_key,
        MAX(CASE WHEN source = 'redfin'  THEN median_sale_price END) AS sale_redfin,
        MAX(CASE WHEN source = 'realtor' THEN median_sale_price END) AS sale_realtor,
        MAX(CASE WHEN source = 'fhfa'    THEN median_sale_price END) AS sale_fhfa,
        MAX(CASE WHEN source = 'redfin'  THEN median_list_price END) AS list_redfin,
        MAX(CASE WHEN source = 'realtor' THEN median_list_price END) AS list_realtor,
        MAX(CASE WHEN source = 'fhfa'    THEN hpi_index          END) AS hpi_index
    FROM {SILVER}.home_prices
    GROUP BY county_fips, date_key
),
activity AS (
    SELECT
        county_fips,
        date_key,
        AVG(active_listings)     AS active_listings,
        AVG(new_listings)        AS new_listings,
        AVG(pending_listings)    AS pending_listings,
        AVG(median_dom)          AS median_dom,
        AVG(sale_to_list_ratio)  AS sale_to_list_ratio,
        AVG(sold_above_list_pct) AS sold_above_list_pct,
        AVG(price_per_sqft)      AS price_per_sqft,
        AVG(months_of_supply)    AS months_of_supply_src
    FROM {SILVER}.market_activity
    GROUP BY county_fips, date_key
)
SELECT
    p.county_fips,
    p.date_key,
    COALESCE(p.sale_redfin,  p.sale_realtor,  p.sale_fhfa)  AS median_sale_price,
    COALESCE(p.list_realtor, p.list_redfin)                  AS median_list_price,
    p.hpi_index,
    ROUND(
        COALESCE(p.list_realtor, p.list_redfin)
        - COALESCE(p.sale_redfin, p.sale_realtor), 2
    )                                                        AS list_vs_sale_gap,
    CAST(a.active_listings  AS BIGINT)                       AS active_listings,
    CAST(a.new_listings     AS BIGINT)                       AS new_listings,
    CAST(a.pending_listings AS BIGINT)                       AS pending_listings,
    ROUND(COALESCE(a.months_of_supply_src,
        CASE WHEN a.new_listings > 0
             THEN a.active_listings / a.new_listings END), 2) AS months_of_supply,
    ROUND(a.median_dom,            1)                        AS median_dom,
    ROUND(a.sale_to_list_ratio,    4)                        AS sale_to_list_ratio,
    ROUND(a.sold_above_list_pct,   2)                        AS sold_above_list_pct,
    ROUND(a.price_per_sqft,        2)                        AS price_per_sqft
FROM prices p
LEFT JOIN activity a USING (county_fips, date_key)
WHERE p.county_fips IS NOT NULL AND p.date_key IS NOT NULL
""")

w = Window.partitionBy("county_fips").orderBy("date_key")

new_data = (base
    .withColumn("price_mom",
        F.round(
            (F.col("median_sale_price") - F.lag("median_sale_price", 1).over(w))
            / F.lag("median_sale_price", 1).over(w) * 100, 2))
    .withColumn("price_yoy",
        F.round(
            (F.col("median_sale_price") - F.lag("median_sale_price", 12).over(w))
            / F.lag("median_sale_price", 12).over(w) * 100, 2))
    .withColumn("_row_hash",
        F.md5(F.concat_ws("|", *[
            F.coalesce(F.col(c).cast("string"), F.lit("")) for c in METRIC_COLS
        ]))))

new_data.createOrReplaceTempView("new_cmm")

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
    median_list_price    DOUBLE,
    hpi_index            DOUBLE,
    list_vs_sale_gap     DOUBLE,
    active_listings      BIGINT,
    new_listings         BIGINT,
    pending_listings     BIGINT,
    months_of_supply     DOUBLE,
    median_dom           DOUBLE,
    sale_to_list_ratio   DOUBLE,
    sold_above_list_pct  DOUBLE,
    price_per_sqft       DOUBLE,
    price_mom            DOUBLE,
    price_yoy            DOUBLE,
    _row_hash            STRING,
    effective_start_date DATE    NOT NULL,
    effective_end_date   DATE,
    is_current           BOOLEAN NOT NULL,
    _updated_at          TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

# ── Pass 1: Close rows whose metrics changed ───────────────────────────────────
spark.sql(f"""
MERGE INTO {TARGET} AS t
USING new_cmm AS s
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

# ── Pass 2: Insert new version for any key with no active row ─────────────────
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
FROM new_cmm s
LEFT JOIN {TARGET} t
       ON t.county_fips = s.county_fips
      AND t.date_key    = s.date_key
      AND t.is_current  = true
WHERE t.county_fips IS NULL
""")

# COMMAND ----------

# ── Validation ────────────────────────────────────────────────────────────────
total   = spark.sql(f"SELECT COUNT(*) FROM {TARGET}").collect()[0][0]
current = spark.sql(f"SELECT COUNT(*) FROM {TARGET} WHERE is_current = true").collect()[0][0]
print(f"Total rows: {total:,}  |  Current: {current:,}  |  Historical: {total - current:,}")

display(spark.table(TARGET).filter("is_current = true").orderBy("county_fips", "date_key").limit(24))
