# Databricks notebook source
# MAGIC %md
# MAGIC # 46 - Gold: zip_hotspots (SCD Type 2)
# MAGIC
# MAGIC **Role:** ZIP-level quarterly hotspot detection for the RDU area.
# MAGIC Surfaces ZIP codes with outlier price appreciation (>2 std dev from mean).
# MAGIC
# MAGIC **SCD Type 2:** natural key (zip, quarter_start).
# MAGIC Revision triggers: Realtor.com republishes or corrects historical zip data.
# MAGIC Note: appreciation_rank and anomaly_flag shift whenever new quarterly data
# MAGIC arrives (same dynamic as market_health_score PERCENT_RANK).
# MAGIC
# MAGIC **county_fips:** NULL until silver.xwalk_zip_county (HUD crosswalk) is built.

# COMMAND ----------

from pyspark.sql import functions as F, Window

CATALOG = "workspace"
GOLD    = f"{CATALOG}.gold"
BRONZE  = f"{CATALOG}.bronze"
TARGET  = f"{GOLD}.zip_hotspots"

METRIC_COLS = [
    "zip_name", "county_fips",
    "median_listing_price", "active_listing_count", "median_dom",
    "new_listing_count", "price_reduced_count",
    "qoq_price_change_pct", "appreciation_rank", "anomaly_flag",
]

# COMMAND ----------

# ── Compute incoming data ─────────────────────────────────────────────────────
# Monthly → quarterly aggregation
zip_qtr = spark.sql(f"""
SELECT
    CAST(postal_code AS STRING)                              AS zip,
    zip_name,
    DATE_TRUNC('quarter',
        TO_DATE(CONCAT(
            SUBSTRING(CAST(month_date_yyyymm AS STRING), 1, 4), '-',
            SUBSTRING(CAST(month_date_yyyymm AS STRING), 5, 2), '-01'
        )))                                                  AS quarter_start,
    AVG(CAST(median_listing_price  AS DOUBLE))               AS median_listing_price,
    AVG(CAST(active_listing_count  AS DOUBLE))               AS active_listing_count,
    AVG(CAST(median_days_on_market AS DOUBLE))               AS median_dom,
    AVG(CAST(new_listing_count     AS DOUBLE))               AS new_listing_count,
    AVG(CAST(price_reduced_count   AS DOUBLE))               AS price_reduced_count
FROM {BRONZE}.realtor_inventory_zip
WHERE postal_code IS NOT NULL
  AND month_date_yyyymm IS NOT NULL
GROUP BY postal_code, zip_name, quarter_start
""")

# Quarter-over-quarter price change per ZIP
w_zip = Window.partitionBy("zip").orderBy("quarter_start")

zip_qoq = (zip_qtr
    .withColumn("prev_price", F.lag("median_listing_price", 1).over(w_zip))
    .withColumn("qoq_price_change_pct",
        F.round(
            (F.col("median_listing_price") - F.col("prev_price"))
            / F.col("prev_price") * 100, 2))
    .drop("prev_price")
    .withColumn("median_listing_price",  F.round("median_listing_price",  0))
    .withColumn("active_listing_count",  F.col("active_listing_count").cast("long"))
    .withColumn("median_dom",            F.round("median_dom",            1))
    .withColumn("new_listing_count",     F.col("new_listing_count").cast("long"))
    .withColumn("price_reduced_count",   F.col("price_reduced_count").cast("long")))

# Anomaly flag: |qoq| > 2 std deviations from that quarter's mean
w_qtr = Window.partitionBy("quarter_start")

new_data = (zip_qoq
    .withColumn("qtr_mean",   F.avg("qoq_price_change_pct").over(w_qtr))
    .withColumn("qtr_stddev", F.stddev("qoq_price_change_pct").over(w_qtr))
    .withColumn("anomaly_flag",
        F.when(
            F.col("qtr_stddev") > 0,
            F.abs(F.col("qoq_price_change_pct") - F.col("qtr_mean"))
            > 2 * F.col("qtr_stddev")
        ).otherwise(F.lit(False)))
    .withColumn("appreciation_rank",
        F.rank().over(
            Window.partitionBy("quarter_start")
                  .orderBy(F.col("qoq_price_change_pct").desc())))
    .withColumn("county_fips", F.lit(None).cast("string"))
    .drop("qtr_mean", "qtr_stddev"))

new_data = new_data.withColumn("_row_hash",
    F.md5(F.concat_ws("|", *[
        F.coalesce(F.col(c).cast("string"), F.lit("")) for c in METRIC_COLS
    ])))

new_data.createOrReplaceTempView("new_zip")

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
    zip                   STRING  NOT NULL,
    quarter_start         DATE    NOT NULL,
    zip_name              STRING,
    county_fips           STRING,
    median_listing_price  DOUBLE,
    active_listing_count  BIGINT,
    median_dom            DOUBLE,
    new_listing_count     BIGINT,
    price_reduced_count   BIGINT,
    qoq_price_change_pct  DOUBLE,
    appreciation_rank     INT,
    anomaly_flag          BOOLEAN,
    _row_hash             STRING,
    effective_start_date  DATE    NOT NULL,
    effective_end_date    DATE,
    is_current            BOOLEAN NOT NULL,
    _updated_at           TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

# ── Pass 1: Close changed rows ────────────────────────────────────────────────
spark.sql(f"""
MERGE INTO {TARGET} AS t
USING new_zip AS s
  ON  t.zip           = s.zip
 AND  t.quarter_start = s.quarter_start
 AND  t.is_current    = true
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
    s.zip,
    s.quarter_start,
    {metric_select},
    s._row_hash,
    current_date()      AS effective_start_date,
    NULL                AS effective_end_date,
    true                AS is_current,
    current_timestamp() AS _updated_at
FROM new_zip s
LEFT JOIN {TARGET} t
       ON t.zip           = s.zip
      AND t.quarter_start = s.quarter_start
      AND t.is_current    = true
WHERE t.zip IS NULL
""")

# COMMAND ----------

total     = spark.sql(f"SELECT COUNT(*) FROM {TARGET}").collect()[0][0]
current   = spark.sql(f"SELECT COUNT(*) FROM {TARGET} WHERE is_current = true").collect()[0][0]
anomalies = spark.sql(f"SELECT COUNT(*) FROM {TARGET} WHERE is_current = true AND anomaly_flag = true").collect()[0][0]
print(f"Total rows: {total:,}  |  Current: {current:,}  |  Historical: {total - current:,}  |  Anomalies: {anomalies:,}")

display(
    spark.table(TARGET)
    .filter("is_current = true AND anomaly_flag = true")
    .orderBy(F.col("quarter_start").desc(), "appreciation_rank")
    .limit(20))
