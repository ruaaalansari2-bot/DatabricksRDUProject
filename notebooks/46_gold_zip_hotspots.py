# Databricks notebook source
# MAGIC %md
# MAGIC # 46 - Gold: zip_hotspots
# MAGIC
# MAGIC **Role:** ZIP-level quarterly market hotspot detection for RDU area.
# MAGIC Surfaces ZIP codes with outlier price appreciation (>2 std dev from mean).
# MAGIC
# MAGIC **Source:** bronze.realtor_inventory_zip (monthly → aggregated to quarter)
# MAGIC
# MAGIC **Columns:**
# MAGIC - zip, zip_name, quarter_start
# MAGIC - median_listing_price, active_listing_count, median_dom
# MAGIC - qoq_price_change_pct — quarter-over-quarter listing price change %
# MAGIC - appreciation_rank — rank within quarter (1 = highest appreciation)
# MAGIC - anomaly_flag — true when |qoq| > 2 standard deviations from quarterly mean
# MAGIC
# MAGIC **Note on county mapping:**
# MAGIC County attribution (county_fips) requires silver.xwalk_zip_county (HUD ZIP
# MAGIC crosswalk). Until that table is built, county_fips is derived from a
# MAGIC hardcoded RDU ZIP list from dim_geography. Non-RDU NC ZIPs are included
# MAGIC but will have NULL county_fips.

# COMMAND ----------

from pyspark.sql import functions as F, Window

CATALOG = "workspace"
GOLD    = f"{CATALOG}.gold"
SILVER  = f"{CATALOG}.silver"
BRONZE  = f"{CATALOG}.bronze"
TARGET  = f"{GOLD}.zip_hotspots"

# COMMAND ----------

# Aggregate monthly ZIP data to quarterly
zip_quarterly = spark.sql(f"""
WITH monthly AS (
    SELECT
        CAST(postal_code AS STRING)                          AS zip,
        zip_name,
        -- month_date_yyyymm is an int like 202401; convert to quarter start date
        TO_DATE(
            CONCAT(SUBSTRING(CAST(month_date_yyyymm AS STRING), 1, 4), '-',
                   LPAD(
                       CAST((CAST(SUBSTRING(CAST(month_date_yyyymm AS STRING), 5, 2) AS INT) - 1) / 3 * 3 + 1 AS STRING),
                   2, '0'), '-01')
        )                                                    AS quarter_start,
        AVG(CAST(median_listing_price             AS DOUBLE)) AS median_listing_price,
        AVG(CAST(active_listing_count             AS DOUBLE)) AS active_listing_count,
        AVG(CAST(median_days_on_market            AS DOUBLE)) AS median_dom,
        AVG(CAST(new_listing_count                AS DOUBLE)) AS new_listing_count,
        AVG(CAST(price_reduced_count              AS DOUBLE)) AS price_reduced_count
    FROM {BRONZE}.realtor_inventory_zip
    WHERE postal_code IS NOT NULL
      AND month_date_yyyymm IS NOT NULL
    GROUP BY postal_code, zip_name, quarter_start
)
SELECT
    zip,
    zip_name,
    quarter_start,
    ROUND(median_listing_price,  0) AS median_listing_price,
    CAST(active_listing_count AS BIGINT) AS active_listing_count,
    ROUND(median_dom,            1) AS median_dom,
    CAST(new_listing_count    AS BIGINT) AS new_listing_count,
    CAST(price_reduced_count  AS BIGINT) AS price_reduced_count
FROM monthly
""")

# COMMAND ----------

# Quarter-over-quarter price change per ZIP
w_zip = Window.partitionBy("zip").orderBy("quarter_start")

zip_with_qoq = (zip_quarterly
    .withColumn("prev_price",
        F.lag("median_listing_price", 1).over(w_zip))
    .withColumn("qoq_price_change_pct",
        F.round(
            (F.col("median_listing_price") - F.col("prev_price"))
            / F.col("prev_price") * 100, 2))
    .drop("prev_price"))

# COMMAND ----------

# Anomaly flag: |qoq| > 2 std deviations from the mean within the same quarter
w_qtr = Window.partitionBy("quarter_start")

zip_scored = (zip_with_qoq
    .withColumn("qtr_mean_qoq",  F.avg("qoq_price_change_pct").over(w_qtr))
    .withColumn("qtr_stddev_qoq", F.stddev("qoq_price_change_pct").over(w_qtr))
    .withColumn("anomaly_flag",
        F.when(
            F.col("qtr_stddev_qoq") > 0,
            F.abs(F.col("qoq_price_change_pct") - F.col("qtr_mean_qoq"))
            > 2 * F.col("qtr_stddev_qoq")
        ).otherwise(F.lit(False)))
    .withColumn("appreciation_rank",
        F.rank().over(
            Window.partitionBy("quarter_start")
                  .orderBy(F.col("qoq_price_change_pct").desc())))
    .drop("qtr_mean_qoq", "qtr_stddev_qoq"))

# COMMAND ----------

# Optional: join county_fips from dim_geography redfin_region when xwalk not yet available.
# For now, add county_fips as NULL — populate once silver.xwalk_zip_county is built.
hotspots = zip_scored.withColumn("county_fips", F.lit(None).cast("string"))

# COMMAND ----------

(hotspots
 .withColumn("_updated_at", F.current_timestamp())
 .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(TARGET))

total    = spark.table(TARGET).count()
anomalies = spark.table(TARGET).filter(F.col("anomaly_flag")).count()
print(f"Rows written: {total:,}  |  Anomaly-flagged ZIP-quarters: {anomalies:,}")

display(
    spark.table(TARGET)
    .filter(F.col("anomaly_flag"))
    .orderBy(F.col("quarter_start").desc(), "appreciation_rank")
    .limit(20))
