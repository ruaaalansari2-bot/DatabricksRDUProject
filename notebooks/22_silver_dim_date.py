# Databricks notebook source
# MAGIC %md
# MAGIC # 22 - Silver: dim_date
# MAGIC
# MAGIC **Role:** Standard date dimension at monthly grain — the common denominator
# MAGIC across all sources. Sub-monthly sources (Redfin weekly) roll up to month.
# MAGIC Supra-monthly sources (FHFA annual, Census annual) are forward-filled to month.
# MAGIC
# MAGIC **date_key:** first calendar day of each month (DATE type). All silver/gold
# MAGIC tables join on this key. Covers 2010-01-01 through 2030-12-01.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG       = "workspace"
SILVER_SCHEMA = "silver"

START_YEAR, END_YEAR = 2010, 2030

# COMMAND ----------

# Generate one row per month using a range of integers.
months = spark.range(0, (END_YEAR - START_YEAR + 1) * 12).select(
    F.add_months(F.lit(f"{START_YEAR}-01-01").cast("date"), F.col("id").cast("int")).alias("date_key")
)

dim = (months
       .withColumn("year",         F.year("date_key"))
       .withColumn("quarter",      F.quarter("date_key"))
       .withColumn("month",        F.month("date_key"))
       .withColumn("month_name",   F.date_format("date_key", "MMMM"))
       .withColumn("month_abbr",   F.date_format("date_key", "MMM"))
       .withColumn("year_month",   F.date_format("date_key", "yyyy-MM"))
       .withColumn("is_current_month",
                   F.col("date_key") == F.date_trunc("month", F.current_date()))
       .withColumn("_updated_at",  F.current_timestamp()))

# COMMAND ----------

(dim.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.dim_date"))

print(f"Generated {dim.count()} monthly date rows ({START_YEAR}-{END_YEAR})")

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SILVER_SCHEMA}.dim_date")
        .orderBy("date_key").limit(5))
