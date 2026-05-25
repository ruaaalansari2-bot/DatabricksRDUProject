# Databricks notebook source
# MAGIC %md
# MAGIC # 28 - Silver: building_permits
# MAGIC
# MAGIC **Role:** New residential construction permits from Census BPS, filtered to
# MAGIC RDU counties, keyed to county + month. The supply-pipeline signal.
# MAGIC
# MAGIC **survey_date format:** YYYYMM string (e.g. "202603"). Parsed to the first
# MAGIC day of that month as date_key.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG       = "workspace"
SILVER_SCHEMA = "silver"
BRONZE_SCHEMA = "bronze"

RDU_FIPS = ["37183","37063","37135","37101","37037","37069","37077","37145"]

# COMMAND ----------

raw = (spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.census_bps_county")
       .filter(F.col("fips_code").isin(RDU_FIPS)))

permits = (raw
           .withColumn("date_key",
                       F.to_date(F.concat(
                           F.substring(F.col("survey_date").cast("string"), 1, 4),
                           F.lit("-"),
                           F.substring(F.col("survey_date").cast("string"), 5, 2),
                           F.lit("-01"))))
           .withColumn("multi_family_units",
                       F.col("units_2unit") + F.col("units_34unit") + F.col("units_5plus"))
           .withColumn("_ingested_at", F.current_timestamp())
           .select(
               F.col("fips_code").alias("county_fips"),
               "date_key",
               F.col("county_name"),
               F.col("units_1unit").alias("single_family_units"),
               F.col("units_2unit").alias("duplex_units"),
               F.col("units_34unit").alias("small_multi_units"),
               F.col("units_5plus").alias("large_multi_units"),
               "multi_family_units",
               "total_units_permitted",
               "_ingested_at"))

# COMMAND ----------

(permits.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.building_permits"))

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.building_permits")
    .orderBy("county_fips", "date_key"))
