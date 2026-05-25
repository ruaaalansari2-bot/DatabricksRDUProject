# Databricks notebook source
# MAGIC %md
# MAGIC # 08 - Bronze: Census Building Permits Survey (County)
# MAGIC
# MAGIC **Layer:** Bronze
# MAGIC **Source:** Census Bureau Building Permits Survey — monthly county-level
# MAGIC file. Shows new residential construction permits by unit type
# MAGIC (1-unit, 2-unit, 3-4 unit, 5+ unit).
# MAGIC **Scope:** Filtered to NC (state FIPS 37) after read.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import glob, os

CATALOG = "workspace"
BRONZE_SCHEMA = "bronze"
SOURCE_DIR = "/Volumes/workspace/landing/raw/bps/"

NC_STATE_FIPS = "37"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# BPS files are comma-delimited .txt with a fixed column layout.
# Columns: survey_date, statefp, countyfp, region, division, county_name,
# then permit counts and values for each unit type.
raw = (spark.read
       .option("header", "false")
       .option("inferSchema", "true")
       .csv(SOURCE_DIR + "*.txt"))

# BPS has no header — assign names based on the published column spec.
BPS_COLUMNS = [
    "survey_date", "state_fips", "county_fips", "region_code", "division_code",
    "county_name",
    "bldgs_1unit", "units_1unit", "value_1unit",
    "bldgs_2unit", "units_2unit", "value_2unit",
    "bldgs_34unit", "units_34unit", "value_34unit",
    "bldgs_5plus", "units_5plus", "value_5plus",
    "bldgs_1unit_rep", "units_1unit_rep", "value_1unit_rep",
    "bldgs_2unit_rep", "units_2unit_rep", "value_2unit_rep",
    "bldgs_34unit_rep", "units_34unit_rep", "value_34unit_rep",
    "bldgs_5plus_rep", "units_5plus_rep", "value_5plus_rep",
]

for i, col_name in enumerate(BPS_COLUMNS):
    raw = raw.withColumnRenamed(f"_c{i}", col_name)

bronze = (raw
          .filter(F.col("state_fips").cast("string") == NC_STATE_FIPS)
          .withColumn("fips_code", F.concat(
              F.lpad(F.col("state_fips").cast("string"), 2, "0"),
              F.lpad(F.col("county_fips").cast("string"), 3, "0")))
          .withColumn("total_units_permitted",
                      F.col("units_1unit") + F.col("units_2unit") +
                      F.col("units_34unit") + F.col("units_5plus"))
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_source_file", F.lit(SOURCE_DIR)))

# COMMAND ----------

(bronze.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.census_bps_county"))

# COMMAND ----------

RDU_FIPS = ["37183", "37063", "37135", "37101", "37037", "37069", "37077", "37145"]

display(
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.census_bps_county")
    .filter(F.col("fips_code").isin(RDU_FIPS))
    .select("survey_date", "county_name", "fips_code",
            "units_1unit", "units_5plus", "total_units_permitted")
    .orderBy("fips_code", "survey_date"))
