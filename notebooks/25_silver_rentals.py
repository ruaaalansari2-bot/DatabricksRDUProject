# Databricks notebook source
# MAGIC %md
# MAGIC # 25 - Silver: rentals
# MAGIC
# MAGIC **Role:** Median gross rent facts from Census ACS, forward-filled to monthly
# MAGIC grain so the gold layer can join rent vs. buy on the same date_key.
# MAGIC
# MAGIC **Time-grain rule:** Census ACS is annual (survey year). Each county's
# MAGIC annual rent value is repeated for all 12 months of that year.
# MAGIC The `acs_year` column documents which ACS vintage produced the value.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG       = "workspace"
SILVER_SCHEMA = "silver"
BRONZE_SCHEMA = "bronze"

RDU_FIPS_3 = ["183","063","135","101","037","069","077","145"]
ACS_YEAR   = 2023   # vintage of the ACS 5-year estimates in bronze

# COMMAND ----------

housing = (spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.census_acs_housing")
           .filter(F.col("county").isin(RDU_FIPS_3))
           .select("fips_code",
                   F.col("median_gross_rent").cast("double")))

# Forward-fill to monthly: cross-join with months 1-12 for the ACS year.
months = spark.range(1, 13).select(F.col("id").cast("int").alias("month"))

rentals = (housing.crossJoin(months)
           .withColumn("date_key",
                       F.to_date(F.concat_ws("-", F.lit(ACS_YEAR), F.col("month"), F.lit("1"))))
           .withColumn("acs_year", F.lit(ACS_YEAR))
           .withColumnRenamed("fips_code", "county_fips")
           .filter(F.col("median_gross_rent").isNotNull())
           .withColumn("_ingested_at", F.current_timestamp())
           .select("county_fips", "date_key", "acs_year", "median_gross_rent", "_ingested_at"))

# COMMAND ----------

(rentals.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.rentals"))

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.rentals")
    .orderBy("county_fips", "date_key")
    .limit(16))
