# Databricks notebook source
# MAGIC %md
# MAGIC # 26 - Silver: demographics
# MAGIC
# MAGIC **Role:** Population and tenure facts from Census ACS, keyed to county + year.
# MAGIC Joins income (B19013) and housing (B25003/B25077/B25064/B01003) on county FIPS.
# MAGIC
# MAGIC **Note:** Demographics are annual — no forward-fill to monthly here.
# MAGIC Gold tables that need monthly income use the `acs_year` column to pick
# MAGIC the most recent vintage.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG       = "workspace"
SILVER_SCHEMA = "silver"
BRONZE_SCHEMA = "bronze"

RDU_FIPS_3 = ["183","063","135","101","037","069","077","145"]
ACS_YEAR   = 2023

# COMMAND ----------

income = (spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.census_acs_income")
          .filter(F.col("county").isin(RDU_FIPS_3))
          .select(F.col("state"), F.col("county"),
                  F.col("median_household_income")))

housing = (spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.census_acs_housing")
           .filter(F.col("county").isin(RDU_FIPS_3))
           .select("fips_code", "state", "county",
                   "median_home_value", "median_gross_rent",
                   "total_occupied_units", "owner_occupied_units",
                   "renter_occupied_units", "total_population"))

demo = (housing
        .join(income, on=["state", "county"], how="left")
        .withColumn("renter_share",
                    F.round(F.col("renter_occupied_units") /
                            F.nullif(F.col("total_occupied_units"), F.lit(0)), 4))
        .withColumn("owner_share",
                    F.round(F.col("owner_occupied_units") /
                            F.nullif(F.col("total_occupied_units"), F.lit(0)), 4))
        .withColumn("acs_year", F.lit(ACS_YEAR))
        .withColumn("_ingested_at", F.current_timestamp())
        .select(
            F.col("fips_code").alias("county_fips"),
            "acs_year",
            "median_household_income",
            F.col("median_home_value").alias("median_home_value_selfreport"),
            "median_gross_rent",
            "owner_occupied_units",
            "renter_occupied_units",
            "renter_share",
            "owner_share",
            "total_population",
            "_ingested_at"))

# COMMAND ----------

(demo.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.demographics"))

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.demographics")
    .orderBy(F.col("median_household_income").desc()))
