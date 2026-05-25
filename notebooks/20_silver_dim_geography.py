# Databricks notebook source
# MAGIC %md
# MAGIC # 20 - Silver: dim_geography
# MAGIC
# MAGIC **Role:** Backbone dimension — the Rosetta Stone that maps every source's
# MAGIC native geographic encoding to one canonical key: 5-digit county FIPS.
# MAGIC
# MAGIC **Crosswalk fields stored here:**
# MAGIC - `redfin_region`  — the string Redfin uses (e.g. "Wake County, NC")
# MAGIC - `realtor_fips`   — the integer FIPS Realtor.com uses (e.g. 37183)
# MAGIC - `fhfa_fips`      — integer FIPS FHFA uses (same as realtor, stored explicitly)
# MAGIC - `bls_series_id`  — BLS LAUS series ID for this county's unemployment rate
# MAGIC
# MAGIC **Scope:** RDU CSA (8 counties). Extend by adding rows to RDU_COUNTIES.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

CATALOG       = "workspace"
SILVER_SCHEMA = "silver"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

# COMMAND ----------

# One row per county. Every crosswalk field any silver/gold notebook needs
# to resolve a source's geo encoding lives here — add columns, never rebuild.
RDU_COUNTIES = [
    # county_fips, county_name, cbsa_code, cbsa_name,            state_fips, redfin_region,              realtor_fips, fhfa_fips, bls_series_id
    ("37183", "Wake",      "39580", "Raleigh-Cary",        "37", "Wake County, NC",      37183, 37183, "LAUCN371830000000003"),
    ("37063", "Durham",    "20500", "Durham-Chapel Hill",   "37", "Durham County, NC",    37063, 37063, "LAUCN370630000000003"),
    ("37135", "Orange",    "20500", "Durham-Chapel Hill",   "37", "Orange County, NC",    37135, 37135, "LAUCN371350000000003"),
    ("37101", "Johnston",  "39580", "Raleigh-Cary",        "37", "Johnston County, NC",  37101, 37101, "LAUCN371010000000003"),
    ("37037", "Chatham",   "20500", "Durham-Chapel Hill",   "37", "Chatham County, NC",  37037, 37037, "LAUCN370370000000003"),
    ("37069", "Franklin",  "39580", "Raleigh-Cary",        "37", "Franklin County, NC",  37069, 37069, "LAUCN370690000000003"),
    ("37077", "Granville", "20500", "Durham-Chapel Hill",   "37", "Granville County, NC", 37077, 37077, "LAUCN370770000000003"),
    ("37145", "Person",    "20500", "Durham-Chapel Hill",   "37", "Person County, NC",   37145, 37145, "LAUCN371450000000003"),
]

schema = StructType([
    StructField("county_fips",    StringType()),
    StructField("county_name",    StringType()),
    StructField("cbsa_code",      StringType()),
    StructField("cbsa_name",      StringType()),
    StructField("state_fips",     StringType()),
    StructField("redfin_region",  StringType()),
    StructField("realtor_fips",   IntegerType()),
    StructField("fhfa_fips",      IntegerType()),
    StructField("bls_series_id",  StringType()),
])

dim = (spark.createDataFrame(RDU_COUNTIES, schema)
       .withColumn("state_name", F.lit("North Carolina"))
       .withColumn("csa_name",   F.lit("Raleigh-Durham-Cary"))
       .withColumn("_updated_at", F.current_timestamp()))

# COMMAND ----------

(dim.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.dim_geography"))

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SILVER_SCHEMA}.dim_geography"))
