# Databricks notebook source
# MAGIC %md
# MAGIC # 21 - Silver: dim_metro
# MAGIC
# MAGIC **Role:** One row per CBSA (metro area). Gives FHFA metro-level data and
# MAGIC any metro-aggregated analysis a clean home. RDU spans two CBSAs.
# MAGIC
# MAGIC - 39580 Raleigh-Cary MSA       → Wake, Johnston, Franklin
# MAGIC - 20500 Durham-Chapel Hill MSA → Durham, Orange, Chatham, Granville, Person

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG       = "workspace"
SILVER_SCHEMA = "silver"

# COMMAND ----------

METROS = [
    ("39580", "Raleigh-Cary MSA",        "NC", ["37183", "37101", "37069"]),
    ("20500", "Durham-Chapel Hill MSA",   "NC", ["37063", "37135", "37037", "37077", "37145"]),
]

rows = [(code, name, state, ",".join(fips_list))
        for code, name, state, fips_list in METROS]

dim = (spark.createDataFrame(rows, ["cbsa_code", "cbsa_name", "state_fips", "county_fips_list"])
       .withColumn("csa_name", F.lit("Raleigh-Durham-Cary"))
       .withColumn("_updated_at", F.current_timestamp()))

# COMMAND ----------

(dim.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.dim_metro"))

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SILVER_SCHEMA}.dim_metro"))
