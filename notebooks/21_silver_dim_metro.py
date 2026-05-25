# Databricks notebook source
# MAGIC %md
# MAGIC # 21 - Silver: dim_metro (SCD Type 2)
# MAGIC
# MAGIC **Role:** One row per CBSA (metro area). RDU spans two CBSAs.
# MAGIC
# MAGIC **SCD Type 2 implementation:**
# MAGIC Same two-pass pattern as dim_geography. If a CBSA's name or county
# MAGIC composition changes (Census redraws metro boundaries roughly every decade),
# MAGIC the old row is closed and a new version is inserted.
# MAGIC
# MAGIC **Tracked attributes:** cbsa_name, county_fips_list
# MAGIC
# MAGIC - 39580 Raleigh-Cary MSA       → Wake (37183), Johnston (37101), Franklin (37069)
# MAGIC - 20500 Durham-Chapel Hill MSA → Durham (37063), Orange (37135), Chatham (37037),
# MAGIC                                   Granville (37077), Person (37145)

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG       = "workspace"
SILVER_SCHEMA = "silver"
TARGET        = f"{CATALOG}.{SILVER_SCHEMA}.dim_metro"

# COMMAND ----------

METROS = [
    ("39580", "Raleigh-Cary MSA",       "NC", "37", "37183,37101,37069"),
    ("20500", "Durham-Chapel Hill MSA",  "NC", "37", "37063,37135,37037,37077,37145"),
]

new_data = (spark.createDataFrame(METROS,
                ["cbsa_code", "cbsa_name", "state_abbr", "state_fips", "county_fips_list"])
            .withColumn("csa_name", F.lit("Raleigh-Durham-Cary")))

new_data.createOrReplaceTempView("new_metro_data")

# COMMAND ----------

existing_cols = []
try:
    existing_cols = [c.name for c in spark.table(TARGET).schema]
except Exception:
    pass

if "is_current" not in existing_cols:
    spark.sql(f"DROP TABLE IF EXISTS {TARGET}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    cbsa_code           STRING      NOT NULL,
    cbsa_name           STRING,
    state_abbr          STRING,
    state_fips          STRING,
    county_fips_list    STRING,
    csa_name            STRING,
    effective_start_date DATE       NOT NULL,
    effective_end_date  DATE,
    is_current          BOOLEAN     NOT NULL,
    _updated_at         TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

# ── Pass 1: Close rows where tracked attributes changed ───────────────────────
spark.sql(f"""
MERGE INTO {TARGET} AS t
USING new_metro_data AS s
  ON t.cbsa_code   = s.cbsa_code
 AND t.is_current  = true
WHEN MATCHED AND (
       t.cbsa_name        != s.cbsa_name
    OR t.county_fips_list != s.county_fips_list
) THEN UPDATE SET
    t.effective_end_date = current_date(),
    t.is_current         = false,
    t._updated_at        = current_timestamp()
""")

# COMMAND ----------

# ── Pass 2: Insert new version for any CBSA with no active row ────────────────
spark.sql(f"""
INSERT INTO {TARGET}
SELECT
    s.cbsa_code,
    s.cbsa_name,
    s.state_abbr,
    s.state_fips,
    s.county_fips_list,
    s.csa_name,
    current_date()      AS effective_start_date,
    NULL                AS effective_end_date,
    true                AS is_current,
    current_timestamp() AS _updated_at
FROM new_metro_data s
LEFT JOIN {TARGET} t
       ON t.cbsa_code  = s.cbsa_code
      AND t.is_current = true
WHERE t.cbsa_code IS NULL
""")

# COMMAND ----------

total   = spark.sql(f"SELECT COUNT(*) FROM {TARGET}").collect()[0][0]
current = spark.sql(f"SELECT COUNT(*) FROM {TARGET} WHERE is_current = true").collect()[0][0]
print(f"Total rows: {total}  |  Current: {current}  |  Historical: {total - current}")

display(spark.table(TARGET).orderBy("cbsa_code", "effective_start_date"))
