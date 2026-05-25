# Databricks notebook source
# MAGIC %md
# MAGIC # 20 - Silver: dim_geography (SCD Type 2)
# MAGIC
# MAGIC **Role:** Backbone dimension — the Rosetta Stone mapping every source's
# MAGIC native geographic encoding to one canonical key: 5-digit county FIPS.
# MAGIC
# MAGIC **SCD Type 2 implementation:**
# MAGIC Every run compares incoming data against the currently-active row for each
# MAGIC county. If a tracked attribute has changed (e.g. CBSA reassignment after
# MAGIC a Census redraw), the old row is closed (`effective_end_date` set,
# MAGIC `is_current = false`) and a new row is inserted. Unchanged rows are
# MAGIC untouched. New counties are inserted fresh.
# MAGIC
# MAGIC **Tracked attributes (changes trigger a new version):**
# MAGIC   cbsa_code, cbsa_name, redfin_region, realtor_fips, bls_series_id
# MAGIC
# MAGIC **Non-tracked (county_name, state_fips etc. are stable identifiers —
# MAGIC   update in place via the MERGE if needed).**
# MAGIC
# MAGIC **Two-pass pattern (required by Delta MERGE semantics):**
# MAGIC   Pass 1 — MERGE: close changed active rows.
# MAGIC   Pass 2 — INSERT: add new version for any county with no active row.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

CATALOG       = "workspace"
SILVER_SCHEMA = "silver"
TARGET        = f"{CATALOG}.{SILVER_SCHEMA}.dim_geography"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

# COMMAND ----------

# ── Incoming data (source of truth) ──────────────────────────────────────────
RDU_COUNTIES = [
    ("37183", "Wake",      "39580", "Raleigh-Cary",        "37", "Wake County, NC",       37183, 37183, "LAUCN371830000000003"),
    ("37063", "Durham",    "20500", "Durham-Chapel Hill",   "37", "Durham County, NC",     37063, 37063, "LAUCN370630000000003"),
    ("37135", "Orange",    "20500", "Durham-Chapel Hill",   "37", "Orange County, NC",     37135, 37135, "LAUCN371350000000003"),
    ("37101", "Johnston",  "39580", "Raleigh-Cary",        "37", "Johnston County, NC",   37101, 37101, "LAUCN371010000000003"),
    ("37037", "Chatham",   "20500", "Durham-Chapel Hill",   "37", "Chatham County, NC",   37037, 37037, "LAUCN370370000000003"),
    ("37069", "Franklin",  "39580", "Raleigh-Cary",        "37", "Franklin County, NC",   37069, 37069, "LAUCN370690000000003"),
    ("37077", "Granville", "20500", "Durham-Chapel Hill",   "37", "Granville County, NC",  37077, 37077, "LAUCN370770000000003"),
    ("37145", "Person",    "20500", "Durham-Chapel Hill",   "37", "Person County, NC",    37145, 37145, "LAUCN371450000000003"),
]

schema = StructType([
    StructField("county_fips",   StringType()),
    StructField("county_name",   StringType()),
    StructField("cbsa_code",     StringType()),
    StructField("cbsa_name",     StringType()),
    StructField("state_fips",    StringType()),
    StructField("redfin_region", StringType()),
    StructField("realtor_fips",  IntegerType()),
    StructField("fhfa_fips",     IntegerType()),
    StructField("bls_series_id", StringType()),
])

new_data = (spark.createDataFrame(RDU_COUNTIES, schema)
            .withColumn("state_name", F.lit("North Carolina"))
            .withColumn("csa_name",   F.lit("Raleigh-Durham-Cary")))

new_data.createOrReplaceTempView("new_geo_data")

# COMMAND ----------

# ── Create table on first run ────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    county_fips         STRING      NOT NULL,
    county_name         STRING,
    cbsa_code           STRING,
    cbsa_name           STRING,
    state_fips          STRING,
    state_name          STRING,
    csa_name            STRING,
    redfin_region       STRING,
    realtor_fips        INT,
    fhfa_fips           INT,
    bls_series_id       STRING,
    effective_start_date DATE       NOT NULL,
    effective_end_date  DATE,
    is_current          BOOLEAN     NOT NULL,
    _updated_at         TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

# ── Pass 1: Close rows where tracked attributes changed ───────────────────────
# A MERGE match fires only against is_current=true rows.
# If cbsa_code, cbsa_name, redfin_region, realtor_fips, or bls_series_id
# differ from the incoming value, the row is expired.
spark.sql(f"""
MERGE INTO {TARGET} AS t
USING new_geo_data AS s
  ON t.county_fips = s.county_fips
 AND t.is_current  = true
WHEN MATCHED AND (
       t.cbsa_code      != s.cbsa_code
    OR t.cbsa_name      != s.cbsa_name
    OR t.redfin_region  != s.redfin_region
    OR t.realtor_fips   != s.realtor_fips
    OR t.bls_series_id  != s.bls_series_id
) THEN UPDATE SET
    t.effective_end_date = current_date(),
    t.is_current         = false,
    t._updated_at        = current_timestamp()
""")

# COMMAND ----------

# ── Pass 2: Insert new version for any county with no active row ──────────────
# Covers two cases:
#   a) Brand-new county (never seen before).
#   b) County whose active row was just closed in Pass 1.
spark.sql(f"""
INSERT INTO {TARGET}
SELECT
    s.county_fips,
    s.county_name,
    s.cbsa_code,
    s.cbsa_name,
    s.state_fips,
    s.state_name,
    s.csa_name,
    s.redfin_region,
    s.realtor_fips,
    s.fhfa_fips,
    s.bls_series_id,
    current_date()      AS effective_start_date,
    NULL                AS effective_end_date,
    true                AS is_current,
    current_timestamp() AS _updated_at
FROM new_geo_data s
LEFT JOIN {TARGET} t
       ON t.county_fips = s.county_fips
      AND t.is_current  = true
WHERE t.county_fips IS NULL
""")

# COMMAND ----------

# ── Validation ────────────────────────────────────────────────────────────────
total   = spark.sql(f"SELECT COUNT(*) FROM {TARGET}").collect()[0][0]
current = spark.sql(f"SELECT COUNT(*) FROM {TARGET} WHERE is_current = true").collect()[0][0]
expired = total - current
print(f"Total rows: {total}  |  Current: {current}  |  Expired (historical): {expired}")

display(spark.table(TARGET).orderBy("county_fips", "effective_start_date"))
