# Databricks notebook source
# MAGIC %md
# MAGIC # 30 - Metric View: Semantic Layer for AI/BI
# MAGIC
# MAGIC **Purpose:** define reusable measures and dimensions ONCE so that both
# MAGIC the AI/BI Dashboard and the Genie Space report identical numbers. This
# MAGIC is the Databricks-native analogue of a Power BI semantic model / DAX
# MAGIC measure set.
# MAGIC
# MAGIC A Unity Catalog **metric view** is defined in YAML: it points at a gold
# MAGIC source table, declares dimensions (the grain you can slice by) and
# MAGIC measures (the aggregations), and becomes a governed dataset that Genie
# MAGIC and dashboards consume.
# MAGIC
# MAGIC This is the **second BI approach** — it does not replace the Power BI
# MAGIC serving layer, it serves the same gold tables natively. See
# MAGIC docs/bi_serving_layer.md.

# COMMAND ----------

CATALOG = "workspace"
GOLD_SCHEMA = "gold"
SEMANTIC_SCHEMA = "semantic"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SEMANTIC_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric view definition
# MAGIC
# MAGIC Sourced from the county-month price/market gold mart. Measures mirror
# MAGIC the pure-Python definitions in utils/financial_calcs.py so the BI layer
# MAGIC and the unit-tested logic stay in agreement.

# COMMAND ----------

metric_view_yaml = """
version: 0.1
source: workspace.gold.county_price_trends

dimensions:
  - name: County
    expr: county_name
  - name: MSA
    expr: msa
  - name: Month
    expr: period_month

measures:
  - name: Median Sale Price
    expr: MEDIAN(median_sale_price)
  - name: Homes Sold
    expr: SUM(homes_sold)
  - name: Active Listings
    expr: SUM(active_listings)
  - name: Months of Supply
    expr: SUM(active_listings) / NULLIF(AVG(trailing_3mo_avg_sales), 0)
  - name: Avg Days on Market
    expr: AVG(median_dom)
  - name: Price to Income Ratio
    expr: MEDIAN(median_sale_price) / NULLIF(MEDIAN(median_household_income), 0)
"""

# COMMAND ----------

# Create the metric view via SQL DDL. The YAML body is passed as the view
# definition. (Adjust to your workspace's current metric-view DDL syntax.)
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SEMANTIC_SCHEMA}.rdu_market_metrics
WITH METRICS
LANGUAGE YAML
AS $$
{metric_view_yaml}
$$
""")

print("Metric view created: "
      f"{CATALOG}.{SEMANTIC_SCHEMA}.rdu_market_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps (done in the Databricks UI, not code)
# MAGIC
# MAGIC 1. **AI/BI Dashboard**: New → Dashboard → add this metric view as a
# MAGIC    dataset → build visuals (county price trends, months-of-supply map,
# MAGIC    affordability page). Publish.
# MAGIC 2. **Genie Space**: New → Genie Space → attach the metric view →
# MAGIC    add curated sample questions, e.g.:
# MAGIC    - "Which RDU county had the highest price growth last year?"
# MAGIC    - "Show months of supply by county for the last 12 months."
# MAGIC    - "Which counties crossed a price-to-income ratio of 4?"
# MAGIC 3. Test Genie answers against the dashboard numbers — they must match,
# MAGIC    because both read this one metric view.
