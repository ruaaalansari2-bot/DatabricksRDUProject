# Databricks notebook source
# MAGIC %md
# MAGIC # 51 - Gold: AI/BI Dashboard Query Reference
# MAGIC
# MAGIC **Role:** SQL query library for Databricks AI/BI (Lakeview) Dashboards.
# MAGIC Each cell is one named query. Copy the SQL into the Dashboard query editor
# MAGIC and pair it with the suggested widget type.
# MAGIC
# MAGIC ## How to create a Databricks AI/BI Dashboard
# MAGIC
# MAGIC 1. In your Databricks workspace, click **SQL** in the left sidebar
# MAGIC 2. Click **Dashboards** → **Create Dashboard** → **New Dashboard**
# MAGIC 3. Click **+ Add** → **Visualization** → paste the SQL below
# MAGIC 4. Select warehouse: `Serverless Starter` (or your SQL warehouse)
# MAGIC 5. Click **Run** to preview data, then choose the chart type noted in each cell
# MAGIC 6. Repeat for each query below — arrange widgets on the canvas
# MAGIC
# MAGIC **Important:** All gold queries filter `WHERE is_current = true`
# MAGIC because tables use SCD Type 2 (multiple versions per key).

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Dashboard 1 — Market Overview
# MAGIC ---

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query: price_trend
# MAGIC -- Widget: Line chart | X=date_key | Y=median_sale_price | Group by=county_name
# MAGIC -- Title: "RDU Median Sale Price by County"
# MAGIC
# MAGIC SELECT
# MAGIC     m.date_key,
# MAGIC     d.county_name,
# MAGIC     ROUND(m.median_sale_price, 0)  AS median_sale_price,
# MAGIC     ROUND(m.price_yoy, 1)          AS price_yoy_pct,
# MAGIC     ROUND(m.price_mom, 1)          AS price_mom_pct
# MAGIC FROM workspace.gold.county_market_monthly m
# MAGIC JOIN workspace.silver.dim_geography d
# MAGIC      ON d.county_fips = m.county_fips AND d.is_current = true
# MAGIC WHERE m.is_current = true
# MAGIC   AND m.date_key >= '2018-01-01'
# MAGIC   AND m.median_sale_price IS NOT NULL
# MAGIC ORDER BY d.county_name, m.date_key

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query: yoy_bar
# MAGIC -- Widget: Bar chart (horizontal) | X=price_yoy_pct | Y=county_name
# MAGIC -- Color: red (negative) / green (positive)
# MAGIC -- Title: "Price Year-over-Year Change % — Latest Month"
# MAGIC
# MAGIC SELECT
# MAGIC     d.county_name,
# MAGIC     ROUND(m.price_yoy, 1)         AS price_yoy_pct,
# MAGIC     ROUND(m.median_sale_price, 0) AS median_sale_price,
# MAGIC     m.date_key                    AS latest_month
# MAGIC FROM workspace.gold.county_market_monthly m
# MAGIC JOIN workspace.silver.dim_geography d
# MAGIC      ON d.county_fips = m.county_fips AND d.is_current = true
# MAGIC WHERE m.is_current = true
# MAGIC   AND m.date_key = (
# MAGIC       SELECT MAX(date_key)
# MAGIC       FROM workspace.gold.county_market_monthly
# MAGIC       WHERE is_current = true AND price_yoy IS NOT NULL)
# MAGIC ORDER BY price_yoy_pct DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query: inventory_velocity
# MAGIC -- Widget: Line chart | X=date_key | Y=months_of_supply (left), median_dom (right)
# MAGIC -- Add reference band at y=6 (balanced market)
# MAGIC -- Title: "Inventory & Velocity by County"
# MAGIC
# MAGIC SELECT
# MAGIC     m.date_key,
# MAGIC     d.county_name,
# MAGIC     ROUND(m.months_of_supply,   1) AS months_of_supply,
# MAGIC     ROUND(m.median_dom,         1) AS median_dom_days,
# MAGIC     ROUND(m.sale_to_list_ratio, 4) AS sale_to_list_ratio,
# MAGIC     ROUND(m.sold_above_list_pct,1) AS sold_above_list_pct,
# MAGIC     m.active_listings
# MAGIC FROM workspace.gold.county_market_monthly m
# MAGIC JOIN workspace.silver.dim_geography d
# MAGIC      ON d.county_fips = m.county_fips AND d.is_current = true
# MAGIC WHERE m.is_current = true
# MAGIC   AND m.date_key >= '2018-01-01'
# MAGIC ORDER BY d.county_name, m.date_key

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Dashboard 2 — Affordability
# MAGIC ---

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query: affordability_trend
# MAGIC -- Widget: Line chart | X=date_key | Y=affordability_index | Group by=county_name
# MAGIC -- Add reference line at y=100 (threshold: can just afford at 28% DTI)
# MAGIC -- Title: "Affordability Index (>100 = can afford median home)"
# MAGIC
# MAGIC SELECT
# MAGIC     a.date_key,
# MAGIC     d.county_name,
# MAGIC     ROUND(a.affordability_index,    1) AS affordability_index,
# MAGIC     ROUND(a.est_monthly_payment,    0) AS est_monthly_payment,
# MAGIC     ROUND(a.mortgage_rate,          2) AS mortgage_rate_pct,
# MAGIC     ROUND(a.price_to_income_ratio,  2) AS price_to_income_ratio,
# MAGIC     ROUND(a.median_income,          0) AS median_household_income,
# MAGIC     ROUND(a.median_sale_price,      0) AS median_sale_price
# MAGIC FROM workspace.gold.affordability_monthly a
# MAGIC JOIN workspace.silver.dim_geography d
# MAGIC      ON d.county_fips = a.county_fips AND d.is_current = true
# MAGIC WHERE a.is_current = true
# MAGIC   AND a.date_key >= '2018-01-01'
# MAGIC ORDER BY d.county_name, a.date_key

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query: affordability_snapshot
# MAGIC -- Widget: Table or Counter tiles | one row per county
# MAGIC -- Title: "Affordability — Latest Snapshot"
# MAGIC
# MAGIC SELECT
# MAGIC     d.county_name,
# MAGIC     a.date_key                                  AS as_of,
# MAGIC     ROUND(a.median_sale_price,     0)           AS median_sale_price,
# MAGIC     ROUND(a.est_monthly_payment,   0)           AS est_monthly_payment,
# MAGIC     ROUND(a.mortgage_rate,         2)           AS mortgage_rate_pct,
# MAGIC     ROUND(a.affordability_index,   1)           AS affordability_index,
# MAGIC     ROUND(a.price_to_income_ratio, 2)           AS price_to_income_ratio,
# MAGIC     CASE WHEN a.affordability_index >= 100
# MAGIC          THEN 'Affordable' ELSE 'Unaffordable'
# MAGIC     END                                        AS affordability_status
# MAGIC FROM workspace.gold.affordability_monthly a
# MAGIC JOIN workspace.silver.dim_geography d
# MAGIC      ON d.county_fips = a.county_fips AND d.is_current = true
# MAGIC WHERE a.is_current = true
# MAGIC   AND a.date_key = (
# MAGIC       SELECT MAX(date_key)
# MAGIC       FROM workspace.gold.affordability_monthly
# MAGIC       WHERE is_current = true)
# MAGIC ORDER BY a.affordability_index DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Dashboard 3 — Rent vs Buy
# MAGIC ---

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query: rent_vs_buy_trend
# MAGIC -- Widget: Line chart | X=date_key | Y=rent_vs_buy_gap | Group by=county_name
# MAGIC -- Add reference line at y=0
# MAGIC -- Title: "Monthly Cost Gap: Buying vs Renting (positive = buying costs more)"
# MAGIC
# MAGIC SELECT
# MAGIC     r.date_key,
# MAGIC     d.county_name,
# MAGIC     ROUND(r.rent_vs_buy_gap,      0) AS rent_vs_buy_gap,
# MAGIC     ROUND(r.rent_burden_pct,      1) AS rent_burden_pct,
# MAGIC     ROUND(r.rent_to_price_ratio,  4) AS rent_to_price_ratio,
# MAGIC     ROUND(r.median_gross_rent,    0) AS median_gross_rent,
# MAGIC     ROUND(r.est_monthly_payment,  0) AS est_monthly_payment
# MAGIC FROM workspace.gold.rent_vs_buy r
# MAGIC JOIN workspace.silver.dim_geography d
# MAGIC      ON d.county_fips = r.county_fips AND d.is_current = true
# MAGIC WHERE r.is_current = true
# MAGIC   AND r.median_gross_rent IS NOT NULL
# MAGIC   AND r.date_key >= '2018-01-01'
# MAGIC ORDER BY d.county_name, r.date_key

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query: rent_burden_snapshot
# MAGIC -- Widget: Bar chart | X=county_name | Y=rent_burden_pct
# MAGIC -- Add reference line at y=30 (cost-burdened threshold)
# MAGIC -- Title: "Rent Burden % by County — Latest (>30% = cost-burdened)"
# MAGIC
# MAGIC SELECT
# MAGIC     d.county_name,
# MAGIC     r.date_key                             AS as_of,
# MAGIC     ROUND(r.rent_burden_pct,   1)          AS rent_burden_pct,
# MAGIC     ROUND(r.median_gross_rent, 0)          AS median_gross_rent,
# MAGIC     ROUND(r.est_monthly_payment, 0)        AS est_monthly_payment,
# MAGIC     CASE WHEN r.rent_burden_pct > 30
# MAGIC          THEN 'Cost-Burdened' ELSE 'Within Range'
# MAGIC     END                                   AS burden_status
# MAGIC FROM workspace.gold.rent_vs_buy r
# MAGIC JOIN workspace.silver.dim_geography d
# MAGIC      ON d.county_fips = r.county_fips AND d.is_current = true
# MAGIC WHERE r.is_current = true
# MAGIC   AND r.median_gross_rent IS NOT NULL
# MAGIC   AND r.date_key = (
# MAGIC       SELECT MAX(date_key) FROM workspace.gold.rent_vs_buy
# MAGIC       WHERE is_current = true AND median_gross_rent IS NOT NULL)
# MAGIC ORDER BY rent_burden_pct DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Dashboard 4 — Market Health Score
# MAGIC ---

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query: health_score_trend
# MAGIC -- Widget: Line chart | X=date_key | Y=market_health_score | Group by=county_name
# MAGIC -- Title: "Market Health Score by County (0–100)"
# MAGIC
# MAGIC SELECT
# MAGIC     h.date_key,
# MAGIC     d.county_name,
# MAGIC     ROUND(h.market_health_score,  1) AS market_health_score,
# MAGIC     ROUND(h.dom_score,            1) AS dom_score,
# MAGIC     ROUND(h.sale_to_list_score,   1) AS sale_to_list_score,
# MAGIC     ROUND(h.supply_score,         1) AS supply_score,
# MAGIC     ROUND(h.momentum_score,       1) AS momentum_score,
# MAGIC     ROUND(h.affordability_score,  1) AS affordability_score
# MAGIC FROM workspace.gold.market_health_score h
# MAGIC JOIN workspace.silver.dim_geography d
# MAGIC      ON d.county_fips = h.county_fips AND d.is_current = true
# MAGIC WHERE h.is_current = true
# MAGIC   AND h.date_key >= '2018-01-01'
# MAGIC ORDER BY d.county_name, h.date_key

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query: health_score_snapshot
# MAGIC -- Widget: Counter tiles (one per county) | Value=market_health_score
# MAGIC -- Title: "Market Health Score — This Month"
# MAGIC
# MAGIC SELECT
# MAGIC     d.county_name,
# MAGIC     h.date_key                              AS as_of,
# MAGIC     ROUND(h.market_health_score, 1)         AS market_health_score,
# MAGIC     ROUND(h.momentum_score,      1)         AS momentum,
# MAGIC     ROUND(h.supply_score,        1)         AS supply,
# MAGIC     ROUND(h.affordability_score, 1)         AS affordability,
# MAGIC     ROUND(h.dom_score,           1)         AS velocity,
# MAGIC     CASE
# MAGIC         WHEN h.market_health_score >= 70 THEN 'Hot'
# MAGIC         WHEN h.market_health_score >= 45 THEN 'Balanced'
# MAGIC         ELSE 'Cool'
# MAGIC     END                                    AS market_condition
# MAGIC FROM workspace.gold.market_health_score h
# MAGIC JOIN workspace.silver.dim_geography d
# MAGIC      ON d.county_fips = h.county_fips AND d.is_current = true
# MAGIC WHERE h.is_current = true
# MAGIC   AND h.date_key = (
# MAGIC       SELECT MAX(date_key)
# MAGIC       FROM workspace.gold.market_health_score
# MAGIC       WHERE is_current = true)
# MAGIC ORDER BY market_health_score DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Dashboard 5 — Supply & Demand
# MAGIC ---

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query: permits_trend
# MAGIC -- Widget: Stacked bar | X=date_key | Y=single_family_units + multi_family_units
# MAGIC --         Group by=county_name | Stack by=unit_type
# MAGIC -- Title: "Building Permits Issued by County"
# MAGIC
# MAGIC SELECT
# MAGIC     s.date_key,
# MAGIC     d.county_name,
# MAGIC     s.single_family_units,
# MAGIC     s.multi_family_units,
# MAGIC     s.total_units_permitted,
# MAGIC     ROUND(s.county_unemployment_rate, 1) AS unemployment_rate_pct,
# MAGIC     ROUND(s.price_yoy,               1) AS price_yoy_pct,
# MAGIC     ROUND(s.months_of_supply,        1) AS months_of_supply
# MAGIC FROM workspace.gold.supply_demand_signals s
# MAGIC JOIN workspace.silver.dim_geography d
# MAGIC      ON d.county_fips = s.county_fips AND d.is_current = true
# MAGIC WHERE s.is_current = true
# MAGIC   AND s.date_key >= '2018-01-01'
# MAGIC ORDER BY d.county_name, s.date_key

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query: macro_context
# MAGIC -- Widget: Line chart (dual axis) | X=date_key
# MAGIC --         Left Y=county_unemployment_rate | Right Y=price_yoy
# MAGIC -- Title: "Unemployment vs Price Appreciation"
# MAGIC
# MAGIC SELECT
# MAGIC     s.date_key,
# MAGIC     d.county_name,
# MAGIC     ROUND(s.county_unemployment_rate, 1) AS unemployment_rate_pct,
# MAGIC     ROUND(s.price_yoy,               1) AS price_yoy_pct,
# MAGIC     ROUND(s.national_housing_starts,  0) AS national_housing_starts,
# MAGIC     ROUND(s.nc_building_permits,      0) AS nc_building_permits
# MAGIC FROM workspace.gold.supply_demand_signals s
# MAGIC JOIN workspace.silver.dim_geography d
# MAGIC      ON d.county_fips = s.county_fips AND d.is_current = true
# MAGIC WHERE s.is_current = true
# MAGIC   AND s.date_key >= '2018-01-01'
# MAGIC   AND s.county_unemployment_rate IS NOT NULL
# MAGIC ORDER BY d.county_name, s.date_key

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Dashboard 6 — ZIP Hotspots
# MAGIC ---

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query: zip_appreciation
# MAGIC -- Widget: Bar chart (horizontal, top N) | X=qoq_change_pct | Y=zip_name
# MAGIC --         Color by anomaly_flag
# MAGIC -- Title: "Top ZIP Codes by Quarterly Listing Price Appreciation"
# MAGIC
# MAGIC SELECT
# MAGIC     z.zip,
# MAGIC     z.zip_name,
# MAGIC     z.quarter_start,
# MAGIC     ROUND(z.median_listing_price,   0) AS median_listing_price,
# MAGIC     ROUND(z.qoq_price_change_pct,   1) AS qoq_change_pct,
# MAGIC     z.appreciation_rank,
# MAGIC     z.anomaly_flag,
# MAGIC     z.active_listing_count,
# MAGIC     ROUND(z.median_dom,             1) AS median_dom
# MAGIC FROM workspace.gold.zip_hotspots z
# MAGIC WHERE z.is_current = true
# MAGIC   AND z.quarter_start = (
# MAGIC       SELECT MAX(quarter_start) FROM workspace.gold.zip_hotspots WHERE is_current = true)
# MAGIC   AND z.qoq_price_change_pct IS NOT NULL
# MAGIC ORDER BY z.appreciation_rank
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query: zip_anomalies
# MAGIC -- Widget: Table | highlight anomaly_flag = true rows in red
# MAGIC -- Title: "Anomaly-Flagged ZIP Codes (>2 std dev from quarterly mean)"
# MAGIC
# MAGIC SELECT
# MAGIC     z.quarter_start,
# MAGIC     z.zip,
# MAGIC     z.zip_name,
# MAGIC     ROUND(z.median_listing_price, 0)  AS median_listing_price,
# MAGIC     ROUND(z.qoq_price_change_pct, 1)  AS qoq_change_pct,
# MAGIC     z.appreciation_rank,
# MAGIC     z.active_listing_count
# MAGIC FROM workspace.gold.zip_hotspots z
# MAGIC WHERE z.is_current = true
# MAGIC   AND z.anomaly_flag = true
# MAGIC ORDER BY z.quarter_start DESC, z.appreciation_rank

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Master Summary — All Counties, Latest Month
# MAGIC ---

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query: master_summary
# MAGIC -- Widget: Table with conditional formatting
# MAGIC -- health_score: color green (≥70) / yellow (45-70) / red (<45)
# MAGIC -- affordability_index: color green (≥100) / red (<100)
# MAGIC -- rent_burden_pct: color red (>30)
# MAGIC -- Title: "RDU Market Intelligence — Current Snapshot"
# MAGIC
# MAGIC SELECT
# MAGIC     d.county_name                               AS county,
# MAGIC     m.date_key                                  AS as_of,
# MAGIC     ROUND(m.median_sale_price,     0)           AS median_sale_price,
# MAGIC     ROUND(m.price_yoy,             1)           AS price_yoy_pct,
# MAGIC     ROUND(m.months_of_supply,      1)           AS months_of_supply,
# MAGIC     ROUND(m.median_dom,            0)           AS median_dom_days,
# MAGIC     ROUND(m.sale_to_list_ratio,    3)           AS sale_to_list,
# MAGIC     ROUND(a.affordability_index,   1)           AS affordability_index,
# MAGIC     ROUND(a.est_monthly_payment,   0)           AS monthly_payment,
# MAGIC     ROUND(r.median_gross_rent,     0)           AS median_gross_rent,
# MAGIC     ROUND(r.rent_burden_pct,       1)           AS rent_burden_pct,
# MAGIC     ROUND(h.market_health_score,   1)           AS health_score,
# MAGIC     CASE
# MAGIC         WHEN h.market_health_score >= 70 THEN 'Hot'
# MAGIC         WHEN h.market_health_score >= 45 THEN 'Balanced'
# MAGIC         ELSE 'Cool'
# MAGIC     END                                        AS market_condition
# MAGIC FROM workspace.gold.county_market_monthly m
# MAGIC JOIN workspace.silver.dim_geography d
# MAGIC      ON d.county_fips = m.county_fips AND d.is_current = true
# MAGIC LEFT JOIN workspace.gold.affordability_monthly a
# MAGIC      ON a.county_fips = m.county_fips AND a.date_key = m.date_key AND a.is_current = true
# MAGIC LEFT JOIN workspace.gold.rent_vs_buy r
# MAGIC      ON r.county_fips = m.county_fips AND r.date_key = m.date_key AND r.is_current = true
# MAGIC LEFT JOIN workspace.gold.market_health_score h
# MAGIC      ON h.county_fips = m.county_fips AND h.date_key = m.date_key AND h.is_current = true
# MAGIC WHERE m.is_current = true
# MAGIC   AND m.date_key = (
# MAGIC       SELECT MAX(date_key) FROM workspace.gold.county_market_monthly WHERE is_current = true)
# MAGIC ORDER BY health_score DESC
