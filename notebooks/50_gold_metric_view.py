# Databricks notebook source
# MAGIC %md
# MAGIC # 50 - Gold: Interactive Metric View (Plotly)
# MAGIC
# MAGIC **Role:** Self-contained visual report over all gold tables.
# MAGIC Run this notebook end-to-end to produce interactive Plotly charts
# MAGIC without needing Databricks SQL / AI-BI Dashboards.
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. Market Overview      — price trends, inventory, velocity
# MAGIC 2. Affordability        — payment model, income ratios, index
# MAGIC 3. Rent vs Buy          — gap, burden, ratio trend
# MAGIC 4. Market Health Score  — composite score + component radar
# MAGIC 5. Supply & Demand      — permits pipeline + unemployment
# MAGIC 6. ZIP Hotspots         — appreciation rank + anomaly flags

# COMMAND ----------

import plotly.express     as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

CATALOG = "workspace"
GOLD    = f"{CATALOG}.gold"

COUNTY_NAMES = {
    "37183": "Wake",
    "37063": "Durham",
    "37135": "Orange",
    "37101": "Johnston",
    "37037": "Chatham",
    "37069": "Franklin",
    "37077": "Granville",
    "37145": "Person",
}

COLOR_MAP = {
    "Wake":      "#1f77b4",
    "Durham":    "#ff7f0e",
    "Orange":    "#2ca02c",
    "Johnston":  "#d62728",
    "Chatham":   "#9467bd",
    "Franklin":  "#8c564b",
    "Granville": "#e377c2",
    "Person":    "#7f7f7f",
}

def show(fig):
    displayHTML(fig.to_html(include_plotlyjs="cdn", full_html=False))

def label_county(df, col="county_fips"):
    df = df.copy()
    df["county"] = df[col].map(COUNTY_NAMES).fillna(df[col])
    return df

# COMMAND ----------
# MAGIC %md ## 1 · Market Overview

# COMMAND ----------

cmm = (spark.sql(f"""
    SELECT county_fips, date_key, median_sale_price, median_list_price,
           price_yoy, price_mom, months_of_supply, median_dom,
           sale_to_list_ratio, sold_above_list_pct, active_listings
    FROM {GOLD}.county_market_monthly
    WHERE is_current = true AND date_key >= '2018-01-01'
    ORDER BY county_fips, date_key
""").toPandas())
cmm = label_county(cmm)
cmm["date_key"] = pd.to_datetime(cmm["date_key"])

# ── 1a: Median Sale Price trend ───────────────────────────────────────────────
fig = px.line(
    cmm.dropna(subset=["median_sale_price"]),
    x="date_key", y="median_sale_price", color="county",
    color_discrete_map=COLOR_MAP,
    title="Median Sale Price by County (Monthly)",
    labels={"date_key": "", "median_sale_price": "Median Sale Price ($)", "county": "County"},
    template="plotly_white",
)
fig.update_layout(hovermode="x unified", yaxis_tickformat="$,.0f")
show(fig)

# COMMAND ----------

# ── 1b: Year-over-year price change — latest available month ──────────────────
latest_yoy = (cmm.dropna(subset=["price_yoy"])
              .sort_values("date_key")
              .groupby("county", as_index=False)
              .last()[["county", "date_key", "price_yoy"]])

fig = px.bar(
    latest_yoy.sort_values("price_yoy", ascending=True),
    x="price_yoy", y="county", orientation="h",
    color="price_yoy",
    color_continuous_scale=["#d62728", "#ffffff", "#2ca02c"],
    color_continuous_midpoint=0,
    title=f"Price Year-over-Year Change % — Latest Month",
    labels={"price_yoy": "YoY Change (%)", "county": ""},
    template="plotly_white",
    text="price_yoy",
)
fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
fig.update_layout(coloraxis_showscale=False)
show(fig)

# COMMAND ----------

# ── 1c: Months of supply + median DOM (dual axis) ─────────────────────────────
wake = cmm[cmm["county"] == "Wake"].dropna(subset=["months_of_supply", "median_dom"])

fig = make_subplots(specs=[[{"secondary_y": True}]])
fig.add_trace(go.Scatter(
    x=wake["date_key"], y=wake["months_of_supply"],
    name="Months of Supply", line=dict(color="#1f77b4")), secondary_y=False)
fig.add_trace(go.Scatter(
    x=wake["date_key"], y=wake["median_dom"],
    name="Median DOM", line=dict(color="#ff7f0e", dash="dot")), secondary_y=True)
fig.add_hline(y=6, line_dash="dash", line_color="gray",
              annotation_text="Balanced market (6 mo)", secondary_y=False)
fig.update_layout(title="Wake County — Months of Supply vs Median Days on Market",
                  template="plotly_white", hovermode="x unified")
fig.update_yaxes(title_text="Months of Supply", secondary_y=False)
fig.update_yaxes(title_text="Median DOM (days)", secondary_y=True)
show(fig)

# COMMAND ----------
# MAGIC %md ## 2 · Affordability

# COMMAND ----------

afford = (spark.sql(f"""
    SELECT county_fips, date_key, affordability_index, price_to_income_ratio,
           est_monthly_payment, mortgage_rate, median_income, median_sale_price
    FROM {GOLD}.affordability_monthly
    WHERE is_current = true AND date_key >= '2018-01-01'
    ORDER BY county_fips, date_key
""").toPandas())
afford = label_county(afford)
afford["date_key"] = pd.to_datetime(afford["date_key"])

# ── 2a: Affordability index over time ─────────────────────────────────────────
fig = px.line(
    afford.dropna(subset=["affordability_index"]),
    x="date_key", y="affordability_index", color="county",
    color_discrete_map=COLOR_MAP,
    title="Affordability Index by County (100 = median HH can just afford median home)",
    labels={"date_key": "", "affordability_index": "Affordability Index", "county": "County"},
    template="plotly_white",
)
fig.add_hline(y=100, line_dash="dash", line_color="red",
              annotation_text="Affordability threshold (100)")
fig.update_layout(hovermode="x unified")
show(fig)

# COMMAND ----------

# ── 2b: Est monthly payment vs mortgage rate (Wake) ───────────────────────────
wake_afford = afford[afford["county"] == "Wake"].dropna(subset=["est_monthly_payment"])

fig = make_subplots(specs=[[{"secondary_y": True}]])
fig.add_trace(go.Scatter(
    x=wake_afford["date_key"], y=wake_afford["est_monthly_payment"],
    name="Est. Monthly Payment", fill="tozeroy",
    line=dict(color="#1f77b4")), secondary_y=False)
fig.add_trace(go.Scatter(
    x=wake_afford["date_key"], y=wake_afford["mortgage_rate"],
    name="30yr Mortgage Rate (%)", line=dict(color="#d62728", dash="dot")), secondary_y=True)
fig.update_layout(title="Wake County — Estimated Monthly Payment vs Mortgage Rate",
                  template="plotly_white", hovermode="x unified")
fig.update_yaxes(title_text="Monthly Payment ($)", tickformat="$,.0f", secondary_y=False)
fig.update_yaxes(title_text="Mortgage Rate (%)", secondary_y=True)
show(fig)

# COMMAND ----------

# ── 2c: Price-to-income ratio — latest snapshot bar ───────────────────────────
latest_pti = (afford.dropna(subset=["price_to_income_ratio"])
              .sort_values("date_key")
              .groupby("county", as_index=False)
              .last()[["county", "price_to_income_ratio"]])

fig = px.bar(
    latest_pti.sort_values("price_to_income_ratio", ascending=False),
    x="county", y="price_to_income_ratio",
    color="county", color_discrete_map=COLOR_MAP,
    title="Price-to-Income Ratio by County — Latest Snapshot",
    labels={"price_to_income_ratio": "Price / Annual Income", "county": ""},
    template="plotly_white", text="price_to_income_ratio",
)
fig.add_hline(y=5, line_dash="dash", line_color="orange",
              annotation_text="Historically elevated threshold (5x)")
fig.update_traces(texttemplate="%{text:.1f}x", textposition="outside", showlegend=False)
show(fig)

# COMMAND ----------
# MAGIC %md ## 3 · Rent vs Buy

# COMMAND ----------

rvb = (spark.sql(f"""
    SELECT county_fips, date_key, rent_vs_buy_gap, rent_burden_pct,
           rent_to_price_ratio, median_gross_rent, est_monthly_payment
    FROM {GOLD}.rent_vs_buy
    WHERE is_current = true AND median_gross_rent IS NOT NULL AND date_key >= '2018-01-01'
    ORDER BY county_fips, date_key
""").toPandas())
rvb = label_county(rvb)
rvb["date_key"] = pd.to_datetime(rvb["date_key"])

# ── 3a: Rent vs Buy gap over time ─────────────────────────────────────────────
fig = px.line(
    rvb.dropna(subset=["rent_vs_buy_gap"]),
    x="date_key", y="rent_vs_buy_gap", color="county",
    color_discrete_map=COLOR_MAP,
    title="Rent vs Buy Gap (positive = buying costs more per month than renting)",
    labels={"date_key": "", "rent_vs_buy_gap": "Gap ($/month)", "county": "County"},
    template="plotly_white",
)
fig.add_hline(y=0, line_dash="dash", line_color="gray")
fig.update_layout(hovermode="x unified", yaxis_tickformat="$,.0f")
show(fig)

# COMMAND ----------

# ── 3b: Monthly payment vs gross rent — latest month grouped bar ──────────────
latest_rvb = (rvb.sort_values("date_key")
              .groupby("county", as_index=False)
              .last()[["county", "median_gross_rent", "est_monthly_payment"]])

fig = go.Figure()
fig.add_trace(go.Bar(name="Median Gross Rent",
    x=latest_rvb["county"], y=latest_rvb["median_gross_rent"],
    marker_color="#2ca02c"))
fig.add_trace(go.Bar(name="Est. Monthly Mortgage Payment",
    x=latest_rvb["county"], y=latest_rvb["est_monthly_payment"],
    marker_color="#1f77b4"))
fig.update_layout(
    barmode="group",
    title="Rent vs Monthly Mortgage Payment — Latest Month",
    yaxis_tickformat="$,.0f",
    template="plotly_white",
    xaxis_title="", yaxis_title="$/month",
)
show(fig)

# COMMAND ----------
# MAGIC %md ## 4 · Market Health Score

# COMMAND ----------

mhs = (spark.sql(f"""
    SELECT county_fips, date_key, market_health_score,
           dom_score, sale_to_list_score, supply_score,
           momentum_score, affordability_score
    FROM {GOLD}.market_health_score
    WHERE is_current = true AND date_key >= '2018-01-01'
    ORDER BY county_fips, date_key
""").toPandas())
mhs = label_county(mhs)
mhs["date_key"] = pd.to_datetime(mhs["date_key"])

# ── 4a: Market health score over time ─────────────────────────────────────────
fig = px.line(
    mhs.dropna(subset=["market_health_score"]),
    x="date_key", y="market_health_score", color="county",
    color_discrete_map=COLOR_MAP,
    title="Market Health Score by County (0–100, higher = hotter / more active)",
    labels={"date_key": "", "market_health_score": "Score", "county": "County"},
    template="plotly_white",
)
fig.update_layout(hovermode="x unified", yaxis_range=[0, 100])
show(fig)

# COMMAND ----------

# ── 4b: Component scores — radar chart for latest month ───────────────────────
latest_mhs = (mhs.sort_values("date_key")
              .groupby("county", as_index=False)
              .last())

categories = ["DOM Speed", "Sale-to-List", "Supply", "Momentum", "Affordability"]
score_cols  = ["dom_score", "sale_to_list_score", "supply_score", "momentum_score", "affordability_score"]

fig = go.Figure()
for _, row in latest_mhs.iterrows():
    vals = [row[c] for c in score_cols]
    vals_closed = vals + [vals[0]]
    cats_closed = categories + [categories[0]]
    fig.add_trace(go.Scatterpolar(
        r=vals_closed, theta=cats_closed,
        fill="toself", name=row["county"],
        line_color=COLOR_MAP.get(row["county"], None)))

fig.update_layout(
    polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
    title="Market Health Components — Latest Month (Radar)",
    template="plotly_white",
    showlegend=True,
)
show(fig)

# COMMAND ----------

# ── 4c: Health score heatmap (county × month) ─────────────────────────────────
heatmap_df = (mhs.dropna(subset=["market_health_score"])
              .assign(month=lambda d: d["date_key"].dt.to_period("M").astype(str))
              .pivot_table(index="county", columns="month",
                           values="market_health_score", aggfunc="mean"))

fig = px.imshow(
    heatmap_df,
    color_continuous_scale="RdYlGn",
    zmin=0, zmax=100,
    title="Market Health Score Heatmap — County × Month",
    labels={"color": "Score"},
    aspect="auto",
)
fig.update_layout(xaxis_tickangle=-45)
show(fig)

# COMMAND ----------
# MAGIC %md ## 5 · Supply & Demand

# COMMAND ----------

sds = (spark.sql(f"""
    SELECT county_fips, date_key,
           single_family_units, multi_family_units, total_units_permitted,
           county_unemployment_rate, price_yoy, months_of_supply,
           national_housing_starts, nc_building_permits
    FROM {GOLD}.supply_demand_signals
    WHERE is_current = true AND date_key >= '2018-01-01'
    ORDER BY county_fips, date_key
""").toPandas())
sds = label_county(sds)
sds["date_key"] = pd.to_datetime(sds["date_key"])

# ── 5a: Building permits stacked bar — Wake County ────────────────────────────
wake_sds = sds[sds["county"] == "Wake"].dropna(subset=["single_family_units"])

fig = go.Figure()
fig.add_trace(go.Bar(name="Single Family",
    x=wake_sds["date_key"], y=wake_sds["single_family_units"],
    marker_color="#1f77b4"))
fig.add_trace(go.Bar(name="Multi-Family",
    x=wake_sds["date_key"], y=wake_sds["multi_family_units"],
    marker_color="#ff7f0e"))
fig.update_layout(
    barmode="stack",
    title="Wake County — Monthly Building Permits Issued",
    template="plotly_white",
    xaxis_title="", yaxis_title="Units Permitted",
    hovermode="x unified",
)
show(fig)

# COMMAND ----------

# ── 5b: Unemployment rate vs price YoY — Wake ─────────────────────────────────
wake_macro = wake_sds.dropna(subset=["county_unemployment_rate", "price_yoy"])

fig = make_subplots(specs=[[{"secondary_y": True}]])
fig.add_trace(go.Scatter(
    x=wake_macro["date_key"], y=wake_macro["county_unemployment_rate"],
    name="Unemployment Rate (%)", line=dict(color="#d62728")), secondary_y=False)
fig.add_trace(go.Scatter(
    x=wake_macro["date_key"], y=wake_macro["price_yoy"],
    name="Price YoY (%)", line=dict(color="#2ca02c", dash="dot")), secondary_y=True)
fig.update_layout(title="Wake County — Unemployment vs Price Appreciation (YoY)",
                  template="plotly_white", hovermode="x unified")
fig.update_yaxes(title_text="Unemployment Rate (%)", secondary_y=False)
fig.update_yaxes(title_text="Price YoY (%)", secondary_y=True)
show(fig)

# COMMAND ----------
# MAGIC %md ## 6 · ZIP Hotspots

# COMMAND ----------

zh = (spark.sql(f"""
    SELECT zip, zip_name, quarter_start, median_listing_price,
           qoq_price_change_pct, appreciation_rank, anomaly_flag,
           active_listing_count, median_dom
    FROM {GOLD}.zip_hotspots
    WHERE is_current = true
    ORDER BY quarter_start DESC, appreciation_rank
""").toPandas())
zh["quarter_start"] = pd.to_datetime(zh["quarter_start"])

latest_qtr = zh["quarter_start"].max()
zh_latest  = zh[zh["quarter_start"] == latest_qtr].copy()

# ── 6a: Top 15 ZIPs by quarterly appreciation ─────────────────────────────────
top15 = zh_latest.dropna(subset=["qoq_price_change_pct"]).nlargest(15, "qoq_price_change_pct")

fig = px.bar(
    top15.sort_values("qoq_price_change_pct"),
    x="qoq_price_change_pct", y="zip_name", orientation="h",
    color="anomaly_flag",
    color_discrete_map={True: "#d62728", False: "#1f77b4"},
    title=f"Top 15 ZIP Codes by Listing Price Appreciation — Q{latest_qtr.quarter} {latest_qtr.year}",
    labels={"qoq_price_change_pct": "QoQ Change (%)", "zip_name": "", "anomaly_flag": "Anomaly"},
    template="plotly_white", text="qoq_price_change_pct",
)
fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
show(fig)

# COMMAND ----------

# ── 6b: Anomaly-flagged ZIPs over time ────────────────────────────────────────
anomaly_trend = (zh[zh["anomaly_flag"] == True]
                 .groupby("quarter_start", as_index=False)
                 .agg(anomaly_count=("zip", "count"),
                      avg_qoq=("qoq_price_change_pct", "mean")))

fig = make_subplots(specs=[[{"secondary_y": True}]])
fig.add_trace(go.Bar(
    x=anomaly_trend["quarter_start"], y=anomaly_trend["anomaly_count"],
    name="Anomaly ZIP Count", marker_color="#d62728"), secondary_y=False)
fig.add_trace(go.Scatter(
    x=anomaly_trend["quarter_start"], y=anomaly_trend["avg_qoq"],
    name="Avg Anomaly QoQ (%)", line=dict(color="#ff7f0e")), secondary_y=True)
fig.update_layout(
    title="Anomaly-Flagged ZIP Codes Over Time (>2 std dev from quarterly mean)",
    template="plotly_white", hovermode="x unified")
fig.update_yaxes(title_text="# Anomaly ZIPs", secondary_y=False)
fig.update_yaxes(title_text="Avg QoQ Change (%)", secondary_y=True)
show(fig)

# COMMAND ----------
# MAGIC %md ## Summary — Latest Snapshot Across All Counties

# COMMAND ----------

summary = spark.sql(f"""
SELECT
    m.county_fips,
    d.county_name,
    m.date_key                              AS latest_month,
    ROUND(m.median_sale_price, 0)           AS median_sale_price,
    ROUND(m.price_yoy, 1)                   AS price_yoy_pct,
    ROUND(m.months_of_supply, 1)            AS months_of_supply,
    ROUND(m.median_dom, 0)                  AS median_dom,
    ROUND(a.affordability_index, 1)         AS affordability_index,
    ROUND(a.est_monthly_payment, 0)         AS monthly_payment,
    ROUND(h.market_health_score, 1)         AS health_score
FROM {GOLD}.county_market_monthly m
JOIN workspace.silver.dim_geography d
     ON d.county_fips = m.county_fips AND d.is_current = true
LEFT JOIN {GOLD}.affordability_monthly a
     ON a.county_fips = m.county_fips AND a.date_key = m.date_key AND a.is_current = true
LEFT JOIN {GOLD}.market_health_score h
     ON h.county_fips = m.county_fips AND h.date_key = m.date_key AND h.is_current = true
WHERE m.is_current = true
  AND m.date_key = (SELECT MAX(date_key) FROM {GOLD}.county_market_monthly WHERE is_current = true)
ORDER BY health_score DESC
""")

display(summary)
