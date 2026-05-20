# Raleigh-Durham Market Intelligence Lakehouse

A medallion-architecture data lakehouse on Databricks that turns public
housing data into market intelligence for the **Raleigh-Durham-Cary CSA**.
Built to be **RDU-focused now, NC-scalable later** — expanding to all 100
North Carolina counties changes one configuration value, not the schema.

> Built as a portfolio project to demonstrate end-to-end data engineering on
> Databricks: multi-source ingestion, medallion layering (bronze/silver/gold),
> CDC/incremental loads, orchestration, data-quality checks, and a BI layer.

---

## Why Raleigh-Durham

The Triangle is one of the clearest recent case studies in US housing:
rapid tech-driven in-migration, a supply squeeze, sharp appreciation, and a
fast slide in affordability. The market also splits cleanly into two MSAs
(Raleigh-Cary and Durham-Chapel Hill) and into very different sub-markets
(urban Wake vs. exurban Johnston), which makes the analytics genuinely
interesting rather than a single flat trend line.

## Geographic scope

Raleigh-Durham-Cary Combined Statistical Area — 8 counties:

| County | FIPS | MSA | Seat |
|---|---|---|---|
| Wake | 37183 | Raleigh-Cary | Raleigh |
| Durham | 37063 | Durham-Chapel Hill | Durham |
| Orange | 37135 | Durham-Chapel Hill | Hillsborough |
| Johnston | 37101 | Raleigh-Cary | Smithfield |
| Chatham | 37037 | Durham-Chapel Hill | Pittsboro |
| Franklin | 37069 | Raleigh-Cary | Louisburg |
| Granville | 37077 | Durham-Chapel Hill | Oxford |
| Person | 37145 | Durham-Chapel Hill | Roxboro |

Scope is controlled in `utils/geography_config.py`. Expanding to all of NC
means populating `NC_ALL_COUNTIES` and repointing `ACTIVE_SCOPE`.

## Architecture

```
 Sources            Bronze (raw)            Silver (conformed)        Gold (marts)
 -------            ------------            ------------------        ------------
 FHFA HPI           fhfa_hpi_county/metro   price_index               metro_health_monthly
 Redfin Tracker     redfin_county_tracker   sale_prices               county_price_trends
 FRED rates         fred_mortgage_rates     market_metrics            affordability_stress
 Census ACS         census_acs              geography_dim  (SCD)      inventory_dynamics
 HUD ZIP crosswalk  hud_zip_crosswalk       economic_indicators       zip_hotspots
 Wake/Durham deeds  county_assessor_sales   demographics_dim          assessor_vs_market   <- showpiece
```

Orchestrated as a monthly Databricks Workflow; Auto Loader handles the
incremental monthly Redfin/Census file drops; `MERGE INTO` handles CDC and
deduplication into silver.

Because the project runs on Databricks Free Edition (serverless, restricted
outbound internet), data acquisition runs *outside* Databricks in a scheduled
GitHub Action that fetches each source and uploads it to a Unity Catalog
volume; Auto Loader ingests from the volume. See `docs/ingestion_architecture.md`.

## Metrics captured

**Market temperature** — months of supply, days on market, list-to-sale
ratio, share sold over list, new-vs-pending.
**Price** — median sale price, ZHVI, the gap between them, YoY/MoM change
(3-month smoothed), price per sqft, price-tier breakdowns.
**Affordability** — price-to-income ratio, estimated monthly P&I payment at
current FRED rates, payment as % of median income, rent burden.
**Investment** — rent-to-price ratio, cap-rate proxy, buy-vs-rent breakeven.
**Sophisticated layer** — sub-market divergence (CoV of ZIP price changes),
anomaly flags (>2 std dev from 12-month trend), and the gap between
county-recorded sale prices and assessed values.

Metric formulas live in `utils/financial_calcs.py` and are unit-tested in
`tests/`. See `docs/methodology.md` for definitions and assumptions.

## Data sources (all free / public)

Price signals are grounded in **actual transactions and repeat-sales
methodology — no algorithmic home valuations are used anywhere.**

- FHFA House Price Index — repeat-sales appreciation, county & metro (the
  price-trend anchor; measures realized price change on the same homes)
- Redfin Data Center — median sale price, inventory, days on market,
  sale-to-list ratio, from MLS + public records (current transaction prices)
- FRED — 30-yr mortgage rate, permits, housing starts
- US Census ACS 5-Year — income, demographics, housing characteristics
- HUD USPS ZIP crosswalk — ZIP-to-county mapping
- Wake & Durham County tax records — parcel-level recorded sales (ground truth)

### Why no Zillow ZHVI
ZHVI is a model-derived estimate of what homes are *worth*, not a record of
what *sold*. This project deliberately anchors price on the FHFA repeat-sales
index (federal, methodologically transparent) and on actual Redfin/county
transaction data, so every price figure traces back to a real sale.

## Repository layout

```
rdu-lakehouse/
├── acquisition/    fetch (acquire.py) + upload (upload.py) + sources registry
├── .github/        GitHub Actions weekly acquisition workflow (CI automation)
├── notebooks/      bronze (0x), silver (1x), gold (2x), metric view (3x)
├── utils/          geography_config, financial_calcs, data_quality helpers
├── workflows/      Databricks Workflow job definition (monthly refresh)
├── tests/          pytest unit tests for the pure-Python metric functions
├── dashboard/      Power BI report built on the gold layer
└── docs/           architecture, ingestion, BI, methodology, scope
```

## Build status

- [x] Project scope + NC-scalable geography config
- [x] Affordability math + unit tests (6/6 passing)
- [x] Acquisition layer: sources registry + fetch/upload + GitHub Actions CI
- [x] Bronze: FHFA + Redfin via Auto Loader from volume
- [ ] Bronze: FRED, Census, Wake sales, Durham parcels (registry ready)
- [ ] Silver: conformed geography dimension + price/metric cleaning
- [ ] Gold: market health, price trends, affordability, assessor-vs-market
- [ ] Orchestration: monthly Databricks Workflow
- [ ] Serving layer 1: Power BI market intelligence report
- [ ] Serving layer 2: Databricks AI/BI (metric view + dashboard + Genie)

## Tech stack

Databricks (Free Edition), PySpark, Delta Lake, Auto Loader, Databricks
Workflows, Unity Catalog. Acquisition automated with GitHub Actions + the
Databricks CLI. Two serving layers: Power BI (external enterprise BI) and
Databricks AI/BI — metric views, AI/BI Dashboards, and Genie (platform-native,
no extra license). Python utilities tested with pytest. See
`docs/ingestion_architecture.md` and `docs/bi_serving_layer.md`.
