# Data Model — Silver & Gold

Design for the conformed (silver) and serving (gold) layers, built from the
13+ bronze sources. Silver CONFORMS (clean, type, dedupe, reconcile onto common
keys). Gold SERVES (business marts shaped by questions, read by both BI layers).

Common grain decision: monthly. Sub-monthly sources (Redfin weekly) roll up to
month; supra-monthly sources (FHFA quarterly, Census annual) are carried to
month per the documented time-grain rule (see docs/decisions_log.md when added).

## Silver layer

### Backbone dimensions (build first — everything joins to these)

**silver.dim_geography** — one row per county (per ZIP when expanded).
- county_fips (PK), county_name, cbsa_code, cbsa_name, state_fips
- redfin_region (e.g. "Wake County, NC"), realtor_fips  — crosswalk fields
- Source: geography_config.py + HUD ZIP crosswalk. The "Rosetta Stone" that
  lets every source's native geo encoding map to one key.

**silver.dim_metro** — one row per CBSA (2 metros for RDU).
- cbsa_code (PK), cbsa_name, county_fips_list
- Source: geography_config.CBSA. Gives FHFA metro data + metro analysis a home.

**silver.dim_date** — monthly grain.
- date_key (PK, month start), year, quarter, month, month_name
- Source: generated.

**silver.xwalk_zip_county** — ZIP -> county mapping with residential ratio.
- zip, county_fips, res_ratio
- Source: HUD ZIP crosswalk. Needed for ZIP rollups and zip_hotspots.

### Conformed subject-area tables (cleaned, keyed to dimensions)

**silver.home_prices** — price facts, one table, source-tagged.
- county_fips, date_key, source ('fhfa'|'redfin'|'realtor'),
  median_sale_price, median_list_price, hpi_index
- Sources: FHFA, Redfin, Realtor.

**silver.market_activity** — inventory & velocity.
- county_fips, date_key, source, active_listings, new_listings,
  median_dom, sale_to_list_ratio, price_per_sqft
- Sources: Redfin, Realtor.

**silver.rentals** — rent facts.
- county_fips, date_key, median_gross_rent
- Source: Census ACS (annual, carried to month).

**silver.demographics** — population & tenure.
- county_fips, year, median_household_income, median_home_value_selfreport,
  owner_occupied_units, renter_occupied_units, renter_share, total_population
- Source: Census ACS.

**silver.economic_indicators** — rates & macro/regional series.
- date_key, geography_scope ('national'|county_fips), indicator
  ('mortgage_30yr'|'mortgage_15yr'|'housing_starts'|'nc_permits'|
   'nc_unemployment'|'county_unemployment'), value
- Sources: FRED (national + NC), BLS LAUS (county).

**silver.building_permits** — supply pipeline.
- county_fips, date_key, single_family_units
- Source: Census BPS.

**silver.parcel_sales** — Wake transaction ground truth (cleaned).
- parcel_id, county_fips, sale_date, sale_price, assessed_value(*), address,
  heated_area, is_outlier_flag
- Source: Wake qualified sales. (*) assessed_value pending confirmation the
  file carries it; may require the separate Wake property file.

**silver.data_quality_log** — audit (written by utils/quality.py).
- table_name, row_count, null_rates, latest_data_ts, passed, checked_at

## Gold layer (marts shaped by questions; both BI layers read these)

**gold.county_market_monthly** — the workhorse. county x month.
- prices (sale/list/index), list_vs_sale_gap, inventory, months_of_supply,
  median_dom, sale_to_list_ratio, price_yoy, price_mom
- From: home_prices + market_activity.

**gold.affordability_monthly** — county x month.
- median_sale_price, mortgage_rate, median_income, est_monthly_payment,
  affordability_index, price_to_income_ratio
- From: home_prices + economic_indicators + demographics.
- Math must match utils/financial_calcs.py.

**gold.rent_vs_buy** — county x month.
- est_monthly_payment, median_gross_rent, rent_vs_buy_gap, rent_burden,
  rent_to_price_ratio
- From: affordability inputs + rentals.

**gold.market_health_score** — county x month, composite 0-100.
- inputs (supply, momentum, affordability, velocity) + weighted score
- From: county_market_monthly + affordability_monthly. Document weights.

**gold.supply_demand_signals** — county x month.
- single_family_permits, county_unemployment, price/inventory context
- From: building_permits + economic_indicators + county_market_monthly.

**gold.assessor_vs_market** — the differentiator. county/zip x period.
- parcel_median_price, market_median (redfin/realtor), hpi_index,
  assessed_vs_sale_gap(*), parcel_vs_market_gap
- From: parcel_sales vs home_prices. (*) needs assessed_value in silver.

**gold.zip_hotspots** — zip x quarter.
- zip, county_fips, price, appreciation_rank, anomaly_flag (>2 std dev)
- From: home_prices/market_activity at ZIP + xwalk_zip_county.

## Bronze -> silver -> gold dependency summary

- FHFA/Redfin/Realtor -> home_prices, market_activity -> county_market_monthly,
  assessor_vs_market, zip_hotspots
- FRED/BLS -> economic_indicators -> affordability_monthly, supply_demand_signals
- Census -> demographics, rentals -> affordability_monthly, rent_vs_buy
- Wake -> parcel_sales -> assessor_vs_market
- BPS -> building_permits -> supply_demand_signals
- HUD -> xwalk_zip_county, dim_geography -> zip_hotspots
- everything -> market_health_score

## Open decisions that gate silver (resolve before/while building)

1. Time-grain rule: forward-fill quarterly FHFA & annual Census into monthly
   rows, or populate only at native period? Pick one, apply consistently.
2. Mortgage rate per month: month-end, monthly average, or last-available?
3. Wake assessed_value: confirm the file has it, or add the property file.
4. Confirm which bronze tables actually populated (some source URLs were
   best-guesses and may have 404'd) before depending on them in silver.
