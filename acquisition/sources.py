"""
Source registry for the acquisition layer.

Each entry declares HOW to acquire one source. The acquisition script reads
this registry and processes every enabled source. Adding a source = adding a
dict here, not writing new code (mirrors the geography_config 'one switch'
philosophy on the ingestion side).

cadence is informational here — the GitHub Actions schedule decides what
actually runs. A weekly run processes everything; slow sources simply produce
an identical file most weeks.

kind:
  'api'         -> GET a JSON/CSV endpoint (key injected from env if key_env set)
  'file'        -> GET a static file URL as-is
  'file_gz'     -> GET a gzip file URL as-is (kept compressed; Spark reads .gz)
  'bps_monthly' -> Census Building Permits Survey (URL built from current date)
  'bls_laus'    -> BLS Local Area Unemployment Statistics (multi-series POST)
  'arcgis'      -> ArcGIS REST FeatureServer query, paginated, returns GeoJSON
"""

# RDU CSA county FIPS (kept here so the fetch can filter API pulls server-side
# where the API supports it, e.g. Census).
RDU_COUNTY_FIPS_3 = ["183", "063", "135", "101", "037", "069", "077", "145"]

SOURCES = [

    # ------------------------------------------------------------------ #
    # FRED — Federal Reserve Economic Data                                #
    # ------------------------------------------------------------------ #
    {
        "name": "fred_mortgage_30yr",
        "kind": "api",
        "cadence": "weekly",
        "url": "https://api.stlouisfed.org/fred/series/observations",
        "params": {"series_id": "MORTGAGE30US", "file_type": "json"},
        "key_env": "FRED_API_KEY",
        "key_param": "api_key",
        "out": "fred/mortgage_30yr.json",
        "enabled": True,
    },
    {
        "name": "fred_mortgage_15yr",
        "kind": "api",
        "cadence": "weekly",
        "url": "https://api.stlouisfed.org/fred/series/observations",
        "params": {"series_id": "MORTGAGE15US", "file_type": "json"},
        "key_env": "FRED_API_KEY",
        "key_param": "api_key",
        "out": "fred/mortgage_15yr.json",
        "enabled": True,
    },
    {
        "name": "fred_housing_starts",
        "kind": "api",
        "cadence": "monthly",
        "url": "https://api.stlouisfed.org/fred/series/observations",
        "params": {"series_id": "HOUST", "file_type": "json"},
        "key_env": "FRED_API_KEY",
        "key_param": "api_key",
        "out": "fred/housing_starts.json",
        "enabled": True,
    },
    {
        "name": "fred_housing_starts_sfr",
        "kind": "api",
        "cadence": "monthly",
        "url": "https://api.stlouisfed.org/fred/series/observations",
        "params": {"series_id": "HOUST1F", "file_type": "json"},
        "key_env": "FRED_API_KEY",
        "key_param": "api_key",
        "out": "fred/housing_starts_sfr.json",
        "enabled": True,
    },
    {
        "name": "fred_nc_unemployment",
        "kind": "api",
        "cadence": "monthly",
        "url": "https://api.stlouisfed.org/fred/series/observations",
        "params": {"series_id": "NCUR", "file_type": "json"},
        "key_env": "FRED_API_KEY",
        "key_param": "api_key",
        "out": "fred/nc_unemployment.json",
        "enabled": True,
    },
    {
        "name": "fred_nc_building_permits",
        "kind": "api",
        "cadence": "monthly",
        "url": "https://api.stlouisfed.org/fred/series/observations",
        "params": {"series_id": "NCBPPRIVSA", "file_type": "json"},
        "key_env": "FRED_API_KEY",
        "key_param": "api_key",
        "out": "fred/nc_building_permits.json",
        "enabled": True,
    },
    {
        "name": "fred_nc_median_listing_price",
        "kind": "api",
        "cadence": "monthly",
        "url": "https://api.stlouisfed.org/fred/series/observations",
        "params": {"series_id": "MEDLISPRINC", "file_type": "json"},
        "key_env": "FRED_API_KEY",
        "key_param": "api_key",
        "out": "fred/nc_median_listing_price.json",
        "enabled": True,
    },

    # ------------------------------------------------------------------ #
    # Census ACS — American Community Survey 5-Year Estimates             #
    # ------------------------------------------------------------------ #
    {
        "name": "census_acs_income",
        "kind": "api",
        "cadence": "annual",
        "url": "https://api.census.gov/data/2023/acs/acs5",
        "params": {
            "get": "NAME,B19013_001E",
            "for": "county:*",
            "in": "state:37",
        },
        "key_env": "CENSUS_API_KEY",
        "key_param": "key",
        "out": "census/acs_income_nc.json",
        "enabled": True,
    },
    {
        "name": "census_acs_housing",
        "kind": "api",
        "cadence": "annual",
        # B25077 = median home value, B25064 = median gross rent,
        # B25003 = tenure (owner vs renter), B01003 = total population
        "url": "https://api.census.gov/data/2023/acs/acs5",
        "params": {
            "get": "NAME,B25077_001E,B25064_001E,B25003_001E,B25003_002E,B25003_003E,B01003_001E",
            "for": "county:*",
            "in": "state:37",
        },
        "key_env": "CENSUS_API_KEY",
        "key_param": "key",
        "out": "census/acs_housing_nc.json",
        "enabled": True,
    },

    # ------------------------------------------------------------------ #
    # FHFA — House Price Index                                            #
    # ------------------------------------------------------------------ #
    {
        "name": "fhfa_hpi_county",
        "kind": "file",
        "cadence": "quarterly",
        "url": "https://www.fhfa.gov/hpi/download/annual/hpi_at_county.xlsx",
        "out": "fhfa/hpi_county.csv",
        "convert_xlsx_to_csv": True,
        "xlsx_header_row": 5,
        "xlsx_rename_columns": {
            "State": "state",
            "County": "county",
            "FIPS code": "fips_code",
            "Year": "year",
            "Annual Change (%)": "annual_change_pct",
            "HPI": "hpi",
            "HPI with 1990 base": "hpi_1990_base",
            "HPI with 2000 base": "hpi_2000_base",
        },
        "enabled": True,
    },

    # ------------------------------------------------------------------ #
    # Redfin — County Market Tracker                                      #
    # ------------------------------------------------------------------ #
    {
        "name": "redfin_county_tracker",
        "kind": "file_gz",
        "cadence": "weekly",
        "url": "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/county_market_tracker.tsv000.gz",
        "out": "redfin/county_market_tracker.tsv000.gz",
        "enabled": True,
    },

    # ------------------------------------------------------------------ #
    # Realtor.com — Inventory Core Metrics                                #
    # No auth required; served from public S3.                            #
    # ------------------------------------------------------------------ #
    {
        "name": "realtor_inventory_county",
        "kind": "file",
        "cadence": "monthly",
        "url": "https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_County_History.csv",
        "out": "realtor/inventory_county.csv",
        "enabled": True,
    },
    {
        "name": "realtor_inventory_zip",
        "kind": "file",
        "cadence": "monthly",
        "url": "https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_Zip.csv",
        "out": "realtor/inventory_zip.csv",
        "enabled": True,
    },

    # ------------------------------------------------------------------ #
    # Census Building Permits Survey (BPS)                                #
    # URL is built dynamically from the current date in acquire.py.       #
    # ------------------------------------------------------------------ #
    {
        "name": "census_bps_county",
        "kind": "bps_monthly",
        "cadence": "monthly",
        "base_url": "https://www2.census.gov/econ/bps/County/",
        "out_dir": "bps",
        "enabled": True,
    },

    # ------------------------------------------------------------------ #
    # BLS LAUS — Local Area Unemployment Statistics                       #
    # Series ID pattern: LAUCN{state_fips}{county_fips}0000000003         #
    # measure 03 = unemployment rate (not seasonally adjusted)            #
    # ------------------------------------------------------------------ #
    {
        "name": "bls_laus_rdu",
        "kind": "bls_laus",
        "cadence": "monthly",
        "series_ids": [
            "LAUCN371830000000003",  # Wake
            "LAUCN370630000000003",  # Durham
            "LAUCN371350000000003",  # Orange
            "LAUCN371010000000003",  # Johnston
            "LAUCN370370000000003",  # Chatham
            "LAUCN370690000000003",  # Franklin
            "LAUCN370770000000003",  # Granville
            "LAUCN371450000000003",  # Person
        ],
        "out": "bls/laus_rdu_counties.json",
        "enabled": True,
    },

    # ------------------------------------------------------------------ #
    # Wake County — Qualified Sales                                       #
    # ------------------------------------------------------------------ #
    {
        "name": "wake_qualified_sales",
        "kind": "file",
        "cadence": "weekly",
        "url": "https://services.wake.gov/realdata_extracts/Qualified_Sales_Past_24Months.xlsx",
        "out": "wake/qualified_sales.csv",
        "convert_xlsx_to_csv": True,
        "enabled": True,
    },

    # ------------------------------------------------------------------ #
    # Durham Parcels — ArcGIS (disabled until live URL confirmed)         #
    # ------------------------------------------------------------------ #
    {
        "name": "durham_parcels",
        "kind": "arcgis",
        "cadence": "monthly",
        "url": "https://services.arcgis.com/DURHAM_ORG_ID/arcgis/rest/services/Parcels/FeatureServer/0/query",
        "out": "durham/parcels.geojson",
        "enabled": False,
    },
]
