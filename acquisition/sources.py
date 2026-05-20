"""
Source registry for the acquisition layer.

Each entry declares HOW to acquire one source. The acquisition script reads
this registry and processes every enabled source. Adding a source = adding a
dict here, not writing new code (mirrors the geography_config 'one switch'
philosophy on the ingestion side).

cadence is informational here — the GitHub Actions schedule decides what
actually runs. A weekly run processes everything; slow sources simply produce
an identical file most weeks, which Auto Loader will skip by content hash if
unchanged (see autoloader note in the bronze notebook).

kind:
  'api'       -> GET a JSON/CSV endpoint (key injected from env if key_env set)
  'file'      -> GET a static file URL as-is
  'file_gz'   -> GET a gzip file URL as-is (kept compressed; Spark reads .gz)
  'arcgis'    -> ArcGIS REST FeatureServer query, paginated, returns GeoJSON
"""

# RDU CSA county FIPS (kept here so the fetch can filter API pulls server-side
# where the API supports it, e.g. Census).
RDU_COUNTY_FIPS_3 = ["183", "063", "135", "101", "037", "069", "077", "145"]

SOURCES = [
    {
        "name": "fred_mortgage_30yr",
        "kind": "api",
        "cadence": "weekly",
        "url": "https://api.stlouisfed.org/fred/series/observations",
        "params": {
            "series_id": "MORTGAGE30US",
            "file_type": "json",
        },
        "key_env": "FRED_API_KEY",      # appended as &api_key=...
        "key_param": "api_key",
        "out": "fred/mortgage_30yr.json",
        "enabled": True,
    },
    {
        "name": "census_acs_income",
        "kind": "api",
        "cadence": "annual",
        # ACS 5-year, median household income (B19013_001E) by county in NC (state 37)
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
        "name": "fhfa_hpi_county",
        "kind": "file",
        "cadence": "quarterly",
        # Confirm the exact current filename on fhfa.gov before first run.
        "url": "https://www.fhfa.gov/hpi/download/annually/HPI_AT_BDL_county.csv",
        "out": "fhfa/hpi_county.csv",
        "enabled": True,
    },
    {
        "name": "redfin_county_tracker",
        "kind": "file_gz",
        "cadence": "weekly",
        "url": "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/county_market_tracker.tsv000.gz",
        "out": "redfin/county_market_tracker.tsv000.gz",
        "enabled": True,
    },
    {
        "name": "wake_qualified_sales",
        "kind": "file",
        "cadence": "weekly",
        # Wake publishes a qualified (arms-length) sales file. If it is xlsx,
        # the fetch step converts it to CSV (see acquire.py convert step).
        "url": "https://www.wake.gov/documents/RealEstateData/Qualified_Sales.xlsx",
        "out": "wake/qualified_sales.csv",   # .csv because we convert on fetch
        "convert_xlsx_to_csv": True,
        "enabled": True,
    },
    {
        "name": "durham_parcels",
        "kind": "arcgis",
        "cadence": "monthly",
        # Confirm the live FeatureServer layer URL from Durham's open-data portal.
        "url": "https://services.arcgis.com/DURHAM_ORG_ID/arcgis/rest/services/Parcels/FeatureServer/0/query",
        "out": "durham/parcels.geojson",
        "enabled": False,   # enable once the live layer URL + sale fields confirmed
    },
]
