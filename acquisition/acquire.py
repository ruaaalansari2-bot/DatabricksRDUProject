"""
Acquisition layer — runs OUTSIDE Databricks (locally or in GitHub Actions).

For each enabled source in sources.py:
  1. fetch it (API GET, file GET, gzip GET, or paginated ArcGIS query)
  2. convert if needed (xlsx -> csv)
  3. write it to ./staging/<out path>

A separate step (upload.py) then pushes ./staging into the Databricks volume.
Keeping fetch and upload separate makes each independently testable and lets
you re-upload without re-downloading.

Why this lives outside Databricks: Free Edition serverless restricts outbound
internet to an allowlist, so the pipeline cannot fetch arbitrary URLs. This
acquisition layer does the internet-facing work; Databricks only reads the
volume. On a paid workspace the fetch could move into the pipeline.

Run:  python acquisition/acquire.py
Env:  FRED_API_KEY, CENSUS_API_KEY  (set as GitHub Actions secrets in CI)
"""
import io
import json
import os
import sys
import gzip
import time
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))
from sources import SOURCES  # noqa: E402

STAGING = Path(__file__).parent / "staging"
USER_AGENT = "rdu-lakehouse-acquisition/1.0 (portfolio project)"


def _http_get(url, params=None, binary=False, timeout=120):
    if params:
        url = url + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
    return data if binary else data.decode("utf-8")


def _write(rel_path, content, binary=False):
    dest = STAGING / rel_path
    dest.parent.mkdir(parents=True, exist_ok=True)
    mode = "wb" if binary else "w"
    with open(dest, mode) as f:
        f.write(content)
    size = dest.stat().st_size
    print(f"  wrote {rel_path} ({size:,} bytes)")
    return dest


def fetch_api(src):
    params = dict(src.get("params", {}))
    key_env = src.get("key_env")
    if key_env:
        key = os.environ.get(key_env)
        if not key:
            raise RuntimeError(f"Missing env var {key_env} for {src['name']}")
        params[src["key_param"]] = key
    body = _http_get(src["url"], params=params)
    # Validate it parses as JSON before we trust it.
    json.loads(body)
    _write(src["out"], body)


def fetch_file(src):
    body = _http_get(src["url"], binary=True)
    if src.get("convert_xlsx_to_csv"):
        body = _xlsx_bytes_to_csv_bytes(body)
    _write(src["out"], body, binary=True)


def fetch_file_gz(src):
    # Keep the gzip compressed — Spark/Auto Loader reads .gz natively, and it
    # keeps the volume small (matters for Free Edition).
    body = _http_get(src["url"], binary=True)
    _write(src["out"], body, binary=True)


def fetch_arcgis(src):
    """Paginated ArcGIS FeatureServer query. ArcGIS caps records per request
    (commonly 1000-2000), so page with resultOffset until exhausted."""
    all_features = []
    offset, page = 0, 2000
    while True:
        params = {
            "where": "1=1",
            "outFields": "*",
            "f": "geojson",
            "resultOffset": offset,
            "resultRecordCount": page,
        }
        body = _http_get(src["url"], params=params)
        gj = json.loads(body)
        feats = gj.get("features", [])
        all_features.extend(feats)
        print(f"    arcgis page offset={offset} -> {len(feats)} features")
        if len(feats) < page:
            break
        offset += page
        time.sleep(0.5)  # be polite to the public endpoint
    out = {"type": "FeatureCollection", "features": all_features}
    _write(src["out"], json.dumps(out))


def _xlsx_bytes_to_csv_bytes(xlsx_bytes):
    """Convert the first sheet of an xlsx to CSV. Uses openpyxl (pure Python,
    installable via pip — avoids the Maven Spark-Excel reader that Free Edition
    serverless cannot install)."""
    from openpyxl import load_workbook
    import csv
    wb = load_workbook(io.BytesIO(xlsx_bytes), read_only=True, data_only=True)
    ws = wb.active
    buf = io.StringIO()
    writer = csv.writer(buf)
    for row in ws.iter_rows(values_only=True):
        writer.writerow(["" if c is None else c for c in row])
    return buf.getvalue().encode("utf-8")


DISPATCH = {
    "api": fetch_api,
    "file": fetch_file,
    "file_gz": fetch_file_gz,
    "arcgis": fetch_arcgis,
}


def main():
    STAGING.mkdir(exist_ok=True)
    failures = []
    for src in SOURCES:
        if not src.get("enabled", True):
            print(f"- {src['name']}: skipped (disabled)")
            continue
        print(f"- {src['name']}: fetching ({src['kind']})")
        try:
            DISPATCH[src["kind"]](src)
        except Exception as e:                       # noqa: BLE001
            print(f"  FAILED: {e}")
            failures.append(src["name"])
    if failures:
        print(f"\nCompleted with failures: {failures}")
        sys.exit(1)
    print("\nAll enabled sources acquired into ./staging")


if __name__ == "__main__":
    main()
