# Ingestion Architecture — Acquisition + Auto Loader

This project runs on **Databricks Free Edition**, whose serverless compute
restricts outbound internet to an allowlist. The pipeline therefore cannot
fetch arbitrary source URLs from inside a notebook. The architecture splits
cleanly into two halves, with a **Unity Catalog volume** as the boundary.

```
GitHub Actions (weekly cron)            Databricks (serverless)
  acquire.py  -> fetch + convert          Auto Loader -> bronze
  upload.py   -> push to volume   ----->  (reads volume only)  -> silver -> gold
        |                                          ^
        +--------- UC volume (landing zone) -------+
```

## Why this split

Free Edition serverless cannot reach the internet for arbitrary hosts, and
UDFs cannot access the internet at all. So the internet-facing *acquisition*
work runs on GitHub's runners, not on Databricks. Databricks only ever reads
the volume. On a paid workspace with classic compute or configured egress,
the fetch could move inside the pipeline — the volume boundary stays the same.

This is a deliberate, defensible design, not a workaround forced by weakness:
it separates acquisition from ingestion, adds CI/CD and the Databricks CLI to
the stack, and keeps the Databricks side purely about transformation.

## Half 1 — Acquisition (runs in GitHub Actions)

Location: `acquisition/`

- `sources.py` — declarative registry. Each source is a dict describing how to
  fetch it (`api`, `file`, `file_gz`, `arcgis`). Add a source = add a dict.
- `acquire.py` — reads the registry, fetches each enabled source, converts
  xlsx -> csv where needed, writes to `acquisition/staging/<path>`.
- `upload.py` — pushes `staging/` into the Databricks volume via the
  Databricks CLI (`databricks fs cp --overwrite`).
- `.github/workflows/weekly-acquisition.yml` — runs both on a Monday cron and
  on manual dispatch.

### Secrets (set in GitHub repo settings -> Secrets and variables -> Actions)
- `FRED_API_KEY` — from fredaccount.stlouisfed.org
- `CENSUS_API_KEY` — from api.census.gov/data/key_signup.html
- `DATABRICKS_HOST` — your workspace URL
- `DATABRICKS_TOKEN` — a personal access token from the workspace
- `RDU_VOLUME_PATH` — e.g. `dbfs:/Volumes/workspace/landing/raw`

Keys never live in code — they are injected from CI secrets at runtime. This
is the security-best-practice answer to "where do you keep credentials."

## Half 2 — Ingestion (runs in Databricks)

Location: `notebooks/0x_bronze_*.py`

Each bronze notebook uses **Auto Loader** (`cloudFiles`) pointed at that
source's subfolder in the volume. Auto Loader:
- tracks already-processed files in a checkpoint, so re-runs ingest only new
  files (incremental load — a headline JD skill);
- infers and evolves schema via a schema location;
- runs with `trigger(availableNow=True)` so a scheduled job processes all new
  files then stops, rather than running as an always-on stream.

A Databricks Workflow runs the bronze notebooks, then silver, then gold, on a
schedule that matches the data (weekly for the fast tier).

## Cadence

Acquisition runs weekly. Sources publish at different rates — Redfin and FRED
weekly, Wake sales daily, FHFA quarterly, Census annually — so slow sources
simply re-land an identical file most weeks. Auto Loader will still register a
new file each week; if you want to skip unchanged content, compare a content
hash before upload in `upload.py` (future enhancement). For a portfolio
project, re-landing is harmless and keeps the design simple.

## Free Edition constraints that shaped this

- Serverless only; outbound internet restricted -> acquisition runs in CI.
- No Maven libraries -> xlsx converted with openpyxl (pip) in acquisition, not
  with a Spark-Excel Maven reader.
- Quota-limited compute -> weekly (not hourly) schedule; Redfin filtered on
  read to keep bronze small.
- DBFS limited -> use Unity Catalog volumes (this design already does).
- `.cache()` / SQL cache APIs unsupported on serverless -> not used.

## First-run checklist

1. Create the UC volume in the workspace (Catalog UI):
   `workspace.landing.raw` (catalog.schema.volume).
2. Get FRED + Census API keys.
3. Create a Databricks personal access token.
4. Add the five GitHub secrets above.
5. Trigger the workflow manually (Actions tab -> weekly-acquisition -> Run).
6. Confirm files land in the volume, then run the bronze notebooks.
7. Adjust column names in the bronze notebooks to match real headers.
