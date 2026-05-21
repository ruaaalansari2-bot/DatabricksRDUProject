"""
Upload layer — pushes ./staging into the Databricks Unity Catalog volume.

Uses the Databricks CLI (`databricks fs cp`) which authenticates from env vars
DATABRICKS_HOST and DATABRICKS_TOKEN (set as GitHub Actions secrets). The CLI
talks to the Databricks REST API from outside — it does NOT run on serverless,
so the Free Edition internet restriction does not apply to it.

The target volume path mirrors the staging layout, so a source landing at
staging/redfin/county_market_tracker.tsv000.gz goes to
<VOLUME>/redfin/county_market_tracker.tsv000.gz . Auto Loader watches each
source subfolder.

Run:  python acquisition/upload.py
Env:  DATABRICKS_HOST, DATABRICKS_TOKEN, RDU_VOLUME_PATH
      (RDU_VOLUME_PATH e.g. dbfs:/Volumes/workspace/landing/raw  -- the UC
       volume path; confirm yours in the workspace Catalog UI.)
"""
import os
import subprocess
import sys
from pathlib import Path

STAGING = Path(__file__).parent / "staging"


def main():
    volume = os.environ.get("RDU_VOLUME_PATH")
    if not volume:
        print("Missing env var RDU_VOLUME_PATH")
        sys.exit(1)
    if not STAGING.exists():
        print("No ./staging directory — run acquire.py first")
        sys.exit(1)

    uploaded = 0
    for path in sorted(STAGING.rglob("*")):
        if path.is_dir():
            continue
        rel = path.relative_to(STAGING).as_posix()
        target = f"{volume.rstrip('/')}/{rel}"
        target_dir = target.rsplit("/", 1)[0]
        print(f"uploading {rel} -> {target}")
        # Ensure the destination subfolder exists (cp won't create it).
        subprocess.run(
            ["databricks", "fs", "mkdir", target_dir],
            capture_output=True, text=True,
        )
        # --overwrite so re-runs replace the prior week's file in place.
        result = subprocess.run(
            ["databricks", "fs", "cp", "--overwrite", str(path), target],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            print(f"  FAILED: {result.stderr.strip()}")
            sys.exit(1)
        uploaded += 1
    print(f"\nUploaded {uploaded} file(s) to {volume}")


if __name__ == "__main__":
    main()
