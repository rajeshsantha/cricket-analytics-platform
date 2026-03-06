#!/usr/bin/env python3
"""
CI script: Download Cricsheet T20 WC data, detect new matches.
Sets GitHub Actions output 'new_matches' with the count of new files.
Works without Spark — pure Python.
"""
import json
import os
import sys
import zipfile
from pathlib import Path
from urllib.request import urlretrieve

CRICSHEET_URL = "https://cricsheet.org/downloads/t20s_male_json.zip"
RAW_DIR = Path("visualization/streamlit/data/raw_json")
STAGING_DIR = Path("/tmp/cricsheet-staging")

def main():
    force = os.environ.get("FORCE", "false").lower() == "true"

    # ── Download ────────────────────────────────────────────────────────────
    print("📥 Downloading Cricsheet T20 WC data...")
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    zip_path = STAGING_DIR / "t20wc.zip"
    urlretrieve(CRICSHEET_URL, zip_path)

    all_dir = STAGING_DIR / "all"
    all_dir.mkdir(exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(all_dir)

    # ── Filter 2026 T20 World Cup ───────────────────────────────────────────
    wc2026 = []
    for f in sorted(all_dir.glob("*.json")):
        try:
            d = json.loads(f.read_text())
            info = d.get("info", {})
            dates = info.get("dates", [])
            event_name = info.get("event", {}).get("name", "")
            is_2026 = any("2026" in str(dt) for dt in dates)
            is_wc = "T20 World Cup" in event_name
            if is_2026 and is_wc:
                wc2026.append(f)
        except Exception:
            pass

    print(f"  Found {len(wc2026)} matches from 2026 in Cricsheet")

    # ── Detect new ──────────────────────────────────────────────────────────
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    existing = {f.name for f in RAW_DIR.glob("*.json")}
    new_files = [f for f in wc2026 if f.name not in existing]

    print(f"  Existing: {len(existing)} matches")
    print(f"  New:      {len(new_files)} match(es)")

    for nf in new_files:
        print(f"    + {nf.name}")
        # Copy new file to raw dir
        (RAW_DIR / nf.name).write_bytes(nf.read_bytes())

    # ── Set GitHub Actions output ───────────────────────────────────────────
    gh_output = os.environ.get("GITHUB_OUTPUT")
    if gh_output:
        with open(gh_output, "a") as fh:
            fh.write(f"new_matches={len(new_files)}\n")
            fh.write(f"total_matches={len(existing) + len(new_files)}\n")
    else:
        print(f"\n  [local] new_matches={len(new_files)}")
        print(f"  [local] total_matches={len(existing) + len(new_files)}")

    if len(new_files) == 0 and not force:
        print("\n✅ No new matches. Pipeline up to date.")
    else:
        print(f"\n🆕 {len(new_files)} new match(es) ready for processing.")

    return len(new_files)


if __name__ == "__main__":
    count = main()
    # Exit 0 always — GitHub Actions checks the output, not exit code
    sys.exit(0)



