from pathlib import Path
import os

from scraper.stages.crews import run as run_crews
from scraper.stages.crew_details import run as run_crew_details
from scraper.stages.pirate_urls import run as run_pirate_urls
from scraper.stages.pirates import run as run_pirates
from scraper.stages.shoppes import run as run_shoppes
from scraper.stages.finalize import run as run_finalize


def main():
    output_dir = Path(os.getenv("OUTPUT_DIR", "data"))
    output_dir.mkdir(parents=True, exist_ok=True)

    ctx = {"data": {}}

    print("Running crews stage...")
    ctx["data"]["crews"] = run_crews(ctx)

    print("Running crew_details stage...")
    ctx["data"]["crew_details"] = run_crew_details(ctx)

    print("Running pirate_urls stage...")
    ctx["data"]["pirate_urls"] = run_pirate_urls(ctx)

    print("Running pirates stage...")
    ctx["data"]["pirates"] = run_pirates(ctx)

    print("Running shoppes stage...")
    ctx["data"]["shoppes"] = run_shoppes(ctx)

    print("Running finalize stage...")
    outputs = run_finalize(ctx)

    for filename, df in outputs.items():
        path = output_dir / filename
        df.to_csv(path, index=False)
        print(f"Wrote {path}")

    print("Pipeline complete.")


if __name__ == "__main__":
    main()