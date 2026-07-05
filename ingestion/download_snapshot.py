"""Download an Inside Airbnb snapshot for Chicago.
Usage: python ingestion/download_snapshot.py 2025-09-22
"""
import sys, urllib.request
from pathlib import Path

BASE = "https://data.insideairbnb.com/united-states/il/chicago/{date}/data/{file}"
FILES = ["listings.csv.gz", "calendar.csv.gz", "reviews.csv.gz"]

def main(snapshot_date: str):
    out_dir = Path("data/raw") / snapshot_date
    out_dir.mkdir(parents=True, exist_ok=True)
    for f in FILES:
        url = BASE.format(date=snapshot_date, file=f)
        dest = out_dir / f
        print(f"downloading {url}")
        urllib.request.urlretrieve(url, dest)
        print(f"  -> {dest} ({dest.stat().st_size:,} bytes)")

if __name__ == "__main__":
    main(sys.argv[1])