#!/usr/bin/env python3
"""Fetch Account records from Salesforce, enrich with SerpApi and print results.

Usage examples:
  # use env var for SERPAPI_API_KEY and Salesforce creds
  python run_fetch_enrich_print.py --limit 50

  # pass api key directly
  python run_fetch_enrich_print.py --api-key YOUR_KEY --limit 20
"""
from __future__ import annotations

import argparse
import json
from typing import Optional

from fetcher import SalesforceFetcher
from fetcher.serp import SerpEnricher


def print_account_row(row: dict, print_all: bool = False) -> None:
    # select common SerpApi-derived columns
    keys = [
        "Id",
        "Name",
        "Google_Place_ID__c",
        "Google_Data_ID__c",
        "Google_Rating__c",
        "Google_Review_Count__c",
        "Restaurant_Type__c",
        "Google_Price__c",
        "Prospection_Status__c",
        "Has_Google_Accept_Bookings_Extension__c",
        "Google_Updated_Date__c",
    ]
    if print_all:
        print(json.dumps(row, indent=2, ensure_ascii=False))
        return

    out = {k: row.get(k) for k in keys}
    print(json.dumps(out, indent=2, ensure_ascii=False))


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--api-key", required=False, help="SerpApi API key (optional, can use SERPAPI_API_KEY env)")
    p.add_argument("--limit", type=int, default=50)
    p.add_argument("--chunk-size", type=int, default=40)
    p.add_argument("--workers", type=int, default=5)
    p.add_argument("--serp-workers", type=int, default=5)
    p.add_argument("--pause", type=float, default=0.2)
    p.add_argument("--print-all", action="store_true", help="Print full merged row as JSON")
    args = p.parse_args()

    # fetch accounts from Salesforce
    sf_fetcher = SalesforceFetcher()
    print(f"Fetching up to {args.limit} accounts from Salesforce...")
    df = sf_fetcher.fetch_accounts(limit=args.limit, chunk_size=args.chunk_size, workers=args.workers)
    print(f"Fetched {len(df)} rows")

    # enrich via SerpApi
    enr = SerpEnricher(api_key=args.api_key)
    print("Running SerpApi enrichment (this will skip hotels and rows with existing place ids)...")
    merged = enr.enrich(df, workers=args.serp_workers, pause=args.pause)

    # iterate and print
    for _, r in merged.iterrows():
        # convert Series to dict (to avoid numpy types in json)
        print_account_row({k: (None if r.get(k) is None else (r.get(k))) for k in merged.columns}, print_all=args.print_all)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
