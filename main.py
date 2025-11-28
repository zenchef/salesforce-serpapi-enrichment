"""Main CLI demonstrating fetching Accounts from Salesforce and enriching via SerpApi.

Usage examples:
  - Fetch and enrich, saving CSV:
	  python main.py enrich --api-key YOUR_KEY --limit 100 --output out/enriched.csv

  - Propose labels for a Known_Internal_Issue CSV:
	  python main.py label --input ~/Downloads/Known_Internal_Issue__c-19_11_2025.csv

This file uses the `fetcher` package which contains OOP classes:
 - fetcher.SalesforceFetcher
 - fetcher.SerpEnricher
 - fetcher.LabelProposer
"""
from __future__ import annotations

import argparse
import os
from typing import Optional

from fetcher import SalesforceFetcher
from fetcher.serp import SerpEnricher
from fetcher.labeler import LabelProposer


def cmd_enrich(args: argparse.Namespace) -> int:
	# create Salesforce fetcher and fetch accounts
	sf = None
	fetcher = SalesforceFetcher(sf)
	df = fetcher.fetch_accounts(limit=args.limit, chunk_size=args.chunk_size, workers=args.workers)

	enr = SerpEnricher(api_key=args.api_key)
	out = enr.enrich(df, workers=args.serp_workers, pause=args.pause, save_csv=args.output)
	if not args.output:
		print(out.head())
	return 0


def cmd_label(args: argparse.Namespace) -> int:
	proposer = LabelProposer()
	out = proposer.process_csv(args.input, args.output)
	print(f"Wrote labeled CSV to: {out}")
	return 0


def main(argv: Optional[list[str]] = None) -> int:
	p = argparse.ArgumentParser(description="Simple runner for fetching and enriching Accounts")
	sub = p.add_subparsers(dest="command")

	enrich = sub.add_parser("enrich")
	enrich.add_argument("--api-key", required=True, help="SerpApi API key")
	enrich.add_argument("--limit", type=int, default=None, help="How many accounts to fetch (omit for no limit)")
	enrich.add_argument("--chunk-size", type=int, default=40)
	enrich.add_argument("--workers", type=int, default=5)
	enrich.add_argument("--serp-workers", type=int, default=5)
	enrich.add_argument("--pause", type=float, default=0.2)
	enrich.add_argument("--output", type=str, default=None, help="CSV output path")

	label = sub.add_parser("label")
	label.add_argument("--input", required=True)
	label.add_argument("--output", required=False)

	args = p.parse_args(argv)
	if args.command == "enrich":
		return cmd_enrich(args)
	if args.command == "label":
		return cmd_label(args)
	p.print_help()
	return 1


if __name__ == "__main__":
	raise SystemExit(main())

