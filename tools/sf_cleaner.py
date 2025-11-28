#!/usr/bin/env python3
"""Salesforce Account enrichment and deduplication CLI.

Usage:
  python tools/sf_cleaner.py --dry-run --limit 100

This script will:
 - Fetch Accounts (optionally limited)
 - Enrich missing Google fields via SERPapi using fetcher.serp.SerpEnricher
 - Write a backup CSV of fetched accounts
 - Produce a report CSV of proposed/actual updates
 - Optionally push updates to Salesforce (--commit)
 - Optionally run deduplication by Google_Place_ID__c (--merge)

Notes:
 - By default runs as dry-run (no writes or deletes). Use --commit to apply updates and --merge to perform merges/deletes.
 - Requires environment variables for Salesforce credentials or an active simple_salesforce connection via fetcher.salesforce_impl.SalesforceFetcher
 - Requires serpapi client (SERPAPI_API_KEY) for enrichment
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional

import pathlib
import pandas as pd

# Ensure the repository root is on sys.path so local packages (fetcher, etc.) can be imported
REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
	sys.path.insert(0, str(REPO_ROOT))

# Load .env (if present) and map common SERPapi key names to the canonical SERPAPI_API_KEY
def _load_dotenv_and_set_serpapi_key(env_path: Optional[pathlib.Path] = None) -> None:
	env_path = env_path or (REPO_ROOT / ".env")
	if not env_path.exists():
		return
	try:
		with env_path.open("r", encoding="utf-8") as fh:
			for line in fh:
				line = line.strip()
				if not line or line.startswith("#"):
					continue
				if "=" not in line:
					continue
				k, v = line.split("=", 1)
				k = k.strip()
				v = v.strip().strip('"').strip("'")
				# common misspellings/variants we accept
				if k.upper() in ("SERPAPI_API_KEY", "SERPAPI_KEY", "SEPRAPI_KEY", "SEPRAPIKEY", "SEPRAPI"):
					# do not overwrite existing env var if present
					if not os.environ.get("SERPAPI_API_KEY"):
						os.environ["SERPAPI_API_KEY"] = v
						masked = (v[:4] + "..." + v[-4:]) if len(v) > 8 else "(set)"
						print(f"Loaded SERPAPI_API_KEY from {env_path} (masked={masked})")
						return
	except Exception:
		# ignore failures reading .env
		return


_load_dotenv_and_set_serpapi_key()

from fetcher.salesforce_impl import SalesforceFetcher
from fetcher.serp import SerpEnricher


def _backup_df(df: pd.DataFrame, path: str) -> None:
	try:
		df.to_csv(path, index=False)
		print(f"Backup written to: {path}")
	except Exception as e:
		print(f"Failed to write backup CSV to {path}: {e}")


def _collect_updates(original: pd.DataFrame, enriched: pd.DataFrame, fields_to_keep: List[str]) -> List[Dict]:
	"""Return list of updates where enriched has new non-empty values differing from original.

	Each item: {Id, changes: {field: (old, new)}, new_values: {...}}
	"""
	orig = original.set_index("Id")
	enr = enriched.set_index("Id")
	updates: List[Dict] = []
	for rid, row in enr.iterrows():
		if rid not in orig.index:
			continue
		changes = {}
		new_values = {}
		for f in fields_to_keep:
			newv = row.get(f) if f in enr.columns else None
			oldv = orig.at[rid, f] if f in orig.columns else None
			# Normalize NaN/None/empty
			if pd.isna(newv) or newv == "":
				continue
			if pd.isna(oldv) or oldv == "" or str(oldv) != str(newv):
				changes[f] = (oldv, newv)
				new_values[f] = newv
		if changes:
			updates.append({"Id": rid, "changes": changes, "new_values": new_values})
	return updates


def _apply_updates(sf, updates: List[Dict], dry_run: bool = True, workers: int = 8) -> List[Dict]:
	results: List[Dict] = []
	if not updates:
		return results

	def _updater(item: Dict) -> Dict:
		rid = item["Id"]
		payload = item["new_values"]
		try:
			if not dry_run:
				sf.Account.update(rid, payload)
				status = "updated"
			else:
				status = "dry-run"
			return {"Id": rid, "status": status, "updated_fields": list(payload.keys())}
		except Exception as e:
			return {"Id": rid, "status": "error", "error": str(e)}

	with ThreadPoolExecutor(max_workers=workers) as ex:
		futures = {ex.submit(_updater, u): u for u in updates}
		for fut in as_completed(futures):
			results.append(fut.result())

	return results


def _find_duplicate_groups(df: pd.DataFrame) -> Dict[str, List[str]]:
	# group by Google_Place_ID__c exact match
	if "Google_Place_ID__c" not in df.columns:
		return {}
	df2 = df.copy()
	df2 = df2[df2["Google_Place_ID__c"].notna() & (df2["Google_Place_ID__c"] != "")]
	groups = df2.groupby("Google_Place_ID__c")["Id"].apply(list)
	return {k: v for k, v in groups.items() if len(v) > 1}


def _choose_master(df: pd.DataFrame, ids: List[str]) -> str:
	sub = df.set_index("Id").loc[ids]
	# Prefer IsCustomer__c truthy
	if "IsCustomer__c" in sub.columns:
		customers = sub[sub["IsCustomer__c"].astype(bool) == True]
		if len(customers) > 0:
			# choose the most recently modified among customers
			if "LastModifiedDate" in customers.columns:
				return customers["LastModifiedDate"].astype(str).sort_values(ascending=False).index[0]
			return customers.index[0]
	# fallback: choose most recent LastModifiedDate if available
	if "LastModifiedDate" in sub.columns:
		return sub["LastModifiedDate"].astype(str).sort_values(ascending=False).index[0]
	# otherwise just pick first
	return ids[0]


def _reparent_records(sf, object_name: str, parent_field: str, from_ids: List[str], to_id: str, dry_run: bool = True) -> Dict:
	"""Find records of object_name where parent_field in from_ids and update to to_id.

	Returns a dict summarizing action counts and any errors.
	"""
	out = {"object": object_name, "updated": 0, "errors": []}
	if not from_ids:
		return out
	ids_sql = ", ".join([f"'{i}'" for i in from_ids])
	q = f"SELECT Id FROM {object_name} WHERE {parent_field} IN ({ids_sql})"
	try:
		resp = sf.query_all(q)
		recs = resp.get("records", [])
		ids = [r["Id"] for r in recs if r.get("Id")]
		if not ids:
			return out
		# update in batches
		batch = []
		for rid in ids:
			batch.append({"Id": rid, parent_field: to_id})
			if len(batch) >= 200:
				if not dry_run:
					sf.bulk.__getattr__(object_name).update(batch)
				out["updated"] += len(batch)
				batch = []
		if batch:
			if not dry_run:
				sf.bulk.__getattr__(object_name).update(batch)
			out["updated"] += len(batch)
	except Exception as e:
		out["errors"].append(str(e))
	return out


def _process_duplicate_group(sf, df: pd.DataFrame, ids: List[str], dry_run: bool = True) -> Dict:
	"""Process one duplicate group: choose master, reparent common related objects, delete duplicates."""
	master = _choose_master(df, ids)
	others = [i for i in ids if i != master]
	summary = {"master": master, "merged": others, "actions": []}

	# Reparent common objects to master
	# We'll process a safe set: Opportunity (AccountId), Case (AccountId), Task (WhatId), Note (ParentId), Attachment (ParentId)
	reparent_jobs = [
		("Opportunity", "AccountId"),
		("Case", "AccountId"),
		("Task", "WhatId"),
		("Note", "ParentId"),
		("Attachment", "ParentId"),
	]

	for obj, fld in reparent_jobs:
		res = _reparent_records(sf, obj, fld, others, master, dry_run=dry_run)
		summary["actions"].append(res)

	# Delete duplicates (or report)
	del_results = []
	for dup in others:
		try:
			if not dry_run:
				sf.Account.delete(dup)
				del_results.append({"Id": dup, "status": "deleted"})
			else:
				del_results.append({"Id": dup, "status": "dry-run"})
		except Exception as e:
			del_results.append({"Id": dup, "status": "error", "error": str(e)})
	summary["deletions"] = del_results
	return summary


def main(argv: Optional[List[str]] = None) -> int:
	parser = argparse.ArgumentParser()
	parser.add_argument("--limit", type=int, default=None, help="Limit number of Accounts to process (for testing)")
	parser.add_argument("--backup", type=str, default="accounts_backup.csv", help="Path to write backup CSV")
	parser.add_argument("--report", type=str, default="sf_cleaner_report.csv", help="Path to write changes report CSV")
	parser.add_argument("--workers", type=int, default=6, help="Parallel workers for SF updates")
	parser.add_argument("--commit", action="store_true", help="Apply updates and deletions to Salesforce (otherwise dry-run)")
	parser.add_argument("--merge", action="store_true", help="Run deduplication/merge step after enrichment")
	parser.add_argument("--limit-enrich-only", action="store_true", help="Only enrich subset then exit (helper)")
	args = parser.parse_args(argv)

	dry_run = not args.commit

	print("Connecting to Salesforce...")
	sf_fetcher = SalesforceFetcher()
	sf = sf_fetcher.sf

	print("Fetching Accounts...")
	df = sf_fetcher.fetch_accounts(limit=args.limit)
	if df.empty:
		print("No accounts fetched. Exiting.")
		return 0

	# Backup
	_backup_df(df, args.backup)

	# Enrich only accounts lacking Google_Place_ID__c
	to_enrich = df[df.get("Google_Place_ID__c").isna() | (df.get("Google_Place_ID__c") == "")]
	if to_enrich.empty:
		print("No accounts to enrich (all have Google_Place_ID__c).")
	else:
		print(f"Enriching {len(to_enrich)} accounts via SERPapi (dry_run={dry_run})...")
		enricher = SerpEnricher()
		merged = enricher.enrich(df, workers=args.workers, save_csv=None)

		# fields of interest to update
		fields_to_update = [
			"Restaurant_Type__c",
			"Google_Rating__c",
			"Google_Review_Count__c",
			"Google_Data_ID__c",
			"Google_Place_ID__c",
			"Google_Updated_Date__c",
			"Google_Price__c",
			"Has_Google_Accept_Bookings_Extension__c",
			"Prospection_Status__c",
		]

		updates = _collect_updates(df, merged, fields_to_update)
		print(f"Found {len(updates)} accounts with changes to apply.")

		# Write report header + rows
		report_rows = []
		for u in updates:
			report_rows.append({"Id": u["Id"], "changed_fields": ",".join(u["changes"].keys())})

		# Apply updates
		applied = _apply_updates(sf, updates, dry_run=dry_run, workers=args.workers)

		# Merge applied info into report
		for r in applied:
			report_rows.append({"Id": r.get("Id"), "status": r.get("status"), "updated_fields": ",".join(r.get("updated_fields") or [])})

		# write report CSV
		try:
			with open(args.report, "w", newline="") as fh:
				writer = csv.DictWriter(fh, fieldnames=["Id", "changed_fields", "status", "updated_fields"])
				writer.writeheader()
				for row in report_rows:
					writer.writerow(row)
			print(f"Wrote report to {args.report}")
		except Exception as e:
			print(f"Failed to write report CSV: {e}")

		# Optionally stop here
		if args.limit_enrich_only:
			print("Exiting after enrichment-only run")
			return 0

	# Reload accounts to include any changes (or use merged dataframe in dry-run)
	if dry_run:
		df_after = merged if 'merged' in locals() else df
	else:
		print("Re-fetching accounts post-update...")
		df_after = sf_fetcher.fetch_accounts(limit=args.limit)

	# Deduplication
	if args.merge:
		print("Detecting duplicate Google_Place_ID__c groups...")
		groups = _find_duplicate_groups(df_after)
		print(f"Found {len(groups)} duplicate groups (Google_Place_ID__c shared by >1 account).")
		merge_summaries = []
		for place_id, ids in groups.items():
			print(f"Processing group place_id={place_id} ids={ids}")
			summary = _process_duplicate_group(sf, df_after, ids, dry_run=dry_run)
			summary["place_id"] = place_id
			merge_summaries.append(summary)

		# write merge summary
		try:
			out_path = "merge_summary.json"
			import json

			with open(out_path, "w") as fh:
				json.dump(merge_summaries, fh, default=str, indent=2)
			print(f"Wrote merge summary to {out_path}")
		except Exception as e:
			print(f"Failed to write merge summary: {e}")

	print("Done.")
	return 0


if __name__ == "__main__":
	raise SystemExit(main())

