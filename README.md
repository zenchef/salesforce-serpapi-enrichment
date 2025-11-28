# Salesforce Account fetcher + SerpApi enrichment

This repository fetches Salesforce Account records (many fields) in a chunked, parallel way and can enrich those records with Google/Maps data via SerpApi. The code was refactored into a small `fetcher` package with OOP helpers:

- `fetcher.salesforce.SalesforceFetcher` — fetch Account rows as a pandas DataFrame (chunked field queries, id batching, parallel workers).
- `fetcher.serp.SerpEnricher` — enrich a DataFrame of Accounts via SerpApi (explicit place_id support, retries/backoff, parallel requests).
- `fetcher.labeler.LabelProposer` — simple CSV label proposer (used for the Known_Internal_Issue CSV you provided).

There is a lightweight CLI entrypoint `main.py` with two commands:

- `enrich` — fetch accounts and enrich them with SerpApi, optionally write CSV.
- `label` — propose labels for a Known_Internal_Issue CSV using the labeler.

This README documents quick setup and usage for the current code layout.

## Quick setup

1. Create a virtualenv and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Install SerpApi client into the same Python interpreter you use to run scripts:

```bash
python -m pip install --upgrade google-search-results
```

If you get an ImportError for `GoogleSearch` (e.g. "cannot import name 'GoogleSearch' from 'serpapi'"), it's usually because the package was installed under a different Python installation. Re-run the pip command with the exact python binary you use (for example `/usr/local/bin/python3 -m pip install --upgrade google-search-results`).

## Authentication (Salesforce)

Two common ways to authenticate (the code accepts several env-var name variants):

- Connected App / Consumer credentials (preferred when available):

```bash
export CONSUMER_KEY=your_consumer_key
export CONSUMER_SECRET=your_consumer_secret
export SF_DOMAIN=login   # or 'test' for sandbox
```

- Username/password + security token (fallback):

```bash
export SF_USERNAME=you@example.com
export SF_PASSWORD=yourpassword
export SF_SECURITY_TOKEN=yoursecuritytoken
export SF_DOMAIN=login
```

The `fetcher.SalesforceFetcher` will try a variety of names (SF_USERNAME, SFDC_USERNAME, CONSUMER_KEY, etc.) to maximize compatibility with existing CI or local env setups.

## CLI usage (recommended)

1) Enrich accounts (fetch + SerpApi enrich) and save CSV:

```bash
python main.py enrich --api-key YOUR_SERPAPI_KEY --limit 100 --output out/enriched.csv
```

Flags of interest:
- `--api-key` : SerpApi API key (or set `SERPAPI_API_KEY` env var)
- `--limit` : number of accounts to fetch
- `--chunk-size` : number of fields per SOQL query chunk (default 40)
- `--workers` : number of parallel Salesforce queries
- `--serp-workers` : number of parallel SerpApi requests

2) Propose labels for a Known_Internal_Issue CSV (wrapper around the refactored labeler):

```bash
python main.py label --input path/to/Known_Internal_Issue__c-19_11_2025.csv --output out/labeled.csv
```

## Programmatic usage (library)

You can import the classes directly from the `fetcher` package if you want to script or test things:

```py
from fetcher.salesforce import SalesforceFetcher
from fetcher.serp import SerpEnricher

sf = SalesforceFetcher()  # uses env vars if you don't pass a Simple-Salesforce instance
df = sf.fetch_accounts(limit=200)

enr = SerpEnricher(api_key="MY_KEY")
enriched = enr.enrich(df, save_csv="out/enriched.csv")
```

## Notes on behavior and tuning

- The field list used for Account queries comes from `account_fields.py` and is intentionally large. The fetcher splits fields into chunks (`chunk_size`) and queries each chunk in parallel to avoid SOQL length limits.
- When `--limit` is provided the fetcher first queries the Ids and then batches them into smaller `IN (...)` groups (controlled by `id_batch_size`) to avoid enormous IN clauses.
- If Salesforce returns INVALID_FIELD for a field present in `account_fields`, the fetcher will call `Account.describe()` and drop invalid fields for the failing chunk, then retry.
- For very large exports consider using the Bulk API / data export instead of pulling all fields into memory.

## Troubleshooting

- SerpApi import errors: ensure `google-search-results` is installed into the same Python you run (see the install note above).
- Salesforce auth errors: check env-var names and values; the code looks for many common names (`SF_USERNAME`, `SFDC_USERNAME`, `CONSUMER_KEY`, etc.).

## Next steps (optional)

- Add a small test suite for `SalesforceFetcher` and `SerpEnricher` (I can scaffold pytest tests).
- Add a Docker Compose or `.env` example for easier local runs.


## SF Cleaner CLI (enrich Accounts & deduplicate by Google Place ID)

This repository includes a safe CLI to enrich Salesforce Accounts with Google My Business data (via SERPapi) and deduplicate accounts by Google Place ID: `tools/sf_cleaner.py`.

What it does
- Fetch Accounts (optionally limited).
- Back up a CSV of fetched Accounts before any writes.
- Enrich Accounts that do NOT have `Google_Place_ID__c` using SERPapi.
- Produce a report CSV describing proposed updates and a `merge_summary.json` when merging duplicates.
- Optionally apply updates and deletions to Salesforce when invoked with `--commit` and `--merge`.

Safety and defaults
- The script is DRY-RUN by default: it will not write or delete in Salesforce unless `--commit` is passed.
- Always review the backup CSV and the report before using `--commit`.

Quick examples

- Dry-run enrich (safe; does not write to Salesforce):

```bash
python tools/sf_cleaner.py --limit 100 --backup /tmp/accounts_backup.csv --report /tmp/sf_cleaner_report.csv
```

- Apply updates (writes updates) and run deduplication (will DELETE duplicates):

```bash
python tools/sf_cleaner.py --limit 200 --commit --merge --backup /tmp/accounts_backup.csv --report /tmp/sf_cleaner_report.csv
```

Important environment variables
- Salesforce credentials: the fetcher supports multiple env var names; common ones:
	- `SF_USERNAME`, `SF_PASSWORD`, `SF_SECURITY_TOKEN`, `CONSUMER_KEY`, `CONSUMER_SECRET`, `SF_DOMAIN`
- SERPapi key: the script looks for `SERPAPI_API_KEY` in the environment. It also accepts common variants in a `.env` file such as `SERPAPI_KEY` or the misspelling `SEPRAPI_KEY`; the script will load `.env` at the repo root and export `SERPAPI_API_KEY` automatically for the run.

Outputs
- Backup CSV (default `accounts_backup.csv`, configurable with `--backup`).
- Report CSV (default `sf_cleaner_report.csv`, configurable with `--report`).
- Merge summary JSON (`merge_summary.json`) containing details of reparenting and deletions.

Requirements
- `pandas`
- `simple-salesforce`
- `google-search-results` (SERPapi client)

Databricks notes
- To run on Databricks, install required libraries on the cluster (`pandas`, `simple-salesforce`, `google-search-results`) and ensure environment variables are available to the driver. Use `dbutils.secrets` to store credentials and set env vars in the notebook before running the CLI logic.



