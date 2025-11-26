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

---

If you'd like, I can also update the repository's top-level `requirements.txt` or add a `pyproject.toml` for reproducible installs and for running unit tests. Tell me which you'd prefer and I'll add it.
