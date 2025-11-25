
# SalesforceAccountFetcher

This repository provides a small utility to fetch Salesforce Account records (`SalesforceAccountFetcher` in `main.py`) and a standalone SerpApi enrichment helper.

## Contents

- `main.py` — `SalesforceAccountFetcher` (fetch Accounts via `simple-salesforce`, returns list or pandas DataFrame).
- `serpapi_enrich.py` — standalone SerpApi enrichment script (reads CSV, queries SerpApi Google results, writes JSON/CSV). Does NOT call Salesforce; suitable for local testing.
- `job_enrich_accounts.py` — job runner (uses `main.py` + SerpApi enrichment logic; may be present in the repo).
- `Dockerfile` — original project Dockerfile used to run `job_enrich_accounts.py` (left unchanged).
- `Dockerfile.serpapi` — new Dockerfile to build an image for running `serpapi_enrich.py` without touching the original Dockerfile.

## Setup

1) Create a virtualenv and install requirements:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2) Environment variables for Salesforce (two supported approaches)

- Connected App (preferred when using a client id/secret):

```bash
export CONSUMER_KEY=your_consumer_key
export CONSUMER_SECRET=your_consumer_secret
# domain is either the org instance (e.g. 'organization.my') or 'login' / 'test'
export DOMAIN=organization.my
```

- Username/password + security token (fallback):

```bash
export SF_USERNAME=you@example.com
export SF_PASSWORD=yourpassword
export SF_SECURITY_TOKEN=yoursecuritytoken
export DOMAIN=login   # or 'test' for sandbox
```

The `SalesforceAccountFetcher` prefers `CONSUMER_KEY`/`CONSUMER_SECRET` + `DOMAIN` when present, otherwise it falls back to `SF_USERNAME`/`SF_PASSWORD`/`SF_SECURITY_TOKEN`.

## Using `SalesforceAccountFetcher` (example)

```python
from main import SalesforceAccountFetcher
import os

fetcher = SalesforceAccountFetcher(
		consumer_key=os.getenv("CONSUMER_KEY"),
		consumer_secret=os.getenv("CONSUMER_SECRET"),
		domain=os.getenv("DOMAIN", os.getenv("SF_DOMAIN", "login")),
)

df = fetcher.get_all_accounts_df(limit=100)
print(df.head())
```

## SerpApi enrichment (standalone)

`serpapi_enrich.py` is a small, independent script that reads an input CSV (default `./accounts_sample.csv`), builds a search query per row (prefers Website, otherwise Name + City/Country), queries SerpApi's Google engine, and appends a few SerpApi fields (`serpapi_title`, `serpapi_link`, `serpapi_snippet`) to each row. It writes JSON and optionally CSV output.

Usage locally:

```bash
python serpapi_enrich.py --api-key YOUR_SERPAPI_KEY --input ./accounts_sample.csv --output ./accounts_enriched.json --limit 50 --pause 1.0 --csv-out
```

Docker image for the SerpApi script (separate from the project Dockerfile):

```bash
docker build -f Dockerfile.serpapi -t serpapi-enrich .

# mount the input CSV and an output folder
mkdir -p out
docker run --rm -v "$(pwd)/out:/app/out" -v "$(pwd)/accounts_sample.csv:/app/accounts_sample.csv:ro" \
	-e SERPAPI_API_KEY="YOUR_KEY" \
	serpapi-enrich python serpapi_enrich.py --api-key "$SERPAPI_API_KEY" --input /app/accounts_sample.csv --output /app/out/accounts_enriched.json --limit 50 --pause 1.0 --csv-out
```

## Docker: running the job that uses `main.py`
 
The existing `Dockerfile` in the repo builds an image used by `job_enrich_accounts.py` and imports `main.py`. If you modify `main.py`, rebuild the image before running to pick up changes:

```bash
docker build -t serpapi-job .

# Example run with Connected App creds
docker run --rm \
	-e CONSUMER_KEY="$CONSUMER_KEY" -e CONSUMER_SECRET="$CONSUMER_SECRET" -e DOMAIN="$DOMAIN" \
	-e SERPAPI_API_KEY="$SERPAPI_API_KEY" \
	serpapi-job python job_enrich_accounts.py --api-key "$SERPAPI_API_KEY" --limit 50 --output /app/out/accounts_enriched.json
```

If you prefer the username/password fallback use `SF_USERNAME`, `SF_PASSWORD`, and `SF_SECURITY_TOKEN` env vars instead of `CONSUMER_KEY`/`CONSUMER_SECRET`.

## Notes & recommendations

- Do not commit secrets to the repository. Use environment variables, Docker secrets, or a vault.
- For production, consider implementing the JWT Bearer OAuth flow (no passwords in env). I can help add it if you want.
- `serpapi_enrich.py` is purposely standalone: it does not import `main.py` or call Salesforce. If you want a wrapper that fetches Accounts via `SalesforceAccountFetcher` and then calls SerpApi to enrich them, I can add one.

---

If you'd like a short README section with exact `.env` or Docker Compose examples, tell me which workflow you prefer (Connected App or JWT) and I'll add it.

## sf_fetch.py — batched Id + parallel field queries

This repository also includes `sf_fetch.py`, a helper that fetches the full set of Account fields defined in `account_fields.AccountFields` and returns a merged `pandas.DataFrame`.

Key points
- The module splits the very large field list into smaller `chunk_size` groups and runs SOQL queries for each chunk in parallel (using a ThreadPoolExecutor).
- When `limit` is provided, the function first queries the Account Ids and then splits those Ids into batches (`id_batch_size`) to avoid creating huge `IN(...)` clauses in SOQL.
- The final result merges data by `Id` across all field-chunk and id-batch queries.

Function signature (important params):

```py
fetch_accounts(sf: Optional[Salesforce] = None,
			   limit: Optional[int] = None,
			   chunk_size: int = 40,
			   workers: int = 5,
			   id_batch_size: int = 200) -> pandas.DataFrame
```

Usage examples

Use environment variables (recommended):

```bash
export SF_USERNAME=you@example.com
export SF_PASSWORD=yourpassword
export SF_SECURITY_TOKEN=yoursecuritytoken
export SF_DOMAIN=login

python -c "from sf_fetch import fetch_accounts; df = fetch_accounts(limit=500); print(df.shape)"
```

Or pass a pre-built `Salesforce` instance:

```py
from simple_salesforce import Salesforce
from sf_fetch import fetch_accounts

sf = Salesforce(username='..', password='..', security_token='..', domain='login')
df = fetch_accounts(sf=sf, limit=200, chunk_size=50, workers=6, id_batch_size=100)
```

Notes
- If a field name in `account_fields.AccountFields` is invalid, the chunked query that contains it will raise an error.
- Tune `chunk_size` and `id_batch_size` if you hit SOQL length limits.
- For very large exports consider a different approach (bulk API / data export) rather than fetching all fields into memory.

