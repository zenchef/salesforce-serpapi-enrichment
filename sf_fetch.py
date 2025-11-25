"""Utilities to fetch Account records from Salesforce and return a pandas DataFrame.

This module supports:
- using an optional `sf` Simple-Salesforce instance or creating one from
  environment variables
- splitting the (very large) Account field list into chunks and querying those
  chunks in parallel (ThreadPoolExecutor)
- limiting the number of accounts fetched

Environment variable heuristics (the code will try these names):
- SF_USERNAME / SF_PASSWORD / SF_SECURITY_TOKEN (standard username/password auth)
- SFDC_CLIENT_ID / SFDC_CLIENT_SECRET / SF_DOMAIN (passed to constructor if present)
- also tolerates lowercase variants like sfdc_client_id

Usage summary:
from simple_salesforce import Salesforce
from simple_salesforce.exceptions import SalesforceMalformedRequest
from account_fields import AccountFields
from sf_fetch import fetch_accounts

# either create an sf instance and pass it in, OR set env vars and let fetch
df = fetch_accounts(sf=None, limit=100, chunk_size=40, workers=5)

"""
from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Iterable, List, Optional

import pandas as pd
from simple_salesforce import Salesforce
from simple_salesforce.exceptions import SalesforceMalformedRequest
from typing import Any

# optional serpapi enrichment helper (import here to avoid hard dependency at module import)
try:
    # prefer the OOP enricher if available
    from fetcher.serp import SerpEnricher  # type: ignore

    def _enrich_df(df, api_key, workers=5, pause=0.1, save_csv=None):
        enr = SerpEnricher(api_key=api_key)
        return enr.enrich(df, workers=workers, pause=pause, save_csv=save_csv)

    enrich_with_serpapi = _enrich_df
except Exception:
    try:
        from serp_fetch import enrich_with_serpapi  # fallback to legacy module
    except Exception:
        enrich_with_serpapi = None  # type: ignore

from account_fields import AccountFields


def _env_get(*names: str) -> Optional[str]:
    """Return first non-empty env var value for candidate names."""
    for n in names:
        v = os.environ.get(n)
        if v:
            return v
    return None


def _chunk_list(items: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(items), n):
        yield items[i : i + n]


def _make_salesforce_from_env() -> Salesforce:
    """Create a Salesforce instance using environment variables.

    Tries username/password/token auth and passes consumer key/secret/domain
    if present.
    """
    username = _env_get("SF_USERNAME", "SFDC_USERNAME", "sfdc_username")
    password = _env_get("SF_PASSWORD", "SFDC_PASSWORD", "sfdc_password")
    token = _env_get(
        "SF_SECURITY_TOKEN", "SFDC_SECURITY_TOKEN", "sfdc_security_token"
    )

    consumer_key = _env_get(
        "SFDC_CLIENT_ID",
        "sfdc_client_id",
        "SF_CLIENT_ID",
        "CONSUMER_KEY",
        "CONSUMER_KEY",
    )
    consumer_secret = _env_get(
        "SFDC_CLIENT_SECRET",
        "sfdc_client_secret",
        "SF_CLIENT_SECRET",
        "CONSUMER_SECRET",
        "CONSUMER_SECRET",
    )
    domain = _env_get("SF_DOMAIN", "sfdc_domain", "SFDC_DOMAIN", "DOMAIN")

    kwargs = {}
    if domain:
        kwargs["domain"] = domain
    if consumer_key:
        kwargs["consumer_key"] = consumer_key
    if consumer_secret:
        kwargs["consumer_secret"] = consumer_secret

    if username and password:
        # Standard username/password + token auth is most reliable
        return Salesforce(username=username, password=password, security_token=token or "", **kwargs)

    # Fall back to a minimal constructor that may use OAuth if environment
    # provides a connected session or other credentials.
    return Salesforce(**kwargs)


def fetch_accounts(
    sf: Optional[Salesforce] = None,
    limit: Optional[int] = None,
    chunk_size: int = 40,
    workers: int = 5,
    id_batch_size: int = 200,
) -> pd.DataFrame:
    """Fetch Account records for all fields listed in `AccountFields`.

    Behavior
    - If `sf` is provided it will be used. Otherwise an attempt is made to
      build a Salesforce instance from environment variables.
    - Fields are split into chunks of `chunk_size` and each chunk is queried in
      parallel using up to `workers` threads. Results are merged by Id.
    - If `limit` is provided, only that many accounts are returned.

    Returns a pandas DataFrame where each row is an Account record.
    """
    if sf is None:
        sf = _make_salesforce_from_env()

    fields_all = AccountFields().all
    # Ensure Id is not duplicated and always included
    if "Id" not in fields_all:
        fields_all = ["Id"] + fields_all

    # Get candidate Ids when a limit is requested to avoid pulling the whole
    # org in each chunk. We'll batch Ids into smaller groups to avoid very
    # long IN(...) clauses. id_batch_size controls the size of those batches.
    id_list: Optional[List[str]] = None
    if limit is not None:
        q = f"SELECT Id FROM Account LIMIT {int(limit)}"
        ids_resp = sf.query_all(q)
        id_list = [r["Id"] for r in ids_resp.get("records", [])]

    # Build field chunks (exclude Id from chunking, since we'll always add it)
    fields_no_id = [f for f in fields_all if f != "Id"]
    chunks = list(_chunk_list(fields_no_id, int(chunk_size) or 40))

    # Build id batches if needed
    id_batches: Optional[List[List[str]]] = None
    if id_list:
        id_batches = list(_chunk_list(id_list, int(id_batch_size) or 200))

    def _query_chunk(chunk_fields: List[str], id_batch: Optional[List[str]] = None) -> List[Dict]:
        """Query a chunk of fields, optionally restricted to an id_batch.

        If Salesforce responds with INVALID_FIELD, attempt to describe the
        Account object to find valid field names, drop invalid fields and
        retry the query once.
        """
        def _run_query(fields: List[str]) -> List[Dict]:
            select_fields = ["Id"] + fields
            select_sql = ", ".join(select_fields)
            if id_batch is not None and len(id_batch) > 0:
                # build IN clause; SOQL accepts single-quoted ids
                ids_sql = ", ".join([f"'{i}'" for i in id_batch])
                q = f"SELECT {select_sql} FROM Account WHERE Id IN ({ids_sql})"
            else:
                q = f"SELECT {select_sql} FROM Account"
            # Use query_all for completeness; simple-salesforce will page.
            resp = sf.query_all(q)
            records = resp.get("records", [])
            # strip attributes sub-dict if present (simple-salesforce returns attributes key)
            out = []
            for r in records:
                d = {k: v for k, v in r.items() if k != "attributes"}
                out.append(d)
            return out

        try:
            return _run_query(chunk_fields)
        except SalesforceMalformedRequest as exc:
            # Try to recover from invalid field errors by describing the
            # Account object and filtering unknown fields.
            msg = getattr(exc, "response", None) or str(exc)
            print(f"Malformed request {exc.args[0]}: attempting to drop invalid fields and retry")
            try:
                desc = sf.Account.describe()
                valid_names = {f.get("name") for f in desc.get("fields", [])}
                filtered = [f for f in chunk_fields if f in valid_names]
                dropped = [f for f in chunk_fields if f not in valid_names]
                if dropped:
                    print(f"Dropping invalid fields: {dropped}")
                if not filtered:
                    # nothing to query
                    return []
                return _run_query(filtered)
            except Exception as e:  # pragma: no cover - surface describe failures
                print(f"Failed to describe Account or retry query: {e}")
                raise

    merged: Dict[str, Dict] = {}

    # Prepare tasks: pairs of (field_chunk, optional id_batch)
    tasks = []
    if id_batches:
        for idb in id_batches:
            for c in chunks:
                tasks.append((c, idb))
    else:
        for c in chunks:
            tasks.append((c, None))

    # Execute queries in parallel; number of actual concurrent connections
    # limited by `workers`.
    with ThreadPoolExecutor(max_workers=int(workers) or 5) as ex:
        futures = {ex.submit(_query_chunk, c, idb): (c, idb) for (c, idb) in tasks}
        for fut in as_completed(futures):
            task = futures[fut]
            try:
                records = fut.result()
            except Exception as exc:  # pragma: no cover - surface errors
                print(f"Error querying chunk {task}: {exc}")
                raise
            for r in records:
                rid = r.get("Id")
                if rid is None:
                    continue
                if rid not in merged:
                    merged[rid] = {"Id": rid}
                # merge fields (later chunks overwrite earlier None values)
                merged[rid].update(r)

    # Convert merged dicts into list in deterministic order
    rows = list(merged.values())

    # Respect requested limit: merged may contain more rows if ids were None
    if limit is not None and len(rows) > limit:
        rows = rows[:limit]

    df = pd.DataFrame.from_records(rows)
    return df


def fetch_and_enrich(
    api_key: str,
    sf: Optional[Salesforce] = None,
    limit: Optional[int] = None,
    chunk_size: int = 40,
    workers: int = 5,
    id_batch_size: int = 200,
    serp_workers: int = 5,
    serp_pause: float = 0.1,
) -> pd.DataFrame:
    """Fetch accounts and enrich them using SerpApi.

    This convenience wrapper calls `fetch_accounts(...)` and then
    `enrich_with_serpapi(...)` (if available). The enrichment will prefer any
    existing Google place/data id fields in the fetched dataframe, then the
    Website, then Name+address fields.
    """
    df = fetch_accounts(sf=sf, limit=limit, chunk_size=chunk_size, workers=workers, id_batch_size=id_batch_size)
    if enrich_with_serpapi is None:
        raise RuntimeError("serp_fetch.enrich_with_serpapi is not available. Ensure serp_fetch.py exists and 'google-search-results' is installed.")
    return enrich_with_serpapi(df, api_key=api_key, workers=serp_workers, pause=serp_pause)

