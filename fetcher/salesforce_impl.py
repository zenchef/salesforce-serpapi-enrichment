from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Iterable, List, Optional

import pandas as pd
from simple_salesforce import Salesforce
from simple_salesforce.exceptions import SalesforceMalformedRequest

from account_fields import AccountFields


def _env_get(*names: str) -> Optional[str]:
    for n in names:
        v = os.environ.get(n)
        if v:
            return v
    return None


def _chunk_list(items: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(items), n):
        yield items[i : i + n]


class SalesforceFetcher:
    """OOP wrapper to fetch Account records from Salesforce."""

    def __init__(self, sf: Optional[Salesforce] = None):
        self.sf = sf or self._make_salesforce_from_env()

    def _make_salesforce_from_env(self) -> Salesforce:
        username = _env_get("SF_USERNAME", "SFDC_USERNAME", "sfdc_username")
        password = _env_get("SF_PASSWORD", "SFDC_PASSWORD", "sfdc_password")
        token = _env_get("SF_SECURITY_TOKEN", "SFDC_SECURITY_TOKEN", "sfdc_security_token")

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
            return Salesforce(username=username, password=password, security_token=token or "", **kwargs)
        return Salesforce(**kwargs)

    def fetch_accounts(
        self,
        limit: Optional[int] = None,
        chunk_size: int = 40,
        workers: int = 5,
        id_batch_size: int = 200,
    ) -> pd.DataFrame:
        sf = self.sf
        fields_all = AccountFields().all
        if "Id" not in fields_all:
            fields_all = ["Id"] + fields_all

        id_list: Optional[List[str]] = None
        if limit is not None:
            q = f"SELECT Id FROM Account LIMIT {int(limit)}"
            ids_resp = sf.query_all(q)
            id_list = [r["Id"] for r in ids_resp.get("records", [])]

        fields_no_id = [f for f in fields_all if f != "Id"]
        chunks = list(_chunk_list(fields_no_id, int(chunk_size) or 40))

        id_batches: Optional[List[List[str]]] = None
        if id_list:
            id_batches = list(_chunk_list(id_list, int(id_batch_size) or 200))

        def _run_query(select_fields: List[str], id_batch: Optional[List[str]] = None) -> List[Dict]:
            select_sql = ", ".join(["Id"] + select_fields)
            if id_batch:
                ids_sql = ", ".join([f"'{i}'" for i in id_batch])
                q = f"SELECT {select_sql} FROM Account WHERE Id IN ({ids_sql})"
            else:
                q = f"SELECT {select_sql} FROM Account"
            resp = sf.query_all(q)
            records = resp.get("records", [])
            out = []
            for r in records:
                out.append({k: v for k, v in r.items() if k != "attributes"})
            return out

        def _query_chunk(chunk_fields: List[str], id_batch: Optional[List[str]] = None) -> List[Dict]:
            try:
                return _run_query(chunk_fields, id_batch)
            except SalesforceMalformedRequest:
                # describe and filter invalid fields
                desc = sf.Account.describe()
                valid_names = {f.get("name") for f in desc.get("fields", [])}
                filtered = [f for f in chunk_fields if f in valid_names]
                if not filtered:
                    return []
                return _run_query(filtered, id_batch)

        merged: Dict[str, Dict] = {}
        tasks = []
        if id_batches:
            for idb in id_batches:
                for c in chunks:
                    tasks.append((c, idb))
        else:
            for c in chunks:
                tasks.append((c, None))

        with ThreadPoolExecutor(max_workers=int(workers) or 5) as ex:
            futures = {ex.submit(_query_chunk, c, idb): (c, idb) for (c, idb) in tasks}
            for fut in as_completed(futures):
                records = fut.result()
                for r in records:
                    rid = r.get("Id")
                    if rid is None:
                        continue
                    if rid not in merged:
                        merged[rid] = {"Id": rid}
                    merged[rid].update(r)

        rows = list(merged.values())
        if limit is not None and len(rows) > limit:
            rows = rows[:limit]

        df = pd.DataFrame.from_records(rows)
        return df
