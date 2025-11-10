from typing import List, Optional
import os
import re
import time
import logging
import pandas as pd
from simple_salesforce import Salesforce, SalesforceMalformedRequest, SalesforceAuthenticationFailed
from account_fields import AccountFields


# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def timed(label: str, func, *args, **kwargs):
    """Measure execution time of a function call."""
    start = time.perf_counter()
    result = func(*args, **kwargs)
    elapsed = time.perf_counter() - start
    logging.info(f"{label} took {elapsed:.2f} seconds")
    return result


class SalesforceAccountFetcher:
    """Fetch Account records from Salesforce with field validation and performance logs."""

    def __init__(
        self,
        domain: Optional[str] = None,
        consumer_key: Optional[str] = None,
        consumer_secret: Optional[str] = None,
    ):

        self.domain = domain or os.getenv("DOMAIN") or "login"
        self.consumer_key = consumer_key or os.getenv("CONSUMER_KEY")
        self.consumer_secret = consumer_secret or os.getenv("CONSUMER_SECRET")

        logging.info("Connecting to Salesforce...")
        start = time.perf_counter()
        try:
            if self.consumer_key and self.consumer_secret:
                self.sf = Salesforce(
                    consumer_key=self.consumer_key,
                    consumer_secret=self.consumer_secret,
                    domain=self.domain,
                )
            else:
                if not (self.username and self.password):
                    raise ValueError("Missing Salesforce credentials.")
                self.sf = Salesforce(
                    username=self.username,
                    password=self.password,
                    security_token=self.security_token,
                    domain=self.domain,
                )

            elapsed = time.perf_counter() - start
            logging.info(f"Connected to Salesforce in {elapsed:.2f}s")

        except SalesforceAuthenticationFailed as e:
            raise RuntimeError(f"Salesforce auth failed: {e}")
        except Exception as e:
            raise RuntimeError(f"Salesforce connection failed: {e}")

        # Initialize field list
        self.account_fields = AccountFields().all

    # --- Utility Methods ---

    def _filter_valid_fields(self, fields: List[str]) -> List[str]:
        """Validate fields using Account.describe()."""
        try:
            desc = timed("Account.describe()", self.sf.Account.describe)
            api_field_names = {f.get("name") for f in desc.get("fields", [])}
        except Exception as e:
            logging.warning(
                "Could not describe Account object: %s. Proceeding without validation.", e
            )
            return fields

        valid = [f for f in fields if f in api_field_names]
        invalid = [f for f in fields if f not in api_field_names]

        if invalid:
            logging.warning("Invalid fields skipped: %s", invalid)
        return valid

    # --- Main Query Methods ---

    def get_all_accounts(self, fields: Optional[List[str]] = None, limit: Optional[int] = None) -> List[dict]:
        """Fetch all Account records (optionally limited)."""
        if fields is None:
            fields = self.account_fields

        fields = self._filter_valid_fields(fields)
        if not fields:
            raise ValueError("No valid Account fields remain after validation.")

        soql = f"SELECT {', '.join(fields)} FROM Account"
        if limit:
            soql += f" LIMIT {limit}"

        logging.info("Running SOQL query with %d fields%s", len(fields), f" (LIMIT {limit})" if limit else "")
        result = timed("Salesforce query_all()", self.sf.query_all, soql)

        records = result.get("records", [])
        for r in records:
            r.pop("attributes", None)
        logging.info("Fetched %d Account records", len(records))
        return records

    def get_all_accounts_df(
        self,
        fields: Optional[List[str]] = None,
        normalize: bool = False,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Return Account data as pandas DataFrame."""
        records = self.get_all_accounts(fields=fields, limit=limit)
        if not records:
            return pd.DataFrame()

        if normalize:
            try:
                return pd.json_normalize(records)
            except Exception:
                logging.warning("Failed to normalize records, returning raw DataFrame.")
                return pd.DataFrame(records)
        return pd.DataFrame(records)


# --- Example Usage ---
if __name__ == "__main__":
    fetcher = SalesforceAccountFetcher()

    # Run a small test to check connection and performance
    test_fields = ["Id", "Name", "AccountNumber"]
    logging.info("Testing fetch performance with limited fields and 5000 records max...")

    start = time.perf_counter()
    accounts = fetcher.get_all_accounts(fields=test_fields, limit=5000)
    total_time = time.perf_counter() - start

    logging.info("Total fetch time: %.2fs", total_time)
    if accounts:
        print(f"Fetched {len(accounts)} records")
        print("Sample keys:", list(accounts[0].keys()))
