"""Enrich Account DataFrame rows with Google/Maps data using SerpApi.

This module provides `enrich_with_serpapi` which accepts a pandas DataFrame
with Account rows (must contain at least `Id` and preferably `Website` or
`Name`) and calls SerpApi (Google Maps) to fetch rating, review counts,
place ids, price level, and other fields.

The implementation is defensive: it attempts a few common response keys and
falls back gracefully when a key is missing. Calls are parallelized with a
ThreadPoolExecutor; tune `workers` and `pause` to control rate.

You need the `google-search-results` package installed (already present in
requirements.txt as `google-search-results`).

Note: SerpApi response shapes vary between engines and changes; the parser
tries multiple common key names. If you need exact mappings for your org,
we can tailor the parser after inspecting a sample SerpApi response.
"""
from __future__ import annotations

import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

# Robust import of SerpApi client. Different package versions expose the
# GoogleSearch class in different places; try common locations and provide a
# helpful error message if the import fails.
try:
    from serpapi import GoogleSearch
except Exception:
    try:
        # older/newer layout
        from serpapi.google_search_results import GoogleSearch
    except Exception:
        try:
            import serpapi.google_search_results as _gsr

            GoogleSearch = _gsr.GoogleSearch
        except Exception:
            raise ImportError(
                "Could not import GoogleSearch from the 'serpapi' package.\n"
                "Please install the official client 'google-search-results' (pip install google-search-results) "
                "and ensure there is no local module named 'serpapi' shadowing the package."
            )


def _first_key_recursive(obj: Any, keys: Iterable[str]) -> Optional[Any]:
    """Recursively search `obj` (dict/list) for the first occurrence of any key in `keys`.

    Returns the value for the first key found or None.
    """
    if obj is None:
        return None
    if isinstance(obj, dict):
        for k in keys:
            if k in obj:
                return obj[k]
        for v in obj.values():
            found = _first_key_recursive(v, keys)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _first_key_recursive(item, keys)
            if found is not None:
                return found
    return None


def _build_query_from_row(row: pd.Series) -> Optional[str]:
    """Build a sensible search query from an Account row.

    Prefer `Website` when present, otherwise fallback to `Name` and other
    address-like fields.
    """
    # Prefer existing Google place/id fields if present (more reliable)
    for id_field in ("Google_Place_ID__c", "Google_Data_ID__c", "Google_Place_ID", "place_id"):
        v = row.get(id_field)
        if pd.notna(v) and v:
            return str(v)

    if pd.notna(row.get("Website")) and row.get("Website"):
        # Use website as the next-strongest signal
        return str(row.get("Website"))
    name = row.get("Name")
    if pd.notna(name) and name:
        parts = [str(name)]
        for key in ("BillingCity", "BillingCountry", "City", "Country", "Phone"):
            v = row.get(key)
            if pd.notna(v) and v:
                parts.append(str(v))
        return " ".join(parts)
    return None


def _parse_serp_result(result: Dict[str, Any]) -> Dict[str, Any]:
    """Extract desired Google fields from a SerpApi result dict.

    This is heuristic and tries several common key names.
    """
    out: Dict[str, Any] = {}

    # Place/data ids
    out["Google_Place_ID__c"] = _first_key_recursive(result, ("place_id", "placeId", "place_id_token", "place_id_token"))
    out["Google_Data_ID__c"] = _first_key_recursive(result, ("data_id", "data_id_token", "business_id", "id"))

    # Ratings and counts (Google uses rating and user_ratings_total)
    out["Google_Rating__c"] = _first_key_recursive(result, ("rating", "reviews_rating", "score"))
    out["Google_Review_Count__c"] = _first_key_recursive(result, ("user_ratings_total", "review_count", "reviews_count", "total_reviews"))

    # Price level
    price = _first_key_recursive(result, ("price_level", "price", "price_str"))
    if isinstance(price, (int, float)):
        # convert numeric price level to dollar signs
        out["Google_Price__c"] = "$" * int(price) if price > 0 else ""
    else:
        out["Google_Price__c"] = price

    # Updated date: we'll set to now when we fetched
    out["Google_Updated_Date__c"] = datetime.utcnow().isoformat()

    # Restaurant type / category
    cat = _first_key_recursive(result, ("category", "categories", "type", "types"))
    if isinstance(cat, list):
        out["Restaurant_Type__c"] = ", ".join(cat)
    else:
        out["Restaurant_Type__c"] = cat

    # Booking extension presence heuristic
    booking = _first_key_recursive(result, ("has_booking", "booking_enabled", "has_booking_option"))
    if booking is None:
        # fallback to searching for 'book' in textual snippets
        snippet = _first_key_recursive(result, ("snippet", "description", "text"))
        out["Has_Google_Accept_Bookings_Extension__c"] = bool(snippet and "book" in str(snippet).lower())
    else:
        out["Has_Google_Accept_Bookings_Extension__c"] = bool(booking)

    # Closed status heuristic
    status = _first_key_recursive(result, ("status", "business_status", "place_status"))
    if status and "close" in str(status).lower():
        out["Prospection_Status__c"] = "Permanently Closed"
    else:
        # some responses contain a field like `permanently_closed` or `closed`
        closed_flag = _first_key_recursive(result, ("permanently_closed", "closed"))
        out["Prospection_Status__c"] = "Permanently Closed" if closed_flag else None

    return out


def enrich_with_serpapi(
    df: pd.DataFrame,
    api_key: Optional[str] = None,
    workers: int = 5,
    pause: float = 0.1,
    engine: str = "google_maps",
    save_csv: Optional[str] = None,
    max_retries: int = 3,
    backoff_factor: float = 1.0,
    hl: Optional[str] = None,
    gl: Optional[str] = None,
    google_domain: Optional[str] = None,
) -> pd.DataFrame:
    """Enrich `df` with Google fields using SerpApi.

    Parameters
    - df: input DataFrame, must have `Id` column (and preferably `Website` or `Name`).
    - api_key: your SerpApi API key.
    - workers: thread pool size for parallel requests.
    - pause: seconds to sleep between issuing requests (per-worker) to reduce rate pressure.
    - engine: SerpApi engine to use (default `google_maps`).

    Returns a new DataFrame with extra columns added.
    """
    if "Id" not in df.columns:
        raise ValueError("DataFrame must contain an 'Id' column")

    # fallback to environment variable or GoogleSearch global if api_key omitted
    if not api_key:
        api_key = GoogleSearch.SERP_API_KEY if getattr(GoogleSearch, "SERP_API_KEY", None) else None
        if not api_key:
            import os

            api_key = os.getenv("SERPAPI_API_KEY")
    if not api_key:
        raise ValueError("SerpApi API key not provided (api_key param or SERPAPI_API_KEY env)")

    results_by_id: Dict[str, Dict[str, Any]] = {}

    def _worker_search(idx: int, rid: str, row: pd.Series) -> Dict[str, Any]:
        # Prefer an explicit place_id parameter when available
        place_id = None
        for id_field in ("Google_Place_ID__c", "Google_Place_ID", "place_id"):
            v = row.get(id_field)
            if pd.notna(v) and v:
                place_id = str(v)
                break

        q = None if place_id else _build_query_from_row(row)
        if not q and not place_id:
            return {}

        attempts = 0
        while attempts < max_retries:
            attempts += 1
            params = {"engine": engine, "api_key": api_key}
            # prefer explicit place_id when available
            if place_id:
                params["place_id"] = place_id
            else:
                params["q"] = q

            # add optional localization parameters if available
            # build a sensible `location` from Billing/City/Country fields
            loc = None
            for key in ("location", "BillingCity", "BillingCountry", "City", "Country"):
                v = row.get(key)
                if pd.notna(v) and v:
                    if loc:
                        loc = f"{loc}, {v}"
                    else:
                        loc = str(v)
            if loc:
                params["location"] = loc
            if hl:
                params["hl"] = hl
            if gl:
                params["gl"] = gl
            if google_domain:
                params["google_domain"] = google_domain

            try:
                search = GoogleSearch(params)
                resp = search.get_dict()
                parsed = _parse_serp_result(resp)
                return parsed
            except Exception as e:
                # If this was the last attempt, log and return empty dict
                if attempts >= max_retries:
                    print(f"SerpApi error for Id={rid} after {attempts} attempts: {e}")
                    return {}
                # Otherwise sleep with exponential backoff + jitter and retry
                sleep_for = backoff_factor * (2 ** (attempts - 1))
                # add small random jitter
                sleep_for = sleep_for + random.random() * 0.5
                print(f"SerpApi transient error for Id={rid}, attempt {attempts}/{max_retries}: {e}. Retrying in {sleep_for:.1f}s")
                time.sleep(sleep_for)
            finally:
                if pause:
                    time.sleep(pause)

    # Launch tasks
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {}
        for i, row in df.reset_index(drop=True).iterrows():
            rid = str(row.get("Id"))
            futures[ex.submit(_worker_search, i, rid, row)] = rid

        for fut in as_completed(futures):
            rid = futures[fut]
            try:
                res = fut.result()
            except Exception as e:
                print(f"Unexpected error enriching {rid}: {e}")
                res = {}
            results_by_id[rid] = res

    # Build a DataFrame from results and merge
    enrich_rows = []
    for rid, data in results_by_id.items():
        row = {"Id": rid}
        row.update(data or {})
        enrich_rows.append(row)

    enrich_df = pd.DataFrame.from_records(enrich_rows)

    # Merge left to preserve original ordering and rows
    merged = df.merge(enrich_df, on="Id", how="left")

    if save_csv:
        try:
            merged.to_csv(save_csv, index=False)
            print(f"Wrote enriched CSV to: {save_csv}")
        except Exception as e:
            print(f"Failed to write CSV to {save_csv}: {e}")

    return merged


if __name__ == "__main__":
    print("This module provides `enrich_with_serpapi(df, api_key, ...)`.")
    print("Import it from your script and pass your SerpApi API key.")
