from __future__ import annotations

import random
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# try robust import
try:
    from serpapi import GoogleSearch
except Exception:
    try:
        from serpapi.google_search_results import GoogleSearch
    except Exception:
        try:
            import serpapi.google_search_results as _gsr

            GoogleSearch = _gsr.GoogleSearch
        except Exception:
            GoogleSearch = None  # type: ignore


def _first_key_recursive(obj: Any, keys: Iterable[str]) -> Optional[Any]:
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
    for id_field in ("Google_Place_ID__c", "Google_Data_ID__c", "Google_Place_ID", "place_id"):
        v = row.get(id_field)
        if pd.notna(v) and v:
            return str(v)
    if pd.notna(row.get("Website")) and row.get("Website"):
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
    out: Dict[str, Any] = {}
    out["Google_Place_ID__c"] = _first_key_recursive(result, ("place_id", "placeId", "place_id_token"))
    out["Google_Data_ID__c"] = _first_key_recursive(result, ("data_id", "data_id_token", "business_id", "id"))
    out["Google_Rating__c"] = _first_key_recursive(result, ("rating", "reviews_rating", "score"))
    out["Google_Review_Count__c"] = _first_key_recursive(result, ("user_ratings_total", "review_count", "reviews_count", "total_reviews"))
    price = _first_key_recursive(result, ("price_level", "price", "price_str"))
    if isinstance(price, (int, float)):
        out["Google_Price__c"] = "$" * int(price) if price > 0 else ""
    else:
        out["Google_Price__c"] = price
    out["Google_Updated_Date__c"] = datetime.utcnow().isoformat()
    cat = _first_key_recursive(result, ("category", "categories", "type", "types"))
    if isinstance(cat, list):
        out["Restaurant_Type__c"] = ", ".join(cat)
    else:
        out["Restaurant_Type__c"] = cat
    booking = _first_key_recursive(result, ("has_booking", "booking_enabled", "has_booking_option"))
    if booking is None:
        snippet = _first_key_recursive(result, ("snippet", "description", "text"))
        out["Has_Google_Accept_Bookings_Extension__c"] = bool(snippet and "book" in str(snippet).lower())
    else:
        out["Has_Google_Accept_Bookings_Extension__c"] = bool(booking)
    status = _first_key_recursive(result, ("status", "business_status", "place_status"))
    if status and "close" in str(status).lower():
        out["Prospection_Status__c"] = "Permanently Closed"
    else:
        closed_flag = _first_key_recursive(result, ("permanently_closed", "closed"))
        out["Prospection_Status__c"] = "Permanently Closed" if closed_flag else None
    return out


class SerpEnricher:
    """Object-oriented SerpApi enricher."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key

    def enrich(
        self,
        df: pd.DataFrame,
        workers: int = 5,
        pause: float = 0.1,
        engine: str = "google_maps",
        save_csv: Optional[str] = None,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
        hl: Optional[str] = None,
        gl: Optional[str] = None,
        google_domain: Optional[str] = None,
        progress_interval: int = 250,
    ) -> pd.DataFrame:
        if GoogleSearch is None:
            raise ImportError("SerpApi GoogleSearch client not available; install google-search-results")
        logger.info("Starting enrichment run: rows=%d workers=%d engine=%s", len(df), workers, engine)
        if "Id" not in df.columns:
            raise ValueError("DataFrame must contain an 'Id' column")

        # resolve api key
        if not self.api_key:
            self.api_key = getattr(GoogleSearch, "SERP_API_KEY", None)
        if not self.api_key:
            import os

            self.api_key = os.getenv("SERPAPI_API_KEY")
        if not self.api_key:
            raise ValueError("SerpApi API key not provided")

        results_by_id: Dict[str, Dict[str, Any]] = {}

        # Helper to decide whether to enrich a row: only enrich when
        # - there is no existing Google place id, and
        # - the account does NOT look like a hotel (heuristic on common fields)
        def _should_enrich(row: pd.Series) -> bool:
            # check existing place id fields
            for f in ("Google_Place_ID__c", "Google_Data_ID__c", "Google_Place_ID", "place_id"):
                v = row.get(f)
                if pd.notna(v) and v:
                    return False
            # check hotel-like indicators in several fields
            hotel_fields = ("Restaurant_Type__c", "Type", "Industry", "Account_Type__c", "Business_Type__c")
            for hf in hotel_fields:
                v = row.get(hf)
                if pd.notna(v) and isinstance(v, str) and "hotel" in v.lower():
                    return False
            # also check Name for 'hotel' as a last resort
            name = row.get("Name")
            if pd.notna(name) and isinstance(name, str) and "hotel" in name.lower():
                return False
            return True

        def _worker_search(idx: int, rid: str, row: pd.Series) -> Dict[str, Any]:
            # determine place_id or q
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
                params = {"engine": engine, "api_key": self.api_key}
                if place_id:
                    params["place_id"] = place_id
                else:
                    params["q"] = q
                # location and localization
                loc = None
                for key in ("location", "BillingCity", "BillingCountry", "City", "Country"):
                    v = row.get(key)
                    if pd.notna(v) and v:
                        loc = f"{loc}, {v}" if loc else str(v)
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
                    if parsed.get("Google_Place_ID__c"):
                        logger.debug("Enriched rid=%s place_id=%s", rid, parsed.get("Google_Place_ID__c"))
                    return parsed
                except Exception as e:
                    if attempts >= max_retries:
                        logger.warning("SerpApi error rid=%s after %d attempts: %s", rid, attempts, e)
                        return {}
                    sleep_for = backoff_factor * (2 ** (attempts - 1)) + random.random() * 0.5
                    logger.info(
                        "Transient SerpApi error rid=%s attempt=%d/%d: %s; retrying in %.1fs",
                        rid,
                        attempts,
                        max_retries,
                        e,
                        sleep_for,
                    )
                    time.sleep(sleep_for)
                finally:
                    if pause:
                        time.sleep(pause)

        # Build list of rows to enrich to avoid unnecessary API calls
        rows_to_search: List[tuple] = []  # (idx, rid, row)
        for i, row in df.reset_index(drop=True).iterrows():
            rid = str(row.get("Id"))
            if _should_enrich(row):
                rows_to_search.append((i, rid, row))
            else:
                # preserve explicit empty result so merge will keep original row
                results_by_id[rid] = {}
        logger.info("Rows needing enrichment: %d (skipped=%d)", len(rows_to_search), len(df) - len(rows_to_search))

        # Parallelize SerpApi calls only for selected rows
        completed = 0
        errors = 0
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(_worker_search, i, rid, row): rid for (i, rid, row) in rows_to_search}
            for fut in as_completed(futures):
                rid = futures[fut]
                try:
                    res = fut.result()
                except Exception as e:
                    logger.error("Unexpected worker error rid=%s: %s", rid, e)
                    res = {}
                    errors += 1
                results_by_id[rid] = res
                completed += 1
                if progress_interval and completed % progress_interval == 0:
                    logger.info(
                        "Progress: %d/%d (%.1f%%) errors=%d", completed, len(rows_to_search), (completed/len(rows_to_search))*100 if rows_to_search else 100, errors
                    )
        logger.info("Enrichment complete: processed=%d errors=%d", completed, errors)

        # Build a DataFrame from results and merge
        enrich_rows = []
        for rid, data in results_by_id.items():
            row = {"Id": rid}
            row.update(data or {})
            enrich_rows.append(row)

        enrich_df = pd.DataFrame.from_records(enrich_rows)
        merged = df.merge(enrich_df, on="Id", how="left")
        if save_csv:
            try:
                merged.to_csv(save_csv, index=False)
                logger.info("Saved enriched CSV path=%s rows=%d", save_csv, len(merged))
            except Exception as e:
                logger.error("Failed to write enriched CSV path=%s error=%s", save_csv, e)
        return merged

