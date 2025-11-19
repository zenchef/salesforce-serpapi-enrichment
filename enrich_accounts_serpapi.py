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
        parts = [str(name), "restaurant"]  # Add "restaurant" to help Google understand
        for key in ("BillingCity", "BillingCountry", "City", "Country", "Phone"):
            v = row.get(key)
            if pd.notna(v) and v:
                parts.append(str(v))
        return " ".join(parts)
    return None


def _parse_serp_result(result: Dict[str, Any], validate_restaurant: bool = False) -> Dict[str, Any]:
    """Extract desired Google fields from a SerpApi result dict.

    This is heuristic and tries several common key names.
    
    Args:
        result: SerpAPI response dictionary
        validate_restaurant: If True, verify the result is actually a restaurant
    """
    # Validate it's a restaurant before parsing
    if validate_restaurant:
        # Look inside place_results or local_results for the actual business data
        place_data = result.get('place_results') or result.get('local_results', {})
        if isinstance(place_data, list) and len(place_data) > 0:
            place_data = place_data[0]  # Get first result
        
        # Check if permanently closed
        if isinstance(place_data, dict):
            status = place_data.get('status') or place_data.get('business_status') or place_data.get('place_status')
            permanently_closed = place_data.get('permanently_closed') or place_data.get('closed')
            
            if permanently_closed or (status and 'close' in str(status).lower() and 'permanent' in str(status).lower()):
                print(f"   ⚠️  Skipping - permanently closed")
                return {}  # Skip closed restaurants
        
        # Check types/categories for restaurant-related keywords
        types = None
        if isinstance(place_data, dict):
            types = place_data.get('types') or place_data.get('type') or place_data.get('categories')
        
        if types:
            types_str = str(types).lower()
            restaurant_keywords = ["restaurant", "food", "cafe", "bar", "eatery", "bistro", "brasserie", "meal", "dining"]
            is_restaurant = any(keyword in types_str for keyword in restaurant_keywords)
            if not is_restaurant:
                print(f"   ⚠️  Skipping - not a restaurant (types: {types})")
                return {}  # Return empty dict for non-restaurants
        else:
            print(f"   ⚠️  No type information found - accepting result anyway")
    
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
    validate_restaurant: bool = False,
) -> pd.DataFrame:
    """Enrich `df` with Google fields using SerpApi.

    Parameters
    - df: input DataFrame, must have `Id` column (and preferably `Website` or `Name`).
    - api_key: your SerpApi API key.
    - workers: thread pool size for parallel requests.
    - pause: seconds to sleep between issuing requests (per-worker) to reduce rate pressure.
    - engine: SerpApi engine to use (default `google_maps`).
    - validate_restaurant: if True, skip results that are not restaurants (default False).

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

            # add optional localization parameters
            # For google_maps engine, use type=search instead of location
            if engine == "google_maps":
                params["type"] = "search"
                # Add ll (lat/long) if available, otherwise Google will use IP location
            
            # Use hl/gl for language/country localization (works for all engines)
            if hl:
                params["hl"] = hl
            if gl:
                params["gl"] = gl
            if google_domain:
                params["google_domain"] = google_domain

            try:
                search = GoogleSearch(params)
                resp = search.get_dict()
                
                # Debug: print the query and full response
                print(f"   Query for {rid}: {params.get('q', params.get('place_id', 'N/A'))}")
                print(f"   Response keys: {list(resp.keys())}")
                
                # Print full JSON response for debugging
                import json
                print(f"\n   📄 FULL SERPAPI RESPONSE:")
                print(json.dumps(resp, indent=2, ensure_ascii=False))
                print(f"   " + "="*70 + "\n")
                
                # Check for error in response
                if 'error' in resp:
                    print(f"   ⚠️  SerpAPI returned error: {resp['error']}")
                    return {}
                
                parsed = _parse_serp_result(resp, validate_restaurant=validate_restaurant)
                
                # Debug: show what was parsed
                non_none = {k: v for k, v in parsed.items() if v is not None}
                if non_none:
                    print(f"   ✅ Found: {', '.join(non_none.keys())}")
                else:
                    print(f"   ⚠️  No data extracted from response")
                
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
