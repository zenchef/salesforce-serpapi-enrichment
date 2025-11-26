#!/usr/bin/env python3
"""Quick smoke test for SerpApi (prints top-level keys and first organic result)."""
from serpapi import GoogleSearch
import os
import sys
import json


def main():
    api_key = os.getenv("SERPAPI_API_KEY")
    # allow passing key as first arg for convenience
    if not api_key and len(sys.argv) > 1:
        api_key = sys.argv[1]
    if not api_key:
        print("ERROR: SERPAPI_API_KEY not set (or pass key as first arg)")
        return 2

    params = {
        "q": "Coffee",
        "location": "Austin, Texas, United States",
        "hl": "en",
        "gl": "us",
        "google_domain": "google.com",
        "api_key": api_key,
    }

    search = GoogleSearch(params)
    try:
        res = search.get_dict()
    except Exception as e:
        print(f"SerpApi request failed: {e}")
        return 3

    print("Top-level keys:", list(res.keys()))
    organic = res.get("organic_results") or res.get("organic") or []
    if organic:
        print("First organic result:")
        print(json.dumps(organic[0], indent=2, ensure_ascii=False))
    else:
        print("No organic results found. Full response:\n")
        print(json.dumps(res, indent=2, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
