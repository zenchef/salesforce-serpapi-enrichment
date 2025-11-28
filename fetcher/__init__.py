"""Fetcher package: provides OOP wrappers for Salesforce and SerpApi enrichment."""
# prefer the clean implementation module (salesforce_impl) because
# the original `salesforce.py` in this repo was corrupted. Export
# SalesforceFetcher from the stable implementation file.
from .salesforce_impl import SalesforceFetcher
from .serp import SerpEnricher

# labeler module may not be present in all branches/environments; avoid importing it at package
# import time to keep the package lightweight. Import it explicitly where needed.
__all__ = ["SalesforceFetcher", "SerpEnricher"]
