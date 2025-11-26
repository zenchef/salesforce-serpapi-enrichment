"""Fetcher package: provides OOP wrappers for Salesforce and SerpApi enrichment."""
# prefer the clean implementation module (salesforce_impl) because
# the original `salesforce.py` in this repo was corrupted. Export
# SalesforceFetcher from the stable implementation file.
from .salesforce_impl import SalesforceFetcher
from .serp import SerpEnricher
from .labeler import LabelProposer

__all__ = ["SalesforceFetcher", "SerpEnricher", "LabelProposer"]
