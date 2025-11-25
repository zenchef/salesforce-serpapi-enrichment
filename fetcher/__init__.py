"""Fetcher package: provides OOP wrappers for Salesforce and SerpApi enrichment."""
from .salesforce import SalesforceFetcher
from .serp import SerpEnricher
from .labeler import LabelProposer

__all__ = ["SalesforceFetcher", "SerpEnricher", "LabelProposer"]
