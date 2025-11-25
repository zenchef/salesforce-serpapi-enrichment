"""Propose labels for Known_Internal_Issue records.

This script reads a CSV of Known_Internal_Issue__c records (export from Salesforce)
and adds three columns:
 - Proposed_Label: a short label/category proposed by heuristics
 - Proposed_Confidence: float between 0 and 1 indicating confidence
 - Proposed_Reason: short explanation of why the label was chosen

Usage:
python propose_label.py --input /path/to/Known_Internal_Issue__c-19_11_2025.csv --output out/labeled.csv

If no --output is provided, the script writes to the same directory with
`_labeled.csv` suffix.
"""
from __future__ import annotations

import argparse
import csv
import os
import re
from typing import Dict, Iterable, List, Tuple

import pandas as pd


LABEL_PRIORITY: List[Tuple[str, str]] = [
    ("onboarding", "Onboarding"),
    ("account", "Account"),
    ("contact", "Contact"),
    ("lead", "Lead"),
    ("opportunity", "Sales Opportunity"),
    ("order", "Order/Quote"),
    ("quote", "Order/Quote"),
    ("churn", "Churn"),
    ("customer", "Customer"),
    ("report", "Reporting"),
    ("product", "Product"),
    ("user", "User"),
    ("payment", "Billing"),
    ("migration", "Migration"),
    ("error", "Bug"),
    ("zenchef id", "Data Issue"),
    ("zendesk", "Support"),
]


def propose_label_for_text(text: str) -> Tuple[str, float, str]:
    """Given free text (categories or name), propose a label.

    Returns (label, confidence, reason)
    """
    if not text or not isinstance(text, str):
        return ("Other", 0.0, "no text")

    txt = text.lower()
    """Compatibility wrapper that forwards to fetcher.labeler.LabelProposer.

    This keeps the original CLI entrypoint while using the refactored OOP code.
    """
    from __future__ import annotations

    import argparse
    from typing import Optional

    from fetcher.labeler import LabelProposer


    def main(argv: Optional[list[str]] = None) -> int:
        p = argparse.ArgumentParser(description="Propose labels for Known_Internal_Issue CSV")
        p.add_argument("--input", "-i", required=True, help="Path to input CSV file")
        p.add_argument("--output", "-o", required=False, help="Path to output CSV file")
        args = p.parse_args(argv)

        proposer = LabelProposer()
        out = proposer.process_csv(args.input, args.output)
        print(f"Wrote labeled CSV to: {out}")
        return 0


    if __name__ == "__main__":
        raise SystemExit(main())
