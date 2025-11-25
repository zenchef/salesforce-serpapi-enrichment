from __future__ import annotations

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
]


def propose_label_for_text(text: str):
    if not text or not isinstance(text, str):
        return ("Other", 0.0, "no text")
    txt = text.lower()
    for token, label in LABEL_PRIORITY:
        if token in txt:
            return (label, 1.0, f"matched token '{token}' in text")
    if re.search(r"cannot|can't|cannot create|fail|error", txt):
        return ("Bug", 0.6, "contains error/fail wording")
    if re.search(r"onboarding|onboard", txt):
        return ("Onboarding", 0.9, "contains onboarding wording")
    return ("Other", 0.2, "no strong match")


class LabelProposer:
    def __init__(self):
        pass

    def propose_row_label(self, row: Dict[str, str]):
        impacted = row.get("Impacted_Categories__c", "")
        name = row.get("Name", "")
        if impacted and isinstance(impacted, str):
            label, conf, reason = propose_label_for_text(impacted)
            if conf >= 0.6:
                return {"Proposed_Label": label, "Proposed_Confidence": conf, "Proposed_Reason": reason}
        label, conf, reason = propose_label_for_text(name)
        return {"Proposed_Label": label, "Proposed_Confidence": conf, "Proposed_Reason": reason}

    def process_csv(self, input_path: str, output_path: str | None = None) -> str:
        df = pd.read_csv(input_path)
        for c in ("Id", "Impacted_Categories__c", "Name"):
            if c not in df.columns:
                df[c] = ""
        proposals = df.apply(lambda r: self.propose_row_label(r.to_dict()), axis=1)
        prop_df = pd.DataFrame(list(proposals))
        out = pd.concat([df.reset_index(drop=True), prop_df.reset_index(drop=True)], axis=1)
        if not output_path:
            base, ext = os.path.splitext(os.path.basename(input_path))
            out_dir = os.path.join(os.getcwd(), "out")
            os.makedirs(out_dir, exist_ok=True)
            output_path = os.path.join(out_dir, f"{base}_labeled{ext}")
        out.to_csv(output_path, index=False)
        return output_path
