from typing import List, Optional
from pydantic import BaseModel
from openai import OpenAI
import streamlit as st

def get_openai_client() -> OpenAI:
    api_key = st.secrets.get("OPENAI_API_KEY", os.getenv("OPENAI_API_KEY"))
    return OpenAI(api_key=api_key)

client = get_openai_client()

# Row shape designed to cover BOTH docs.
# For invoice rows, weights/liters may be null.
# For EAD rows, invoice_value/lot may be null.
class PackingRow(BaseModel):
    # shared identifiers
    description: Optional[str] = None
    designation: Optional[str] = None  # EAD can call it designation
    denominazione_origine: Optional[str] = None
    designazione_commerciale: Optional[str] = None
    cn_code: Optional[str] = None
    abv_percent: Optional[float] = None

    # invoice-ish
    bottles: Optional[int] = None
    bottle_liters: Optional[float] = None
    cases: Optional[int] = None
    bottles_per_case: Optional[int] = None      # e.g., 6
    bottles_total: Optional[int] = None
    invoice_value_eur: Optional[float] = None
    lot: Optional[str] = None     

    # ead-ish
    progressivo: Optional[int] = None
    ead_liters: Optional[float] = None
    ead_gross_kg: Optional[float] = None
    ead_net_kg: Optional[float] = None

class InvoiceAI(BaseModel):
    invoice_number: Optional[str] = None
    invoice_date: Optional[str] = None
    arc: Optional[str] = None
    incoterm: Optional[str] = None
    rows: List[PackingRow] = []
    invoice_value_eur: Optional[float] = None
    lot: Optional[str] = None

class EADAI(BaseModel):
    arc: Optional[str] = None
    invoice_number: Optional[str] = None
    invoice_date: Optional[str] = None
    rows: List[PackingRow] = []
    progressivo: Optional[int] = None
    ead_liters: Optional[float] = None
    ead_gross_kg: Optional[float] = None
    ead_net_kg: Optional[float] = None

SYSTEM = """
You are a customs logistics expert extracting structured product-line data for a Packing List.
Input text can be in any language and any layout (tables, wrapped lines, page breaks).

Goal: return one row per wine product line with consistent fields used for customs clearance.

Core definitions (language-agnostic):
- cases = number of shipping cartons/packages/colli/cartons/colis (NOT pallets). Usually an integer.
- bottles_per_case = count of bottles inside each case/carton.
- bottle_liters = bottle volume in liters (e.g., 0.75). Convert any units:
  - 750 ml -> 0.75
  - 75 cl -> 0.75
  - 1.5 L -> 1.5
- bottles_total (if explicitly stated) = total bottles for the line.

Extraction rules:
1) Never interpret a volume (e.g., “750”, “75cl”, “750ml”, “0.75L”) as a bottle count.
2) Never interpret a “quantity” field that looks like volume formatting (e.g., “750,000” next to “BT”) as bottle count.
3) Prefer extracting cases from the line’s “packages/colli/cartons/colis” column/field.
4) Extract bottles_per_case from packaging patterns such as:
   - “CRT DA 6 BTLS”, “CARTON DA 6”, “6 BTLS”, “case of 6 bottles”
   - “6x750ml”, “12 x 75cl”, “6*0.75L”, “12×0,75”
   (In these, bottles_per_case=6 or 12 and bottle_liters=0.75)
5) If cases is missing but bottles_total is present, set bottles_total and leave cases null.
6) If a value is not present, set it to null. Do not guess.

Return one row per product line. Do not merge different wines into one row.

Self-check before output:
- If cases <= 30 and bottles_per_case is present and equals 6/12 and bottle_liters is 0.75, verify that liters = cases*bottles_per_case*bottle_liters is plausible (not required to output liters).
- If you extracted cases=6 while also extracting bottles_per_case=6, re-check: cases should be the package count, not bottles_per_case.

"""

EAD_SYSTEM = """
You are a customs / EMCS (e-AD / EAD) data extraction expert.

The input is raw text extracted from an EAD / e-AD printout. It may be in any language.
Return ONLY data that is explicitly present. If unknown, use null.

CRITICAL: EAD has TWO relevant areas:
A) Product lines: (17.X) DETTAGLI DEL DAA - PROGRESSIVO N. X
   - ead_liters comes from (17.X.d) Quantità (Lt. a 20°)
   - ead_gross_kg comes from (17.X.e) Massa lorda (Kg)
   - ead_net_kg comes from (17.X.f) Massa netta (Kg)
   - cn_code comes from (17.X.c) Codice NC
   - abv_percent comes from (17.X.g) Titolo alcolometrico
   - description/designation comes from (17.X.p) Designazione commerciale 

B) Packaging lines: (17.1.X) IMBALLAGGI - PROGRESSIVO N. X
   - cases (cartons/colli) comes from (17.1.X.b) Numero di colli
   - IMPORTANT: Map packaging progressivo X to product progressivo X (same X).

Header fields to extract if present:
- arc (look for "ARC:" or "(1.d) ARC")
- invoice_number (look for "Numero della fattura" / "invoice number")
- invoice_date (look for "Data della fattura" / "invoice date")

Additionally extract:
- denominazione_origine from (17.X.l) Denominazione di origine
- designazione_commerciale from (17.X.p) Designazione commerciale


Output one row per product progressivo (17.1, 17.2, ...).
Each row MUST include progressivo.
cases MUST be taken from the matching packaging progressivo if present.
Do not confuse "Numero progressivo" in the header with product progressivo.

Do NOT guess. Use null when missing.
"""

def ai_extract_invoice(text: str, model: str = "gpt-4o") -> InvoiceAI:
    resp = client.responses.parse(
        model=model,
        temperature=0,
        top_p=1,
        input=[
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": text},
        ],
        text_format=InvoiceAI,
        store=False,
    )
    return resp.output_parsed

def ai_extract_ead(text: str, model: str = "gpt-4o") -> EADAI:
    resp = client.responses.parse(
        model=model,
        temperature=0,
        top_p=1,
        input=[
            {"role": "system", "content": EAD_SYSTEM},
            {"role": "user", "content": text},
        ],
        text_format=EADAI,
        store=False,
    )
    return resp.output_parsed
