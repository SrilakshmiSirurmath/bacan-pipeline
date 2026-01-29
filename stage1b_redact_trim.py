import re

EMAIL_RE = re.compile(r"\b[\w\.-]+@[\w\.-]+\.\w+\b", re.IGNORECASE)
# IBAN is very recognizable; good to redact
IBAN_RE  = re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{10,30}\b")
BIC_RE   = re.compile(r"\b[A-Z]{6}[A-Z0-9]{2}([A-Z0-9]{3})?\b")

def redact(text: str) -> str:
    text = EMAIL_RE.sub("[EMAIL_REDACTED]", text)
    text = IBAN_RE.sub("[IBAN_REDACTED]", text)
    text = BIC_RE.sub("[BIC_REDACTED]", text)
    return text

def trim_invoice_text(text: str, max_chars: int = 12000) -> str:
    """
    Keep a small header + the product/table region. Works across languages better than
    sending the whole PDF (cheaper + safer).
    """
    t = text
    header = t[:2000]

    # find a likely start of product lines/table
    anchors = [
        "Codice Descrizione", "Descrizione", "U.M.", "Quantità", "BT",
        "Nomenclatura", "Lotto", "Lot", "HS", "CN", "Commodity"
    ]
    start = None
    for a in anchors:
        idx = t.find(a)
        if idx != -1:
            start = idx
            break

    body = t[start:start+9000] if start is not None else t[2000:11000]
    merged = header + "\n\n--- TRIMMED BODY ---\n\n" + body
    return merged[:max_chars]

def trim_ead_text(text: str, max_chars: int = 12000) -> str:
    """
    Keep header + the (17) excise product lines (the important part for matching).
    """
    t = text
    header = t[:3500]

    m = re.search(r"\(17\)\s+DETTAGLI DEL DAA.*", t, flags=re.DOTALL)
    body = (m.group(0)[:9000] if m else t[3500:12500])

    merged = header + "\n\n--- TRIMMED (17) BLOCK ---\n\n" + body
    return merged[:max_chars]
