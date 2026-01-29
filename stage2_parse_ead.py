import re
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict


def _to_float(x: Optional[str]) -> Optional[float]:
    if x is None:
        return None
    return float(x.strip())


def parse_ead(ead_text: str) -> Dict:
    # Header
    arc = re.search(r"\(1\.d\)\s*ARC:\s*([A-Z0-9]+)", ead_text)
    invoice_no = re.search(r"\(9\.b\)\s*Numero della fattura:\s*([0-9]+/[0-9]+)", ead_text)
    invoice_date = re.search(r"\(9\.c\)\s*Data della fattura:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})", ead_text)

    # Product blocks: "(17) DETTAGLI DEL DAA - PROGRESSIVO N. X"
    prod_blocks = re.split(r"\(17\)\s*DETTAGLI DEL DAA - PROGRESSIVO N\.\s*", ead_text)
    products = []

    for b in prod_blocks[1:]:
        # progressivo is first token in the block
        m_prog = re.match(r"(\d+)", b.strip())
        if not m_prog:
            continue
        prog = int(m_prog.group(1))

        cn = re.search(r"Codice NC:\s*([0-9]{6,10})", b)
        abv = re.search(r"Titolo alcolometrico:\s*([0-9]+(?:\.[0-9]+)?)", b)
        liters = re.search(r"Quantità\s*\(Lt\.\s*a\s*20°\):\s*([0-9]+(?:\.[0-9]+)?)", b)
        gross = re.search(r"Massa lorda\s*\(Kg\):\s*([0-9]+(?:\.[0-9]+)?)", b)
        net = re.search(r"Massa netta\s*\(Kg\):\s*([0-9]+(?:\.[0-9]+)?)", b)
        # Capture the "Designazione ..." text that appears after "(17.x.p) Designazione"
        designation = re.search(r"\(17\.\d+\.p\)\s*Designazione\s+(.+)", b)

        products.append({
            "progressivo": prog,
            "cn_code": cn.group(1) if cn else None,
            "abv_percent": _to_float(abv.group(1)) if abv else None,
            "ead_liters": _to_float(liters.group(1)) if liters else None,
            "ead_gross_kg": _to_float(gross.group(1)) if gross else None,
            "ead_net_kg": _to_float(net.group(1)) if net else None,
            "designation": (designation.group(1).strip() if designation else None),
            "cases": None,  # filled from IMBALLAGGI
        })

    # Packaging blocks: "(17.1) IMBALLAGGI - PROGRESSIVO N. X"
    pack_blocks = re.split(r"\(17\.1\)\s*IMBALLAGGI - PROGRESSIVO N\.\s*", ead_text)
    prog_to_cases = {}
    for pb in pack_blocks[1:]:
        m_prog = re.match(r"(\d+)", pb.strip())
        if not m_prog:
            continue
        prog = int(m_prog.group(1))
        m_cases = re.search(r"Numero di colli:\s*(\d+)", pb)
        if m_cases:
            prog_to_cases[prog] = int(m_cases.group(1))

    # Attach cases to products
    for p in products:
        if p["progressivo"] in prog_to_cases:
            p["cases"] = prog_to_cases[p["progressivo"]]

    return {
        "arc": arc.group(1) if arc else None,
        "ead_invoice_number": invoice_no.group(1) if invoice_no else None,
        "ead_invoice_date": invoice_date.group(1) if invoice_date else None,
        "lines": products,
    }
