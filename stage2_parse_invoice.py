import re
from typing import Dict, List, Optional


def euro_to_float(s: str) -> float:
    # "1.650,00" -> 1650.00
    return float(s.replace(".", "").replace(",", ".").strip())


def qty_to_int(q: str) -> int:
    # "750,000" means 750 bottles in this invoice format
    return int(round(euro_to_float(q)))


def parse_invoice(invoice_text: str) -> Dict:
   # More robust: finds "... EURO 64/00 21/01/2026 ..."
    m = re.search(r"\bEURO\s+([0-9]+/[0-9]+)\s+([0-9]{2}/[0-9]{2}/[0-9]{4})\b", invoice_text)
    inv_no = m.group(1) if m else None
    inv_date = m.group(2) if m else None


    # Total cases / gross / net are on page 2 in the narrative
    total_cases = None
    m_cases = re.search(r"Nr\.\s*(\d+)\s*crt da\s*(\d+)\s*bottiglie", invoice_text, re.IGNORECASE)
    if m_cases:
        total_cases = int(m_cases.group(1))

    gross_kg = None
    net_kg = None
    m_gross = re.search(r"Peso Lordo Kg\.\s*([0-9\.,]+)", invoice_text, re.IGNORECASE)
    m_net = re.search(r"peso netto kg\.\s*([0-9\.,]+)", invoice_text, re.IGNORECASE)
    if m_gross:
        gross_kg = euro_to_float(m_gross.group(1))
    if m_net:
        net_kg = euro_to_float(m_net.group(1))

    arc = re.search(r"\bArc\s*:\s*([A-Z0-9]+)", invoice_text, re.IGNORECASE)
    incoterm = re.search(r"Incoterms\s*=\s*([A-Z]{3})", invoice_text)

    # Line starts: CODE + YEAR + DESCRIPTION ... BT qty cases unitprice net ...
    # Capture the whole line, then we’ll look ahead for GRADI/Nomenclatura/Lotto/packaging.
    line_pattern = re.compile(
    r"^(?P<code>[A-Z]{3,6}\s+\d{2})\s+"
    r"(?P<desc>.+?)\s+BT\s+"
    r"(?P<bottles>[0-9\.,]+)\s+"
    r"(?P<cases>\d+)\s+"
    r"(?P<price>[0-9\.,]+)\s+"
    r"(?P<value>[0-9\.,]+)\s+\d+\s*$",
    re.MULTILINE
)

    matches = list(line_pattern.finditer(invoice_text))

    lines = []
    for i, m in enumerate(matches):
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(invoice_text)
        block = invoice_text[start:end]

        code_year = m.group("code").strip()
        desc_head = m.group("desc").strip()
        bottles = qty_to_int(m.group("bottles"))
        cases = int(m.group("cases"))
        unit_price = euro_to_float(m.group("price"))
        value = euro_to_float(m.group("value"))

        # CN / Lot / ABV / packaging inside the block
        cn = re.search(r"Nomenclatura\s*:\s*([0-9]{6,10})", block)
        lot = re.search(r"Lotto\s+num\.?\s*([A-Z0-9\.]+)", block)
        abv = re.search(r"GRADI\s*([0-9]+(?:,[0-9]+)?)%\s*VOL", block)

        # bottles per case + bottle size (cl)
        bpc = re.search(r"CRT\s+DA\s+(\d+)\s+BTLS", block, re.IGNORECASE)
        cl = re.search(r"CL\.?(\d+)", block, re.IGNORECASE)

        bottles_per_case = int(bpc.group(1)) if bpc else None
        bottle_liters = (int(cl.group(1)) / 100.0) if cl else None

        lines.append({
            "code_year": code_year,
            "description": " ".join(desc_head.split()),
            "bottles": bottles,
            "cases": cases,
            "unit_price_eur": unit_price,
            "invoice_value_eur": value,
            "cn_code": cn.group(1) if cn else None,
            "lot": lot.group(1) if lot else None,
            "abv_percent": float(abv.group(1).replace(",", ".")) if abv else None,
            "bottles_per_case": bottles_per_case,
            "bottle_liters": bottle_liters,
        })

    return {
        "invoice_number": inv_no,
        "invoice_date": inv_date,
        "incoterm": incoterm.group(1) if incoterm else None,
        "arc": arc.group(1) if arc else None,
        "totals": {
            "total_cases": total_cases,
            "gross_kg": gross_kg,
            "net_kg": net_kg,
        },
        "lines": lines,
    }
