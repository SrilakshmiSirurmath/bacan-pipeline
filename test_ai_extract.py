from pathlib import Path
from stage2b_ai_extract_openai import ai_extract_invoice, ai_extract_ead

inv_txt = Path("out/invoice_text.safe.txt").read_text()
ead_txt = Path("out/ead_text.safe.txt").read_text()

inv = ai_extract_invoice(inv_txt)
ead = ai_extract_ead(ead_txt)

print("Invoice rows:", len(inv.rows))
print("EAD rows:", len(ead.rows))

if inv.rows:
    print("Invoice sample row:", inv.rows[0])

if ead.rows:
    print("EAD sample row:", ead.rows[0])
