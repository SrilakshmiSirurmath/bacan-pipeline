from pathlib import Path
from stage2_parse_invoice import parse_invoice
from stage2_parse_ead import parse_ead

invoice_text = Path("out/invoice_text.txt").read_text()
ead_text = Path("out/ead_text.txt").read_text()

inv = parse_invoice(invoice_text)
ead = parse_ead(ead_text)

print("Invoice:", inv["invoice_number"], inv["invoice_date"], "arc:", inv["arc"], "incoterm:", inv["incoterm"])
print("Invoice totals:", inv["totals"])
print("Invoice lines:", len(inv["lines"]))
print(inv["lines"][0])

print("\nEAD ARC:", ead["arc"])
print("EAD lines:", len(ead["lines"]))
print(ead["lines"][0])
