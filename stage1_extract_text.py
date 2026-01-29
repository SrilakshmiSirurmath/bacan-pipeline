import pdfplumber
import sys
import re
from pathlib import Path

from stage1b_redact_trim import redact, trim_invoice_text, trim_ead_text


def clean_layout_text_for_ai(text: str) -> str:
    """
    pdfplumber layout=True tends to create:
      - box drawing characters
      - very long whitespace runs
      - "table art" separators
    This reduces that noise before redaction+trimming+AI.
    """
    t = text or ""

    # Remove common box-drawing / table characters
    t = re.sub(r"[│┃╎╏┆┇┊┋╵╷╹╻╼╽╾╿─━┄┅┈┉┌┐└┘├┤┬┴┼═]+", " ", t)

    # Collapse huge whitespace runs caused by layout positioning
    t = re.sub(r"[ \t]{2,}", " ", t)

    # Strip per-line and collapse excessive blank lines
    t = "\n".join(line.strip() for line in t.splitlines())
    t = re.sub(r"\n{3,}", "\n\n", t)

    return t.strip()


def extract_text(pdf_path: str, *, layout: bool) -> str:
    text_chunks = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            page_text = page.extract_text(layout=layout) or ""
            text_chunks.append(f"\n\n--- PAGE {i+1} ---\n\n")
            text_chunks.append(page_text)
    return "\n".join(text_chunks)


def main(invoice_pdf, ead_pdf):
    out_dir = Path("out")
    out_dir.mkdir(exist_ok=True)

    # 1) Extract raw text
    # Invoice: layout=False usually best
    invoice_text = extract_text(invoice_pdf, layout=False)

    # EAD: layout=True often helps table-like blocks, then clean it
    ead_text_raw = extract_text(ead_pdf, layout=True)
    ead_text = clean_layout_text_for_ai(ead_text_raw)

    # 2) Write raw outputs
    (out_dir / "invoice_text.txt").write_text(invoice_text, encoding="utf-8")
    (out_dir / "ead_text.txt").write_text(ead_text, encoding="utf-8")

    # (Optional) keep the unclean layout raw for debugging
    (out_dir / "ead_text.layout_raw.txt").write_text(ead_text_raw, encoding="utf-8")

    # 3) Create SAFE (redacted + trimmed) for AI
    invoice_safe = trim_invoice_text(redact(invoice_text))
    ead_safe = trim_ead_text(redact(ead_text))

    (out_dir / "invoice_text.safe.txt").write_text(invoice_safe, encoding="utf-8")
    (out_dir / "ead_text.safe.txt").write_text(ead_safe, encoding="utf-8")

    print("✅ Text extracted.")
    print("✅ Raw written:")
    print(" - out/invoice_text.txt")
    print(" - out/ead_text.txt")
    print(" - out/ead_text.layout_raw.txt  (debug)")
    print("✅ Safe trimmed written:")
    print(" - out/invoice_text.safe.txt")
    print(" - out/ead_text.safe.txt")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python stage1_extract_text.py invoice.pdf ead.pdf")
        sys.exit(1)

    main(sys.argv[1], sys.argv[2])
