import io
import json
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Tuple

import pandas as pd
import streamlit as st

# Import your existing pipeline pieces
from stage1_extract_text import (extract_text,clean_layout_text_for_ai)
from stage1b_redact_trim import redact, trim_invoice_text, trim_ead_text
from stage2b_ai_extract_openai import ai_extract_invoice, ai_extract_ead
from stage3_match_validate_excel import (
    normalize_invoice_rows,
    normalize_ead_rows,
    match_invoice_to_ead,
    validate_shipment,
    validate_lines,
    build_output_df,
    build_customs_excel
)

@dataclass
class JobResult:
    job_id: str
    invoice_name: str
    ead_name: str
    status: str          # "OK" / "WARN" / "FAIL"
    issues_count: int
    excel_bytes: Optional[bytes] = None
    issues_bytes: Optional[bytes] = None

def run_one_job(invoice_pdf_bytes: bytes, ead_pdf_bytes: bytes, model: str) -> Tuple[pd.DataFrame, List[dict], bytes, bytes]:
    """
    Runs one invoice+EAD through:
    PDF->text -> redact/trim -> AI extract -> normalize -> match -> validate -> excel bytes + issues bytes
    Returns: output_df, issues_list, excel_file_bytes, issues_json_bytes
    """
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        inv_path = td_path / "invoice.pdf"
        ead_path = td_path / "ead.pdf"
        inv_path.write_bytes(invoice_pdf_bytes)
        ead_path.write_bytes(ead_pdf_bytes)

        invoice_text = extract_text(str(inv_path),layout=False)
        ead_text_raw = extract_text(str(ead_path),layout=True)
        ead_text = clean_layout_text_for_ai(ead_text_raw)

        inv_safe = trim_invoice_text(redact(invoice_text))
        ead_safe = trim_ead_text(redact(ead_text))

        inv_ai = ai_extract_invoice(inv_safe, model=model)
        ead_ai = ai_extract_ead(ead_safe, model=model)

        inv = normalize_invoice_rows(inv_ai)
        ead = normalize_ead_rows(ead_ai)

        shipment_issues = validate_shipment(
        inv_ai, ead_ai, inv["lines"], ead["lines"],
        invoice_text=invoice_text, ead_text=ead_text
    )

        # Matching + line-level checks
        matches = match_invoice_to_ead(inv["lines"], ead["lines"])
        line_issues = validate_lines(matches)

        issues = shipment_issues + line_issues

        df = build_output_df(matches)

        excel_bytes = build_customs_excel(
        matches,
        template_path=Path(__file__).parent / "Packing List template.xlsx",
        inv_ai=inv_ai,
        )


        issues_json_bytes = json.dumps(issues, indent=2).encode("utf-8")
        return df, issues, excel_bytes, issues_json_bytes

def status_from_issues(issues: List[dict]) -> str:
    if not issues:
        return "OK"
    # If any NO_MATCH -> fail, else warn
    if any(i.get("type") == "NO_MATCH" for i in issues):
        return "FAIL"
    return "WARN"

st.set_page_config(page_title="Bacan Packing List Generator", layout="wide")
st.title("Bacan — Invoice + EAD → Packing List")

st.markdown(
    "Upload matching **Invoice PDF** and **EAD PDF** for each shipment, then generate the Packing List Excel."
)

# Sidebar config
with st.sidebar:
    st.header("Settings")
    model = st.selectbox("OpenAI model", ["gpt-4o"], index=0)
    st.caption("Tip: start with gpt-4o-mini for cost/speed, switch to gpt-4o if extraction struggles.")

st.subheader("1) Upload files (bulk)")

col1, col2 = st.columns(2)
with col1:
    invoices = st.file_uploader("Invoice PDFs", type=["pdf"], accept_multiple_files=True)
with col2:
    eads = st.file_uploader("EAD PDFs", type=["pdf"], accept_multiple_files=True)

st.info(
    "Single and bulk mode pairs files by **sorted filename order** (Invoice #1 with EAD #1, etc.). "
    "So please name them consistently (e.g., 001_invoice.pdf / 001_ead.pdf)."
)

run_btn = st.button("🚀 Generate Packing Lists", type="primary", disabled=not invoices or not eads)

if "results" not in st.session_state:
    st.session_state.results = []

if run_btn:
    st.session_state.results = []
    inv_sorted = sorted(invoices, key=lambda f: f.name.lower())
    ead_sorted = sorted(eads, key=lambda f: f.name.lower())

    n = min(len(inv_sorted), len(ead_sorted))
    if len(inv_sorted) != len(ead_sorted):
        st.warning(f"Counts differ: {len(inv_sorted)} invoices vs {len(ead_sorted)} EADs. Running first {n} pairs only.")

    progress = st.progress(0)
    for i in range(n):
        inv_file = inv_sorted[i]
        ead_file = ead_sorted[i]
        job_id = f"JOB-{i+1:03d}"

        with st.spinner(f"{job_id}: Processing {inv_file.name} + {ead_file.name} ..."):
            try:
                df, issues, excel_bytes, issues_bytes = run_one_job(
                    invoice_pdf_bytes=inv_file.getvalue(),
                    ead_pdf_bytes=ead_file.getvalue(),
                    model=model,
                )
                status = status_from_issues(issues)
                st.session_state.results.append(
                    JobResult(
                        job_id=job_id,
                        invoice_name=inv_file.name,
                        ead_name=ead_file.name,
                        status=status,
                        issues_count=len(issues),
                        excel_bytes=excel_bytes,
                        issues_bytes=issues_bytes,
                    )
                )
            except Exception as e:
                st.session_state.results.append(
                    JobResult(
                        job_id=job_id,
                        invoice_name=inv_file.name,
                        ead_name=ead_file.name,
                        status="FAIL",
                        issues_count=1,
                        excel_bytes=None,
                        issues_bytes=str(e).encode("utf-8"),
                    )
                )

        progress.progress((i + 1) / n)

st.subheader("2) Results")

if st.session_state.results:
    # Summary table
    summary_rows = []
    for r in st.session_state.results:
        emoji = {"OK": "✅", "WARN": "⚠️", "FAIL": "❌"}.get(r.status, "❓")
        summary_rows.append({
            "Job": r.job_id,
            "Status": f"{emoji} {r.status}",
            "Issues": r.issues_count,
            "Invoice": r.invoice_name,
            "EAD": r.ead_name,
        })
    summary_df = pd.DataFrame(summary_rows)
    st.dataframe(summary_df, use_container_width=True)

    st.subheader("3) Download")
    for r in st.session_state.results:
        with st.expander(f"{r.job_id} — {r.invoice_name} / {r.ead_name}"):
            if r.excel_bytes:
                st.download_button(
                    "⬇️ Download packing_list.xlsx",
                    data=r.excel_bytes,
                    file_name=f"{r.job_id}_packing_list.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            if r.issues_bytes:
                st.download_button(
                    "⬇️ Download issues.json",
                    data=r.issues_bytes,
                    file_name=f"{r.job_id}_issues.json",
                    mime="application/json",
                )
else:
    st.caption("No runs yet. Upload files and click **Generate Packing Lists**.")
