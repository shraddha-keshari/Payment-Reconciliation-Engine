import polars as pl
from datetime import datetime
from pathlib import Path

from jinja2 import Environment, FileSystemLoader
from rich.console import Console
from src.config import TEMPLATES_DIR, OUTPUT_DIR, REPORT_FILENAME

console = Console()

class ReportGenerator:
    """
    Generates HTML reconciliation reports using Jinja2 templates.
    
    This class handles the transformation of Polars DataFrames and 
    statistics into a human-readable HTML format for business review.
    """
    
    def __init__(self, templates_dir: Path = TEMPLATES_DIR, output_dir: Path = OUTPUT_DIR):
        self.output_dir = output_dir
        
        # Configure Jinja2 Environment
        self.env = Environment(
            loader=FileSystemLoader(str(templates_dir)),
            autoescape=False,
        )
    
    def generate(
        self,
        matched_df: pl.DataFrame,
        discrepancies_df: pl.DataFrame,
        stats: dict,
        ledger_count: int,
        gateway_count: int,
        bank_count: int,
    ) -> Path:
        console.print("\n[bold blue]📄 Generating HTML report...[/bold blue]")
        
        # --- Calculate summary statistics ---
        total_matched = len(matched_df)
        total_discrepancies = len(discrepancies_df)
        total_unique = max(ledger_count, gateway_count)
        match_rate = round((total_matched / total_unique * 100), 1) if total_unique > 0 else 0.0
        
        # Aggregate discrepancy types
        disc_counts = {}
        if len(discrepancies_df) > 0 and "discrepancy_type" in discrepancies_df.columns:
            agg_counts = discrepancies_df.group_by("discrepancy_type").agg(
                pl.count().alias("count")
            )
            for row in agg_counts.iter_rows(named=True):
                disc_counts[row["discrepancy_type"]] = row["count"]
        
        # --- Prepare discrepancy data for HTML table ---
        discrepancy_rows = []
        if len(discrepancies_df) > 0:
            for row in discrepancies_df.iter_rows(named=True):
                expected = row.get("expected_amount")
                actual = row.get("actual_amount")
                difference = row.get("difference")
                severity = row.get("severity", "MEDIUM")
                
                discrepancy_rows.append({
                    "type": row.get("discrepancy_type", ""),
                    "severity": severity,
                    "severity_class": severity.lower(),
                    "order_id_display": row.get("order_id") or "—",
                    "gateway_ref_display": row.get("gateway_ref") or "—",
                    "expected_display": f"₹{expected:,.2f}" if expected else "—",
                    "actual_display": f"₹{actual:,.2f}" if actual else "—",
                    "difference_display": f"₹{difference:,.2f}" if difference else "—",
                    "details": row.get("details", ""),
                })
        
        # --- UI Styling Logic ---
        if match_rate >= 90:
            match_rate_color = "linear-gradient(90deg, #4ade80, #22c55e)"
        elif match_rate >= 70:
            match_rate_color = "linear-gradient(90deg, #facc15, #eab308)"
        else:
            match_rate_color = "linear-gradient(90deg, #f87171, #ef4444)"
        
        # --- Build Context ---
        context = {
            "run_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_ledger": f"{ledger_count:,}",
            "total_gateway": f"{gateway_count:,}",
            "total_bank": f"{bank_count:,}",
            "total_matched": f"{total_matched:,}",
            "total_discrepancies": f"{total_discrepancies:,}",
            "match_rate": match_rate,
            "match_rate_color": match_rate_color,
            "exact_matches": f"{stats.get('exact_matches', 0):,}",
            "fuzzy_matches": f"{stats.get('fuzzy_matches', 0):,}",
            "rule_based_matches": f"{stats.get('rule_based_matches', 0):,}",
            "missing_in_gateway": disc_counts.get("MISSING_IN_GATEWAY", 0),
            "missing_in_ledger": disc_counts.get("MISSING_IN_LEDGER", 0),
            "amount_mismatches": disc_counts.get("AMOUNT_MISMATCH", 0),
            "duplicates_found": disc_counts.get("DUPLICATE", 0),
            "discrepancy_rows": discrepancy_rows,
        }
        
        # --- Render and Save ---
        template = self.env.get_template("recon_report.html")
        html_content = template.render(**context)
        
        self.output_dir.mkdir(parents=True, exist_ok=True)
        output_path = self.output_dir / REPORT_FILENAME
        
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        
        console.print(f"   ✅ Report saved: [green]{output_path}[/green]")
        return output_path