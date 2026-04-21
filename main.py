"""
============================================================
main.py — Payment Reconciliation Engine Entry Point

Orchestrates the end-to-end reconciliation pipeline:
Loading -> Validation -> Matching -> Classification -> Persistence -> Reporting
============================================================
"""

import sys
import time
from datetime import datetime
import polars as pl
from rich.console import Console
from rich.panel import Panel

# Core Engine Modules
from src.ingestion.loader import DataLoader
from src.ingestion.validator import DataValidator
from src.engine.matcher import TransactionMatcher
from src.engine.rules import RuleEngine
from src.engine.classifier import DiscrepancyClassifier
from src.persistence.db_manager import DatabaseManager
from src.persistence.repository import ReconciliationRepository
from src.reporting.report_generator import ReportGenerator

console = Console()

def run_reconciliation():
    """
    Executes the full automated reconciliation sequence.
    """
    console.print(Panel.fit(
        "[bold cyan]🏦 Payment Reconciliation Engine v1.0[/bold cyan]\n"
        f"[dim]Run started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}[/dim]",
        border_style="cyan",
    ))
    
    start_time = time.time()
    
    # --- STEP 1: LOAD DATA ---
    console.print(Panel("[bold]📥 STEP 1: Loading Data[/bold]", style="blue"))
    loader = DataLoader()
    try:
        ledger_df, gateway_df, bank_df = loader.load_all_sources()
    except FileNotFoundError as e:
        console.print(f"\n[bold red]❌ {e}[/bold red]\n[yellow]💡 Run sample data generator first.[/yellow]")
        sys.exit(1)
    
    # --- STEP 2: VALIDATE DATA ---
    console.print(Panel("[bold]🔍 STEP 2: Validating Data[/bold]", style="yellow"))
    validator = DataValidator()
    all_valid, _ = validator.validate_all(ledger_df, gateway_df, bank_df)
    if not all_valid:
        console.print("\n[bold yellow]⚠️ Validation warnings detected. Proceeding...[/bold yellow]")
    
    # --- STEP 3: MATCHING PIPELINE ---
    console.print(Panel("[bold]⚙️ STEP 3: Matching Transactions[/bold]", style="cyan"))
    
    # Phase A: Algorithmic (Exact + Fuzzy)
    matcher = TransactionMatcher()
    match_results = matcher.match_all(ledger_df, gateway_df, bank_df)
    
    # Phase B: Rule-Based Heuristics
    rule_engine = RuleEngine()
    rule_results = rule_engine.apply_rules(
        match_results["unmatched_ledger"],
        match_results["unmatched_gateway"],
        bank_df,
    )
    
    # Combine results
    all_matched_parts = [df for df in [match_results["matched"], rule_results["rule_matched"]] if len(df) > 0]
    all_matched = pl.concat(all_matched_parts, how="diagonal_relaxed") if all_matched_parts else pl.DataFrame()
    
    stats = match_results["stats"]
    stats["rule_based_matches"] = rule_results["rule_match_count"]
    
    # --- STEP 4: CLASSIFY DISCREPANCIES ---
    console.print(Panel("[bold]🔎 STEP 4: Classifying Discrepancies[/bold]", style="magenta"))
    classifier = DiscrepancyClassifier()
    discrepancies = classifier.classify_all(
        matched_df=all_matched,
        unmatched_ledger=rule_results["still_unmatched_ledger"],
        unmatched_gateway=rule_results["still_unmatched_gateway"],
        original_ledger=ledger_df,
    )
    
    # --- STEP 5: PERSISTENCE ---
    console.print(Panel("[bold]💾 STEP 5: Saving to Database[/bold]", style="blue"))
    db = DatabaseManager()
    db.create_tables()
    
    repo = ReconciliationRepository(db)
    run_id = repo.save_reconciliation_run(
        matched_df=all_matched,
        discrepancies_df=discrepancies,
        stats=stats,
        ledger_count=len(ledger_df),
        gateway_count=len(gateway_df),
        bank_count=len(bank_df),
    )
    
    # --- STEP 6: REPORTING ---
    console.print(Panel("[bold]📄 STEP 6: Generating Report[/bold]", style="green"))
    report_path = ReportGenerator().generate(
        matched_df=all_matched,
        discrepancies_df=discrepancies,
        stats=stats,
        ledger_count=len(ledger_df),
        gateway_count=len(gateway_df),
        bank_count=len(bank_df),
    )
    
    # --- FINAL SUMMARY ---
    elapsed = time.time() - start_time
    total_matched = len(all_matched)
    total_unique = max(len(ledger_df), len(gateway_df))
    match_rate = (total_matched / total_unique * 100) if total_unique > 0 else 0
    
    console.print("\n" + "=" * 60)
    console.print(Panel.fit(
        f"[bold green]✅ RECONCILIATION COMPLETE[/bold green]\n\n"
        f"⏱️ Time: {elapsed:.2f}s | 🔗 Matched: {total_matched:,} | 📊 Rate: {match_rate:.1f}%\n"
        f"💾 DB Run ID: #{run_id} | 📄 Report: {report_path}",
        border_style="green",
        title="🏦 Summary",
    ))

if __name__ == "__main__":
    run_reconciliation()