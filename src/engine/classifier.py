import polars as pl

from rich.console import Console
from rich.table import Table

console = Console()


class DiscrepancyClassifier:
    
    def classify_all(
        self,
        matched_df: pl.DataFrame,
        unmatched_ledger: pl.DataFrame,
        unmatched_gateway: pl.DataFrame,
        original_ledger: pl.DataFrame
    ) -> pl.DataFrame:
        console.print("\n" + "=" * 60)
        console.print("[bold magenta]🔎 CLASSIFYING DISCREPANCIES[/bold magenta]")
        console.print("=" * 60)
        
        all_discrepancies = []
        
        missing_gateway = self._classify_missing_in_gateway(unmatched_ledger)
        all_discrepancies.append(missing_gateway)
        
        missing_ledger = self._classify_missing_in_ledger(unmatched_gateway)
        all_discrepancies.append(missing_ledger)
        
        amount_mismatches = self._classify_amount_mismatches(matched_df)
        all_discrepancies.append(amount_mismatches)
        
        duplicates = self._classify_duplicates(original_ledger)
        all_discrepancies.append(duplicates)
        
        non_empty = [df for df in all_discrepancies if len(df) > 0]
        
        if non_empty:
            combined = pl.concat(non_empty, how="diagonal_relaxed")
        else:
            combined = pl.DataFrame({
                "discrepancy_type": [],
                "order_id": [],
                "gateway_ref": [],
                "expected_amount": [],
                "actual_amount": [],
                "difference": [],
                "source": [],
                "severity": [],
                "details": [],
            })
        
        self._print_summary(combined)
        
        return combined
    
    def _classify_missing_in_gateway(self, unmatched_ledger: pl.DataFrame) -> pl.DataFrame:
        if len(unmatched_ledger) == 0:
            return pl.DataFrame()
        
        console.print(f"\n   🔴 Missing in Gateway: [red]{len(unmatched_ledger):,}[/red] records")
        
        records = []
        for i in range(len(unmatched_ledger)):
            row = unmatched_ledger.row(i, named=True)
            records.append({
                "discrepancy_type": "MISSING_IN_GATEWAY",
                "order_id": row.get("order_id"),
                "gateway_ref": row.get("gateway_ref"),
                "expected_amount": row.get("ledger_amount"),
                "actual_amount": None,
                "difference": row.get("ledger_amount"),
                "source": "ledger",
                "severity": "HIGH",
                "details": (
                    f"Transaction {row.get('order_id')} exists in ledger "
                    f"(₹{row.get('ledger_amount', 0):,.2f}) but has no "
                    f"matching gateway record. Investigate webhook/gateway logs."
                ),
            })
        
        return pl.DataFrame(records)
    
    def _classify_missing_in_ledger(self, unmatched_gateway: pl.DataFrame) -> pl.DataFrame:
        if len(unmatched_gateway) == 0:
            return pl.DataFrame()
        
        console.print(f"   🔴 Missing in Ledger: [red]{len(unmatched_gateway):,}[/red] records")
        
        records = []
        for i in range(len(unmatched_gateway)):
            row = unmatched_gateway.row(i, named=True)
            records.append({
                "discrepancy_type": "MISSING_IN_LEDGER",
                "order_id": row.get("gateway_order_id"),
                "gateway_ref": row.get("gateway_ref"),
                "expected_amount": None,
                "actual_amount": row.get("gateway_amount"),
                "difference": row.get("gateway_amount"),
                "source": "gateway",
                "severity": "HIGH",
                "details": (
                    f"Gateway ref {row.get('gateway_ref')} exists in gateway "
                    f"(₹{row.get('gateway_amount', 0):,.2f}) but has no "
                    f"matching ledger record. Check app logs for failures."
                ),
            })
        
        return pl.DataFrame(records)
    
    def _classify_amount_mismatches(self, matched_df: pl.DataFrame) -> pl.DataFrame:
        if len(matched_df) == 0:
            return pl.DataFrame()
        
        if "ledger_amount" not in matched_df.columns or "gateway_amount" not in matched_df.columns:
            return pl.DataFrame()
        
        mismatches = matched_df.filter(
            (pl.col("ledger_amount") - pl.col("gateway_amount")).abs() > 1.0
        )
        
        if len(mismatches) == 0:
            return pl.DataFrame()
        
        console.print(f"   🟡 Amount Mismatches: [yellow]{len(mismatches):,}[/yellow] records")
        
        records = []
        for i in range(len(mismatches)):
            row = mismatches.row(i, named=True)
            ledger_amt = row.get("ledger_amount", 0)
            gateway_amt = row.get("gateway_amount", 0)
            diff = round(abs(ledger_amt - gateway_amt), 2)
            
            records.append({
                "discrepancy_type": "AMOUNT_MISMATCH",
                "order_id": row.get("order_id"),
                "gateway_ref": row.get("gateway_ref"),
                "expected_amount": gateway_amt,
                "actual_amount": ledger_amt,
                "difference": diff,
                "source": "both",
                "severity": "MEDIUM" if diff < 500 else "HIGH",
                "details": (
                    f"Amount mismatch for {row.get('order_id')}: "
                    f"Ledger ₹{ledger_amt:,.2f} vs Gateway ₹{gateway_amt:,.2f} "
                    f"(difference: ₹{diff:,.2f})"
                ),
            })
        
        return pl.DataFrame(records)
    
    def _classify_duplicates(self, original_ledger: pl.DataFrame) -> pl.DataFrame:
        if len(original_ledger) == 0 or "order_id" not in original_ledger.columns:
            return pl.DataFrame()
        
        duplicate_ids = (
            original_ledger
            .group_by("order_id")
            .agg(pl.count().alias("count"))
            .filter(pl.col("count") > 1)
        )
        
        if len(duplicate_ids) == 0:
            return pl.DataFrame()
        
        console.print(f"   🟡 Duplicates Found: [yellow]{len(duplicate_ids):,}[/yellow] order IDs")
        
        records = []
        for i in range(len(duplicate_ids)):
            row = duplicate_ids.row(i, named=True)
            order_id = row["order_id"]
            count = row["count"]
            
            records.append({
                "discrepancy_type": "DUPLICATE",
                "order_id": order_id,
                "gateway_ref": None,
                "expected_amount": None,
                "actual_amount": None,
                "difference": None,
                "source": "ledger",
                "severity": "MEDIUM",
                "details": (
                    f"Order {order_id} appears {count} times in the ledger. "
                    f"Possible double-payment or retry bug."
                ),
            })
        
        return pl.DataFrame(records)
    
    def _print_summary(self, discrepancies: pl.DataFrame):
        console.print("\n" + "=" * 60)
        console.print("[bold magenta]📊 DISCREPANCY SUMMARY[/bold magenta]")
        console.print("=" * 60)
        
        if len(discrepancies) == 0:
            console.print("   [bold green]🎉 No discrepancies found! Perfect reconciliation![/bold green]")
            return
        
        table = Table(title="Discrepancy Breakdown")
        table.add_column("Type", style="cyan", no_wrap=True)
        table.add_column("Count", justify="right", style="magenta")
        table.add_column("Severity", style="bold")
        
        summary = (
            discrepancies
            .group_by("discrepancy_type")
            .agg([
                pl.count().alias("count"),
                pl.col("severity").first(),
            ])
            .sort("count", descending=True)
        )
        
        for i in range(len(summary)):
            row = summary.row(i, named=True)
            severity = row["severity"]
            if severity == "HIGH":
                severity_str = "[red]HIGH[/red]"
            elif severity == "MEDIUM":
                severity_str = "[yellow]MEDIUM[/yellow]"
            else:
                severity_str = "[green]LOW[/green]"
            
            table.add_row(
                row["discrepancy_type"],
                f"{row['count']:,}",
                severity_str
            )
        
        console.print(table)
        console.print(f"\n   Total discrepancies: [bold red]{len(discrepancies):,}[/bold red]")