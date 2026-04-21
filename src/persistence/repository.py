import polars as pl
from datetime import datetime
from rich.console import Console

from src.models.database import ReconciliationRun, MatchedTransaction, Discrepancy
from src.persistence.db_manager import DatabaseManager

console = Console()

class ReconciliationRepository:
    """
    Handles all database operations (CRUD) for reconciliation data.
    
    Acts as a mediator between the domain/engine layer and the database,
    abstracting away SQLAlchemy queries into clean Python methods.
    """
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    def save_reconciliation_run(
        self,
        matched_df: pl.DataFrame,
        discrepancies_df: pl.DataFrame,
        stats: dict,
        ledger_count: int,
        gateway_count: int,
        bank_count: int,
    ) -> int:
        console.print("\n[bold blue]💾 Saving results to database...[/bold blue]")
        
        total_matched = len(matched_df)
        total_discrepancies = len(discrepancies_df)
        
        total_unique = max(ledger_count, gateway_count)
        match_rate = (total_matched / total_unique * 100) if total_unique > 0 else 0.0
        
        disc_counts = self._count_discrepancy_types(discrepancies_df)
        
        with self.db.get_session_context() as session:
            # --- Create Summary Record ---
            run = ReconciliationRun(
                run_timestamp=datetime.utcnow(),
                total_ledger_records=ledger_count,
                total_gateway_records=gateway_count,
                total_bank_records=bank_count,
                total_matched=total_matched,
                total_discrepancies=total_discrepancies,
                match_rate=round(match_rate, 2),
                exact_matches=stats.get("exact_matches", 0),
                fuzzy_matches=stats.get("fuzzy_matches", 0),
                rule_based_matches=stats.get("rule_based_matches", 0),
                missing_in_gateway=disc_counts.get("MISSING_IN_GATEWAY", 0),
                missing_in_ledger=disc_counts.get("MISSING_IN_LEDGER", 0),
                amount_mismatches=disc_counts.get("AMOUNT_MISMATCH", 0),
                duplicates_found=disc_counts.get("DUPLICATE", 0),
            )
            
            session.add(run)
            session.flush()  # Populates run.id for child records
            
            run_id = run.id
            
            # --- Bulk Save Children ---
            if len(matched_df) > 0:
                self._save_matched_transactions(session, run_id, matched_df)
            
            if len(discrepancies_df) > 0:
                self._save_discrepancies(session, run_id, discrepancies_df)
            
        console.print(f"   ✅ Run #{run_id} saved: {total_matched:,} matches, "
                      f"{total_discrepancies:,} discrepancies")
        
        return run_id
    
    def _save_matched_transactions(self, session, run_id: int, matched_df: pl.DataFrame):
        records = []
        for i in range(len(matched_df)):
            row = matched_df.row(i, named=True)
            records.append(MatchedTransaction(
                run_id=run_id,
                order_id=str(row.get("order_id", "")),
                gateway_ref=str(row.get("gateway_ref", "")) if row.get("gateway_ref") else None,
                utr_number=str(row.get("utr_number", "")) if row.get("utr_number") else None,
                ledger_amount=float(row.get("ledger_amount", 0)),
                gateway_amount=float(row.get("gateway_amount", 0)) if row.get("gateway_amount") else None,
                bank_credit=float(row.get("bank_credit", 0)) if row.get("bank_credit") else None,
                gateway_fee=float(row.get("gateway_fee", 0)) if row.get("gateway_fee") else 0.0,
                match_type=str(row.get("match_type", "exact")),
                confidence_score=float(row.get("confidence_score", 100.0)),
                transaction_date=str(row.get("ledger_date", "")),
                settlement_date=str(row.get("settlement_date", "")) if row.get("settlement_date") else None,
            ))
        session.add_all(records)
        console.print(f"   📝 Saved {len(records):,} matched transactions")
    
    def _save_discrepancies(self, session, run_id: int, discrepancies_df: pl.DataFrame):
        records = []
        for i in range(len(discrepancies_df)):
            row = discrepancies_df.row(i, named=True)
            records.append(Discrepancy(
                run_id=run_id,
                discrepancy_type=str(row.get("discrepancy_type", "")),
                severity=str(row.get("severity", "MEDIUM")),
                order_id=str(row.get("order_id", "")) if row.get("order_id") else None,
                gateway_ref=str(row.get("gateway_ref", "")) if row.get("gateway_ref") else None,
                expected_amount=float(row.get("expected_amount")) if row.get("expected_amount") is not None else None,
                actual_amount=float(row.get("actual_amount")) if row.get("actual_amount") is not None else None,
                difference=float(row.get("difference")) if row.get("difference") is not None else None,
                source=str(row.get("source", "")),
                details=str(row.get("details", "")),
            ))
        session.add_all(records)
        console.print(f"   📝 Saved {len(records):,} discrepancies")
    
    def _count_discrepancy_types(self, discrepancies_df: pl.DataFrame) -> dict:
        if len(discrepancies_df) == 0 or "discrepancy_type" not in discrepancies_df.columns:
            return {}
        
        counts = (
            discrepancies_df
            .group_by("discrepancy_type")
            .agg(pl.count().alias("count"))
        )
        
        return {
            row["discrepancy_type"]: row["count"]
            for row in counts.iter_rows(named=True)
        }
    
    def get_latest_run(self) -> dict | None:
        session = self.db.get_session()
        try:
            run = (
                session.query(ReconciliationRun)
                .order_by(ReconciliationRun.run_timestamp.desc())
                .first()
            )
            if run is None: return None
            
            return {
                "id": run.id,
                "timestamp": run.run_timestamp,
                "total_matched": run.total_matched,
                "total_discrepancies": run.total_discrepancies,
                "match_rate": run.match_rate,
                "exact_matches": run.exact_matches,
                "fuzzy_matches": run.fuzzy_matches,
                "rule_based_matches": run.rule_based_matches,
                "total_ledger": run.total_ledger_records,
                "total_gateway": run.total_gateway_records,
                "total_bank": run.total_bank_records,
                "missing_in_gateway": run.missing_in_gateway,
                "missing_in_ledger": run.missing_in_ledger,
                "amount_mismatches": run.amount_mismatches,
                "duplicates_found": run.duplicates_found,
            }
        finally:
            session.close()

    def get_all_runs(self) -> list[dict]:
        session = self.db.get_session()
        try:
            runs = (
                session.query(ReconciliationRun)
                .order_by(ReconciliationRun.run_timestamp.desc())
                .all()
            )
            return [
                {
                    "id": r.id,
                    "timestamp": r.run_timestamp,
                    "match_rate": r.match_rate,
                    "total_matched": r.total_matched,
                    "total_discrepancies": r.total_discrepancies,
                }
                for r in runs
            ]
        finally:
            session.close()

    def get_discrepancies_for_run(self, run_id: int) -> list[dict]:
        session = self.db.get_session()
        try:
            discs = (
                session.query(Discrepancy)
                .filter(Discrepancy.run_id == run_id)
                .all()
            )
            return [
                {
                    "type": d.discrepancy_type,
                    "severity": d.severity,
                    "order_id": d.order_id,
                    "gateway_ref": d.gateway_ref,
                    "expected": d.expected_amount,
                    "actual": d.actual_amount,
                    "difference": d.difference,
                    "details": d.details,
                }
                for d in discs
            ]
        finally:
            session.close()