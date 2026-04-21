import polars as pl
from datetime import datetime, timedelta
from rich.console import Console
from src.config import GATEWAY_FEE_PERCENTAGE, DATE_TOLERANCE_DAYS, AMOUNT_TOLERANCE

console = Console()

class RuleEngine:
    
    def __init__(
        self,
        fee_percentage: float = GATEWAY_FEE_PERCENTAGE,
        date_tolerance: int = DATE_TOLERANCE_DAYS,
        amount_tolerance: float = AMOUNT_TOLERANCE
    ):
        self.fee_percentage = fee_percentage
        self.date_tolerance = date_tolerance
        self.amount_tolerance = amount_tolerance
        self.rule_matches = 0
    
    def apply_rules(
        self,
        unmatched_ledger: pl.DataFrame,
        unmatched_gateway: pl.DataFrame,
        bank_df: pl.DataFrame
    ) -> dict:
        console.print("\n[bold cyan]📏 Step 3: Rule-Based Matching[/bold cyan]")
        
        if len(unmatched_ledger) == 0 or len(unmatched_gateway) == 0:
            console.print("   ⏭️  No records for rule-based matching")
            return {
                "rule_matched": pl.DataFrame(),
                "still_unmatched_ledger": unmatched_ledger,
                "still_unmatched_gateway": unmatched_gateway,
                "rule_match_count": 0,
            }
        
        rule_results = self._fee_adjusted_matching(
            unmatched_ledger, unmatched_gateway
        )
        
        self.rule_matches = rule_results["match_count"]
        
        console.print(f"   ✅ Rule-based matches found: [green]{self.rule_matches:,}[/green]")
        
        return {
            "rule_matched": rule_results.get("matched", pl.DataFrame()),
            "still_unmatched_ledger": rule_results["remaining_ledger"],
            "still_unmatched_gateway": rule_results["remaining_gateway"],
            "rule_match_count": self.rule_matches,
        }
    
    def _fee_adjusted_matching(
        self,
        unmatched_ledger: pl.DataFrame,
        unmatched_gateway: pl.DataFrame
    ) -> dict:
        matched_pairs = []
        matched_ledger_indices = set()
        matched_gateway_indices = set()
        
        for l_idx in range(len(unmatched_ledger)):
            l_row = unmatched_ledger.row(l_idx, named=True)
            l_amount = l_row.get("ledger_amount")
            
            if l_amount is None:
                continue
            
            expected_net = round(l_amount * (1 - self.fee_percentage / 100), 2)
            
            for g_idx in range(len(unmatched_gateway)):
                if g_idx in matched_gateway_indices:
                    continue
                
                g_row = unmatched_gateway.row(g_idx, named=True)
                g_net = g_row.get("gateway_net_amount")
                
                if g_net is None:
                    continue
                
                if abs(expected_net - g_net) <= self.amount_tolerance:
                    matched_pairs.append({
                        "order_id": l_row.get("order_id"),
                        "gateway_ref": l_row.get("gateway_ref", g_row.get("gateway_ref")),
                        "ledger_amount": l_amount,
                        "gateway_amount": g_row.get("gateway_amount"),
                        "gateway_fee": g_row.get("gateway_fee"),
                        "gateway_net_amount": g_net,
                        "ledger_status": l_row.get("ledger_status"),
                        "gateway_status": g_row.get("gateway_status"),
                        "gateway_order_id": g_row.get("gateway_order_id"),
                        "customer_id": l_row.get("customer_id"),
                        "payment_method": l_row.get("payment_method"),
                        "ledger_date": str(l_row.get("ledger_date", "")),
                        "settlement_date": g_row.get("settlement_date"),
                        "match_type": "rule_based",
                        "confidence_score": 75.0,
                    })
                    
                    matched_ledger_indices.add(l_idx)
                    matched_gateway_indices.add(g_idx)
                    break
        
        matched_df = pl.DataFrame(matched_pairs) if matched_pairs else pl.DataFrame()
        
        remaining_ledger = unmatched_ledger.with_row_index("_idx").filter(
            ~pl.col("_idx").is_in(list(matched_ledger_indices))
        ).drop("_idx")
        
        remaining_gateway = unmatched_gateway.with_row_index("_idx").filter(
            ~pl.col("_idx").is_in(list(matched_gateway_indices))
        ).drop("_idx")
        
        return {
            "matched": matched_df,
            "remaining_ledger": remaining_ledger,
            "remaining_gateway": remaining_gateway,
            "match_count": len(matched_pairs),
        }