import polars as pl
from rapidfuzz import fuzz, process
from rich.console import Console
from src.config import FUZZY_MATCH_THRESHOLD, AMOUNT_TOLERANCE

console = Console()

class TransactionMatcher:
    
    def __init__(
        self, 
        fuzzy_threshold: int = FUZZY_MATCH_THRESHOLD,
        amount_tolerance: float = AMOUNT_TOLERANCE
    ):
        self.fuzzy_threshold = fuzzy_threshold
        self.amount_tolerance = amount_tolerance
        
        self.stats = {
            "exact_matches": 0,
            "fuzzy_matches": 0,
            "total_ledger": 0,
            "total_gateway": 0,
        }
    
    def exact_match_ledger_gateway(
        self, 
        ledger_df: pl.DataFrame, 
        gateway_df: pl.DataFrame
    ) -> dict:
        console.print("\n[bold cyan]🔗 Step 1: Exact Matching (Ledger ↔ Gateway)[/bold cyan]")
        
        self.stats["total_ledger"] = len(ledger_df)
        self.stats["total_gateway"] = len(gateway_df)
        
        ledger_for_join = ledger_df.select([
            pl.col("order_id"),
            pl.col("gateway_ref"),
            pl.col("amount").alias("ledger_amount"),
            pl.col("status").alias("ledger_status"),
            pl.col("transaction_date").alias("ledger_date"),
            pl.col("customer_id"),
            pl.col("payment_method"),
        ])
        
        gateway_for_join = gateway_df.select([
            pl.col("gateway_ref"),
            pl.col("order_id").alias("gateway_order_id"),
            pl.col("amount").alias("gateway_amount"),
            pl.col("fee").alias("gateway_fee"),
            pl.col("net_amount").alias("gateway_net_amount"),
            pl.col("status").alias("gateway_status"),
            pl.col("settlement_date"),
        ])
        
        joined = ledger_for_join.join(
            gateway_for_join,
            on="gateway_ref",
            how="full",
            coalesce=True,
        )
        
        matched = joined.filter(
            pl.col("order_id").is_not_null() & 
            pl.col("gateway_order_id").is_not_null()
        )
        
        unmatched_ledger = joined.filter(
            pl.col("order_id").is_not_null() & 
            pl.col("gateway_order_id").is_null()
        )
        
        unmatched_gateway = joined.filter(
            pl.col("order_id").is_null() & 
            pl.col("gateway_order_id").is_not_null()
        )
        
        matched = matched.with_columns(
            pl.lit("exact").alias("match_type"),
            pl.lit(100.0).alias("confidence_score"),
        )
        
        self.stats["exact_matches"] = len(matched)
        
        console.print(f"   ✅ Exact matches found: [green]{len(matched):,}[/green]")
        console.print(f"   ❓ Unmatched ledger records: [yellow]{len(unmatched_ledger):,}[/yellow]")
        console.print(f"   ❓ Unmatched gateway records: [yellow]{len(unmatched_gateway):,}[/yellow]")
        
        return {
            "matched": matched,
            "unmatched_ledger": unmatched_ledger,
            "unmatched_gateway": unmatched_gateway,
        }
    
    def fuzzy_match_remaining(
        self,
        unmatched_ledger: pl.DataFrame,
        unmatched_gateway: pl.DataFrame
    ) -> dict:
        console.print("\n[bold cyan]🔍 Step 2: Fuzzy Matching (Remaining Records)[/bold cyan]")
        
        if len(unmatched_ledger) == 0 or len(unmatched_gateway) == 0:
            console.print("   ⏭️  No records to fuzzy match")
            return {
                "fuzzy_matched": pl.DataFrame(),
                "still_unmatched_ledger": unmatched_ledger,
                "still_unmatched_gateway": unmatched_gateway,
            }
        
        ledger_refs = unmatched_ledger.select("gateway_ref").to_series().to_list()
        gateway_refs = unmatched_gateway.select("gateway_ref").to_series().to_list()
        
        valid_ledger_indices = [i for i, ref in enumerate(ledger_refs) if ref is not None]
        valid_gateway_refs = [ref for ref in gateway_refs if ref is not None]
        
        if not valid_ledger_indices or not valid_gateway_refs:
            console.print("   ⏭️  No valid references to compare")
            return {
                "fuzzy_matched": pl.DataFrame(),
                "still_unmatched_ledger": unmatched_ledger,
                "still_unmatched_gateway": unmatched_gateway,
            }
        
        fuzzy_matches = []
        matched_gateway_indices = set()
        matched_ledger_indices = set()
        
        for ledger_idx in valid_ledger_indices:
            ledger_ref = ledger_refs[ledger_idx]
            
            available_refs = [
                (i, ref) for i, ref in enumerate(gateway_refs) 
                if ref is not None and i not in matched_gateway_indices
            ]
            
            if not available_refs:
                break
            
            available_indices, available_ref_strings = zip(*available_refs)
            
            result = process.extractOne(
                query=ledger_ref,
                choices=available_ref_strings,
                scorer=fuzz.ratio,
                score_cutoff=self.fuzzy_threshold,
            )
            
            if result is not None:
                best_match, score, choice_idx = result
                gateway_idx = available_indices[choice_idx]
                
                matched_ledger_indices.add(ledger_idx)
                matched_gateway_indices.add(gateway_idx)
                
                fuzzy_matches.append({
                    "ledger_idx": ledger_idx,
                    "gateway_idx": gateway_idx,
                    "confidence_score": score,
                })
        
        if fuzzy_matches:
            fuzzy_matched_records = []
            
            for match in fuzzy_matches:
                l_row = unmatched_ledger.row(match["ledger_idx"], named=True)
                g_row = unmatched_gateway.row(match["gateway_idx"], named=True)
                
                fuzzy_matched_records.append({
                    "order_id": l_row.get("order_id"),
                    "gateway_ref": g_row.get("gateway_ref", l_row.get("gateway_ref")),
                    "ledger_amount": l_row.get("ledger_amount"),
                    "gateway_amount": g_row.get("gateway_amount"),
                    "gateway_fee": g_row.get("gateway_fee"),
                    "gateway_net_amount": g_row.get("gateway_net_amount"),
                    "ledger_status": l_row.get("ledger_status"),
                    "gateway_status": g_row.get("gateway_status"),
                    "gateway_order_id": g_row.get("gateway_order_id"),
                    "customer_id": l_row.get("customer_id"),
                    "payment_method": l_row.get("payment_method"),
                    "ledger_date": str(l_row.get("ledger_date", "")),
                    "settlement_date": g_row.get("settlement_date"),
                    "match_type": "fuzzy",
                    "confidence_score": match["confidence_score"],
                })
            
            fuzzy_matched_df = pl.DataFrame(fuzzy_matched_records)
        else:
            fuzzy_matched_df = pl.DataFrame()
        
        still_unmatched_ledger = unmatched_ledger.with_row_index("_idx").filter(
            ~pl.col("_idx").is_in(list(matched_ledger_indices))
        ).drop("_idx")
        
        still_unmatched_gateway = unmatched_gateway.with_row_index("_idx").filter(
            ~pl.col("_idx").is_in(list(matched_gateway_indices))
        ).drop("_idx")
        
        self.stats["fuzzy_matches"] = len(fuzzy_matched_df)
        
        console.print(f"   ✅ Fuzzy matches found: [green]{len(fuzzy_matched_df):,}[/green]")
        console.print(f"   ❓ Still unmatched (ledger): [yellow]{len(still_unmatched_ledger):,}[/yellow]")
        console.print(f"   ❓ Still unmatched (gateway): [yellow]{len(still_unmatched_gateway):,}[/yellow]")
        
        return {
            "fuzzy_matched": fuzzy_matched_df,
            "still_unmatched_ledger": still_unmatched_ledger,
            "still_unmatched_gateway": still_unmatched_gateway,
        }
    
    def match_with_bank(
        self,
        matched_df: pl.DataFrame,
        bank_df: pl.DataFrame
    ) -> pl.DataFrame:
        console.print("\n[bold cyan]🏦 Step 3: Bank Statement Matching[/bold cyan]")
        
        if len(matched_df) == 0:
            console.print("   ⏭️  No matched records to match with bank")
            return matched_df
        
        bank_for_join = bank_df.select([
            pl.col("reference"),
            pl.col("utr_number"),
            pl.col("credit_amount").alias("bank_credit"),
            pl.col("transaction_date").alias("bank_date"),
        ])
        
        enriched = matched_df.join(
            bank_for_join,
            left_on="gateway_ref",
            right_on="reference",
            how="left",
        )
        
        bank_matched = enriched.filter(pl.col("utr_number").is_not_null()).height
        
        console.print(f"   ✅ Matched with bank: [green]{bank_matched:,}[/green]")
        console.print(
            f"   ⚠️  No bank entry: [yellow]"
            f"{len(enriched) - bank_matched:,}[/yellow] "
            f"(may be pending settlement)"
        )
        
        return enriched
    
    def match_all(
        self,
        ledger_df: pl.DataFrame,
        gateway_df: pl.DataFrame,
        bank_df: pl.DataFrame
    ) -> dict:
        console.print("\n" + "=" * 60)
        console.print("[bold green]⚙️  STARTING RECONCILIATION MATCHING PIPELINE[/bold green]")
        console.print("=" * 60)
        
        exact_results = self.exact_match_ledger_gateway(ledger_df, gateway_df)
        
        fuzzy_results = self.fuzzy_match_remaining(
            exact_results["unmatched_ledger"],
            exact_results["unmatched_gateway"]
        )
        
        all_matched_parts = []
        if len(exact_results["matched"]) > 0:
            all_matched_parts.append(exact_results["matched"])
        if len(fuzzy_results["fuzzy_matched"]) > 0:
            all_matched_parts.append(fuzzy_results["fuzzy_matched"])
        
        if all_matched_parts:
            all_matched = pl.concat(all_matched_parts, how="diagonal_relaxed")
        else:
            all_matched = pl.DataFrame()
        
        if len(all_matched) > 0:
            all_matched = self.match_with_bank(all_matched, bank_df)
        
        total_processed = self.stats["total_ledger"] + self.stats["total_gateway"]
        total_matched = self.stats["exact_matches"] + self.stats["fuzzy_matches"]
        
        console.print("\n" + "=" * 60)
        console.print("[bold green]📊 MATCHING SUMMARY[/bold green]")
        console.print("=" * 60)
        console.print(f"   Total ledger records:    {self.stats['total_ledger']:,}")
        console.print(f"   Total gateway records:   {self.stats['total_gateway']:,}")
        console.print(f"   Exact matches:           [green]{self.stats['exact_matches']:,}[/green]")
        console.print(f"   Fuzzy matches:           [green]{self.stats['fuzzy_matches']:,}[/green]")
        console.print(f"   Total matched:           [bold green]{total_matched:,}[/bold green]")
        
        final_unmatched_ledger = fuzzy_results["still_unmatched_ledger"]
        final_unmatched_gateway = fuzzy_results["still_unmatched_gateway"]
        
        console.print(f"   Unmatched (ledger):      [red]{len(final_unmatched_ledger):,}[/red]")
        console.print(f"   Unmatched (gateway):     [red]{len(final_unmatched_gateway):,}[/red]")
        console.print("=" * 60)
        
        return {
            "matched": all_matched,
            "unmatched_ledger": final_unmatched_ledger,
            "unmatched_gateway": final_unmatched_gateway,
            "stats": dict(self.stats),
        }