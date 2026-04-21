import polars as pl
from rich.console import Console
from rich.table import Table

console = Console()

class DataValidator:
    """
    Validates DataFrames from all 3 data sources using Polars operations.
    
    Checks for:
    1. Required columns
    2. Null values in critical fields
    3. Positive/Non-negative amounts
    4. Allowed categorical values (statuses)
    """
    
    def validate_ledger(self, df: pl.DataFrame) -> tuple[bool, list[str]]:
        console.print("[bold yellow]🔍 Validating internal ledger...[/bold yellow]")
        
        errors = []
        
        required_columns = [
            "order_id", "customer_id", "amount", "currency",
            "status", "payment_method", "transaction_date", "gateway_ref"
        ]
        errors.extend(self._check_required_columns(df, required_columns))
        
        if errors:
            return False, errors
        
        critical_columns = ["order_id", "amount", "status", "gateway_ref"]
        errors.extend(self._check_nulls(df, critical_columns))
        
        errors.extend(self._check_positive_values(df, "amount"))
        
        valid_statuses = {"SUCCESS", "FAILED", "REFUNDED"}
        errors.extend(self._check_allowed_values(df, "status", valid_statuses))
        
        self._print_validation_results("Internal Ledger", df, errors)
        
        return len(errors) == 0, errors
    
    def validate_gateway(self, df: pl.DataFrame) -> tuple[bool, list[str]]:
        console.print("[bold yellow]🔍 Validating gateway transactions...[/bold yellow]")
        
        errors = []
        
        required_columns = [
            "gateway_ref", "order_id", "amount", "fee", "net_amount",
            "currency", "status", "payment_method", "transaction_date",
            "settlement_date"
        ]
        errors.extend(self._check_required_columns(df, required_columns))
        
        if errors:
            return False, errors
        
        critical_columns = ["gateway_ref", "order_id", "amount", "fee"]
        errors.extend(self._check_nulls(df, critical_columns))
        
        errors.extend(self._check_positive_values(df, "amount"))
        errors.extend(self._check_non_negative_values(df, "fee"))
        
        valid_statuses = {"CAPTURED", "FAILED", "REFUNDED"}
        errors.extend(self._check_allowed_values(df, "status", valid_statuses))
        
        self._print_validation_results("Gateway Transactions", df, errors)
        
        return len(errors) == 0, errors
    
    def validate_bank_statement(self, df: pl.DataFrame) -> tuple[bool, list[str]]:
        console.print("[bold yellow]🔍 Validating bank statement...[/bold yellow]")
        
        errors = []
        
        required_columns = [
            "utr_number", "reference", "credit_amount",
            "debit_amount", "balance", "transaction_date", "description"
        ]
        errors.extend(self._check_required_columns(df, required_columns))
        
        if errors:
            return False, errors
        
        critical_columns = ["utr_number", "reference", "credit_amount"]
        errors.extend(self._check_nulls(df, critical_columns))
        
        errors.extend(self._check_non_negative_values(df, "credit_amount"))
        
        self._print_validation_results("Bank Statement", df, errors)
        
        return len(errors) == 0, errors
    
    def validate_all(
        self, 
        ledger_df: pl.DataFrame, 
        gateway_df: pl.DataFrame, 
        bank_df: pl.DataFrame
    ) -> tuple[bool, dict[str, list[str]]]:
        console.print("\n[bold green]🔍 Starting data validation...[/bold green]\n")
        
        all_errors = {}
        
        ledger_valid, ledger_errors = self.validate_ledger(ledger_df)
        all_errors["ledger"] = ledger_errors
        
        gateway_valid, gateway_errors = self.validate_gateway(gateway_df)
        all_errors["gateway"] = gateway_errors
        
        bank_valid, bank_errors = self.validate_bank_statement(bank_df)
        all_errors["bank"] = bank_errors
        
        all_valid = ledger_valid and gateway_valid and bank_valid
        
        if all_valid:
            console.print("\n[bold green]✅ All data sources passed validation![/bold green]\n")
        else:
            console.print("\n[bold red]❌ Validation failed for one or more sources.[/bold red]\n")
        
        return all_valid, all_errors
    
    def _check_required_columns(self, df: pl.DataFrame, required: list[str]) -> list[str]:
        missing = set(required) - set(df.columns)
        if missing:
            return [f"Missing required columns: {', '.join(sorted(missing))}"]
        return []
    
    def _check_nulls(self, df: pl.DataFrame, columns: list[str]) -> list[str]:
        errors = []
        for col in columns:
            if col in df.columns:
                null_count = df.select(pl.col(col).is_null().sum()).item()
                if null_count > 0:
                    errors.append(
                        f"Column '{col}' has {null_count:,} null values "
                        f"({null_count / len(df) * 100:.1f}% of records)"
                    )
        return errors
    
    def _check_positive_values(self, df: pl.DataFrame, column: str) -> list[str]:
        if column not in df.columns:
            return []
        non_positive_count = df.filter(pl.col(column) <= 0).height
        if non_positive_count > 0:
            return [f"Column '{column}' has {non_positive_count:,} non-positive values"]
        return []
    
    def _check_non_negative_values(self, df: pl.DataFrame, column: str) -> list[str]:
        if column not in df.columns:
            return []
        negative_count = df.filter(pl.col(column) < 0).height
        if negative_count > 0:
            return [f"Column '{column}' has {negative_count:,} negative values"]
        return []
    
    def _check_allowed_values(self, df: pl.DataFrame, column: str, allowed: set[str]) -> list[str]:
        if column not in df.columns:
            return []
        unique_values = set(df.select(pl.col(column)).unique().to_series().to_list())
        invalid = {v for v in unique_values if v is not None and v not in allowed}
        if invalid:
            return [
                f"Column '{column}' has invalid values: {', '.join(sorted(invalid))}. "
                f"Allowed: {', '.join(sorted(allowed))}"
            ]
        return []
    
    def _print_validation_results(self, source_name: str, df: pl.DataFrame, errors: list[str]):
        if not errors:
            console.print(
                f"   ✅ [green]{source_name}[/green]: {len(df):,} records — "
                f"[bold green]ALL CHECKS PASSED[/bold green]"
            )
        else:
            console.print(
                f"   ❌ [red]{source_name}[/red]: {len(df):,} records — "
                f"[bold red]{len(errors)} ISSUE(S) FOUND[/bold red]"
            )
            for error in errors:
                console.print(f"      ⚠️  {error}")