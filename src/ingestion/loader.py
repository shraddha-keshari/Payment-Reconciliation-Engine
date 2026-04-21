import polars as pl  
from pathlib import Path
from rich.console import Console
from src.config import DATA_DIR, LEDGER_FILE, GATEWAY_FILE, BANK_FILE

console = Console()

class DataLoader:
    
    def __init__(self, data_dir: Path = DATA_DIR):
        self.data_dir = data_dir
    
    def load_ledger(self) -> pl.DataFrame:
        console.print("[bold blue]📂 Loading internal ledger...[/bold blue]")
        
        filepath = self.data_dir / LEDGER_FILE
        df = self._load_csv(filepath)
        
        cast_expressions = []
        
        if df.schema["amount"] != pl.Float64:
            cast_expressions.append(pl.col("amount").cast(pl.Float64))
        
        if df.schema["transaction_date"] == pl.Utf8 or df.schema["transaction_date"] == pl.String:
            cast_expressions.append(
                pl.col("transaction_date").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
            )
        
        if cast_expressions:
            df = df.with_columns(cast_expressions)
        
        console.print(f"   ✅ Loaded {len(df):,} ledger records")
        return df
    
    def load_gateway(self) -> pl.DataFrame:
        console.print("[bold blue]📂 Loading gateway transactions...[/bold blue]")
        
        filepath = self.data_dir / GATEWAY_FILE
        df = self._load_csv(filepath)
        
        cast_expressions = []
        for col_name in ["amount", "fee", "net_amount"]:
            if col_name in df.columns and df.schema[col_name] != pl.Float64:
                cast_expressions.append(pl.col(col_name).cast(pl.Float64))
        
        if "transaction_date" in df.columns and df.schema["transaction_date"] in (pl.Utf8, pl.String):
            cast_expressions.append(
                pl.col("transaction_date").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
            )
        
        if cast_expressions:
            df = df.with_columns(cast_expressions)
        
        console.print(f"   ✅ Loaded {len(df):,} gateway records")
        return df
    
    def load_bank_statement(self) -> pl.DataFrame:
        console.print("[bold blue]📂 Loading bank statement...[/bold blue]")
        
        filepath = self.data_dir / BANK_FILE
        df = self._load_csv(filepath)
        
        df = df.with_columns([
            pl.col("credit_amount").cast(pl.Float64),
            pl.col("debit_amount").cast(pl.Float64),
            pl.col("balance").cast(pl.Float64),
        ])
        
        console.print(f"   ✅ Loaded {len(df):,} bank records")
        return df
    
    def load_all_sources(self) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        console.print("\n[bold green]🚀 Loading all data sources...[/bold green]\n")
        
        ledger_df = self.load_ledger()
        gateway_df = self.load_gateway()
        bank_df = self.load_bank_statement()
        
        console.print(f"\n[bold green]✅ All sources loaded successfully![/bold green]\n")
        
        return ledger_df, gateway_df, bank_df
    
    def _load_csv(self, filepath: Path) -> pl.DataFrame:
        if not filepath.exists():
            raise FileNotFoundError(
                f"❌ CSV file not found: {filepath}\n"
                f"   Have you run 'python scripts/generate_sample_data.py' first?"
            )
        
        try:
            df = pl.read_csv(
                source=filepath,
                try_parse_dates=True,
                infer_schema_length=10000,
                null_values=["", "null", "NULL", "None"],
            )
            return df
            
        except Exception as e:
            raise Exception(f"❌ Failed to load {filepath}: {e}") from e