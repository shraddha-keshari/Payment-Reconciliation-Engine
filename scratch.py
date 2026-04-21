import polars as pl
from src.engine.matcher import TransactionMatcher

ledger = pl.DataFrame({
    "order_id": ["ORD-1", "ORD-2", "ORD-3", "ORD-4"],
    "gateway_ref": ["GW-1", "GW-2", "GW-3", None],
    "customer_id": ["C1", "C2", "C3", "C4"],
    "amount": [100.0, 200.0, 300.0, 400.0],
    "status": ["SUCCESS", "SUCCESS", "SUCCESS", "PENDING"],
    "payment_method": ["UPI", "CARD", "UPI", "WALLET"],
    "transaction_date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-03"]
})

gateway = pl.DataFrame({
    "order_id": ["ORD-1", "ORD-2", "ORD-3-TYPO", "ORD-5"],
    "gateway_ref": ["GW-1", "GW-2-NEW", "GW-3", "GW-5"],
    "amount": [100.0, 200.0, 300.0, 500.0],
    "fee": [2.0, 4.0, 6.0, 10.0],
    "net_amount": [98.0, 196.0, 294.0, 490.0],
    "status": ["SUCCESS", "SUCCESS", "SUCCESS", "SUCCESS"],
    "settlement_date": ["2024-01-02", "2024-01-02", "2024-01-03", "2024-01-04"]
})

matcher = TransactionMatcher()
results = matcher.exact_match_ledger_gateway(ledger, gateway)
print(results["matched"])
