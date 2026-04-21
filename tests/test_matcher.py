import pytest
import polars as pl

from src.engine.matcher import TransactionMatcher


# ============================================================
# FIXTURES
# ============================================================

@pytest.fixture
def sample_ledger():
    """Provides a small sample internal ledger."""
    return pl.DataFrame({
        "order_id": ["ORD-1", "ORD-2", "ORD-3", "ORD-4"],
        "gateway_ref": ["GW-1", "GW-REF-1234", "GW-3", None],
        "customer_id": ["C1", "C2", "C3", "C4"],
        "amount": [100.0, 200.0, 300.0, 400.0],
        "status": ["SUCCESS", "SUCCESS", "SUCCESS", "PENDING"],
        "payment_method": ["UPI", "CARD", "UPI", "WALLET"],
        "transaction_date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-03"]
    })

@pytest.fixture
def sample_gateway():
    """Provides a small sample gateway dataset."""
    return pl.DataFrame({
        "order_id": ["ORD-1", "ORD-2", "ORD-3", "ORD-5"],
        "gateway_ref": ["GW-1", "GW-REF-123X", "GW-3-TYPO", "GW-5"],
        "amount": [100.0, 200.0, 300.0, 500.0],
        "fee": [2.0, 4.0, 6.0, 10.0],
        "net_amount": [98.0, 196.0, 294.0, 490.0],
        "status": ["SUCCESS", "SUCCESS", "SUCCESS", "SUCCESS"],
        "settlement_date": ["2024-01-02", "2024-01-02", "2024-01-03", "2024-01-04"]
    })

@pytest.fixture
def sample_bank():
    """Provides a small sample bank statement."""
    return pl.DataFrame({
        "transaction_date": ["2024-01-02", "2024-01-02", "2024-01-03"],
        "reference": ["GW-1", "GW-REF-123X", "MISC"],
        "utr_number": ["UTR-1", "UTR-2", "UTR-3"],
        "credit_amount": [98.0, 196.0, 500.0],
        "debit_amount": [0.0, 0.0, 0.0],
        "balance": [1000.0, 1196.0, 1696.0]
    })


# ============================================================
# TESTS
# ============================================================

def test_exact_matching(sample_ledger, sample_gateway, sample_bank):
    """Checks that records with identical IDs are paired correctly."""
    matcher = TransactionMatcher()
    
    results = matcher.exact_match_ledger_gateway(sample_ledger, sample_gateway)
    
    # Verify ORD-1 is matched via exact gateway_ref
    assert len(results["matched"]) == 1
    assert results["matched"]["order_id"][0] == "ORD-1"
    
    # Verify remaining counts
    assert len(results["unmatched_ledger"]) == 3
    assert len(results["unmatched_gateway"]) == 3


def test_fuzzy_matching(sample_ledger, sample_gateway, sample_bank):
    """Checks that IDs with minor typos/differences are paired."""
    matcher = TransactionMatcher()
    
    exact_results = matcher.exact_match_ledger_gateway(sample_ledger, sample_gateway)
    fuzzy_results = matcher.fuzzy_match_remaining(
        exact_results["unmatched_ledger"],
        exact_results["unmatched_gateway"]
    )
    
    # ORD-2 has "GW-REF-1234" vs "GW-REF-123X"
    assert len(fuzzy_results["fuzzy_matched"]) == 1
    assert fuzzy_results["fuzzy_matched"]["order_id"][0] == "ORD-2"
    
    # Ensure confidence score is high but not 100%
    score = fuzzy_results["fuzzy_matched"]["confidence_score"][0]
    assert 80 <= score < 100


def test_bank_statement_integration(sample_ledger, sample_gateway, sample_bank):
    """Checks the final step: linking matched pairs to bank credits."""
    matcher = TransactionMatcher()
    
    results = matcher.match_all(sample_ledger, sample_gateway, sample_bank)
    matched_df = results["matched"]
    
    # Check ORD-1 linkage to bank
    ord1 = matched_df.filter(pl.col("order_id") == "ORD-1")
    assert ord1["utr_number"][0] == "UTR-1"
    assert ord1["bank_credit"][0] == 98.0
    
    # Check ORD-2 linkage (linked despite the fuzzy match at the gateway level)
    ord2 = matched_df.filter(pl.col("order_id") == "ORD-2")
    assert ord2["utr_number"][0] == "UTR-2"