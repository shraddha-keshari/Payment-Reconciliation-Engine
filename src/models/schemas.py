from datetime import datetime
from pydantic import BaseModel, Field, field_validator
from enum import Enum
from typing import Optional

# ============================================================
# ENUMS — Predefined sets of valid values
# ============================================================

class TransactionStatus(str, Enum):
    SUCCESS = "SUCCESS"      
    FAILED = "FAILED"        
    REFUNDED = "REFUNDED"    

class GatewayStatus(str, Enum):
    CAPTURED = "CAPTURED"    
    FAILED = "FAILED"        
    REFUNDED = "REFUNDED"    

class PaymentMethod(str, Enum):
    UPI = "UPI"                    
    CREDIT_CARD = "CREDIT_CARD"    
    DEBIT_CARD = "DEBIT_CARD"      
    NET_BANKING = "NET_BANKING"    
    WALLET = "WALLET"              

# ============================================================
# PYDANTIC MODELS — Data blueprints for each source
# ============================================================

class LedgerRecord(BaseModel):
    order_id: str = Field(
        ...,  
        description="Unique order identifier from the internal system",
        examples=["ORD_00001"]
    )
    customer_id: str = Field(..., description="Customer identifier")
    amount: float = Field(..., gt=0, description="Transaction amount in INR")
    currency: str = Field(default="INR", description="Currency code")
    status: TransactionStatus = Field(..., description="Transaction status")
    payment_method: PaymentMethod = Field(..., description="Method of payment used")
    transaction_date: datetime = Field(..., description="When the transaction was initiated")
    gateway_ref: str = Field(..., description="Reference ID from the payment gateway")

    @field_validator("order_id")
    @classmethod
    def order_id_must_have_prefix(cls, v: str) -> str:
        if not v.startswith("ORD_"):
            raise ValueError(f"order_id must start with 'ORD_', got: {v}")
        return v

class GatewayRecord(BaseModel):
    gateway_ref: str = Field(..., description="Gateway's unique reference")
    order_id: str = Field(..., description="Merchant's order ID")
    amount: float = Field(..., gt=0, description="Gross transaction amount")
    fee: float = Field(..., ge=0, description="Gateway processing fee")
    net_amount: float = Field(..., description="Amount after fee deduction")
    currency: str = Field(default="INR", description="Currency code")
    status: GatewayStatus = Field(..., description="Gateway-side transaction status")
    payment_method: PaymentMethod = Field(..., description="Payment method used")
    transaction_date: datetime = Field(..., description="When the transaction was initiated")
    settlement_date: Optional[str] = Field(default=None, description="When funds were settled")

    @field_validator("net_amount")
    @classmethod
    def net_amount_must_be_logical(cls, v: float, info) -> float:
        if "amount" in info.data and "fee" in info.data:
            expected = round(info.data["amount"] - info.data["fee"], 2)
            if abs(v - expected) > 0.01:
                raise ValueError(f"net_amount mismatch. Expected: {expected}")
        return v

class BankRecord(BaseModel):
    utr_number: str = Field(..., description="Unique Transaction Reference number")
    reference: str = Field(..., description="Payment reference")
    credit_amount: float = Field(..., ge=0, description="Amount credited")
    debit_amount: float = Field(default=0.0, ge=0, description="Amount debited")
    balance: float = Field(..., description="Account balance")
    transaction_date: str = Field(..., description="Settlement date")
    description: str = Field(..., description="Bank description")

    @field_validator("utr_number")
    @classmethod
    def utr_must_have_prefix(cls, v: str) -> str:
        if not v.startswith("UTR"):
            raise ValueError(f"UTR number must start with 'UTR', got: {v}")
        return v

# ============================================================
# RECONCILIATION RESULT MODELS
# ============================================================

class MatchResult(BaseModel):
    order_id: str = Field(..., description="Internal order ID")
    gateway_ref: str = Field(..., description="Gateway reference")
    utr_number: Optional[str] = Field(default=None, description="Bank UTR number")
    ledger_amount: float = Field(..., description="Amount in internal ledger")
    gateway_amount: float = Field(..., description="Amount in gateway records")
    bank_credit: Optional[float] = Field(default=None, description="Amount credited to bank")
    gateway_fee: float = Field(default=0.0, description="Fee charged by gateway")
    match_type: str = Field(..., description="Match method used")
    confidence_score: float = Field(default=100.0, ge=0, le=100)
    transaction_date: str = Field(..., description="Transaction date")
    settlement_date: Optional[str] = Field(default=None, description="Settlement date")

class DiscrepancyRecord(BaseModel):
    discrepancy_type: str = Field(..., description="Type of discrepancy")
    order_id: Optional[str] = Field(default=None)
    gateway_ref: Optional[str] = Field(default=None)
    expected_amount: Optional[float] = Field(default=None)
    actual_amount: Optional[float] = Field(default=None)
    difference: Optional[float] = Field(default=None)
    source: str = Field(..., description="Source of the record")
    severity: str = Field(default="MEDIUM")
    details: str = Field(default="")

class ReconciliationSummary(BaseModel):
    run_timestamp: datetime = Field(..., description="Reconciliation time")
    total_ledger_records: int = Field(...)
    total_gateway_records: int = Field(...)
    total_bank_records: int = Field(...)
    total_matched: int = Field(...)
    total_discrepancies: int = Field(...)
    match_rate: float = Field(..., ge=0, le=100)
    exact_matches: int = Field(default=0)
    fuzzy_matches: int = Field(default=0)
    rule_based_matches: int = Field(default=0)
    missing_in_gateway: int = Field(default=0)
    missing_in_ledger: int = Field(default=0)
    amount_mismatches: int = Field(default=0)
    duplicates_found: int = Field(default=0)
    total_amount_ledger: float = Field(default=0.0)
    total_amount_gateway: float = Field(default=0.0)
    total_amount_bank: float = Field(default=0.0)