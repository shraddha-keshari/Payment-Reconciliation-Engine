from datetime import datetime
from sqlalchemy import (
    Column,           
    Integer,          
    Float,            
    String,           
    DateTime,         
    Text,             
    ForeignKey,       
    create_engine,    
)
from sqlalchemy.orm import (
    DeclarativeBase,  
    relationship,     
    Session,          
)

# ============================================================
# BASE CLASS
# ============================================================

class Base(DeclarativeBase):
    pass

# ============================================================
# TABLE: ReconciliationRun
# ============================================================

class ReconciliationRun(Base):
    __tablename__ = "reconciliation_runs"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    run_timestamp = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # --- Input Statistics ---
    total_ledger_records = Column(Integer, nullable=False)    
    total_gateway_records = Column(Integer, nullable=False)   
    total_bank_records = Column(Integer, nullable=False)      
    
    # --- Matching Results ---
    total_matched = Column(Integer, nullable=False)           
    total_discrepancies = Column(Integer, nullable=False)     
    match_rate = Column(Float, nullable=False)                
    
    exact_matches = Column(Integer, default=0)                
    fuzzy_matches = Column(Integer, default=0)                
    rule_based_matches = Column(Integer, default=0)           
    
    # --- Discrepancy Breakdown ---
    missing_in_gateway = Column(Integer, default=0)
    missing_in_ledger = Column(Integer, default=0)
    amount_mismatches = Column(Integer, default=0)
    duplicates_found = Column(Integer, default=0)
    
    # --- Financial Summary ---
    total_amount_ledger = Column(Float, default=0.0)          
    total_amount_gateway = Column(Float, default=0.0)         
    total_amount_bank = Column(Float, default=0.0)            
    
    # --- Relationships ---
    matched_transactions = relationship(
        "MatchedTransaction", 
        back_populates="run",
        cascade="all, delete-orphan"
    )
    
    discrepancies = relationship(
        "Discrepancy",
        back_populates="run",
        cascade="all, delete-orphan"
    )
    
    def __repr__(self) -> str:
        return (
            f"<ReconciliationRun(id={self.id}, "
            f"match_rate={self.match_rate:.1f}%, "
            f"matched={self.total_matched}, "
            f"discrepancies={self.total_discrepancies})>"
        )

# ============================================================
# TABLE: MatchedTransaction
# ============================================================

class MatchedTransaction(Base):
    __tablename__ = "matched_transactions"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, ForeignKey("reconciliation_runs.id"), nullable=False)
    
    # --- Transaction Identifiers ---
    order_id = Column(String(50), nullable=False)
    gateway_ref = Column(String(50))
    utr_number = Column(String(50))       
    
    # --- Amounts ---
    ledger_amount = Column(Float, nullable=False)
    gateway_amount = Column(Float)
    bank_credit = Column(Float)           
    gateway_fee = Column(Float, default=0.0)
    
    # --- Match Details ---
    match_type = Column(String(20), nullable=False)   
    confidence_score = Column(Float, default=100.0)   
    
    # --- Dates ---
    transaction_date = Column(String(30))
    settlement_date = Column(String(30))
    
    # --- Relationship ---
    run = relationship("ReconciliationRun", back_populates="matched_transactions")
    
    def __repr__(self) -> str:
        return (
            f"<MatchedTransaction(order_id={self.order_id}, "
            f"type={self.match_type}, "
            f"amount=₹{self.ledger_amount:,.2f})>"
        )

# ============================================================
# TABLE: Discrepancy
# ============================================================

class Discrepancy(Base):
    __tablename__ = "discrepancies"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, ForeignKey("reconciliation_runs.id"), nullable=False)
    
    # --- Classification ---
    discrepancy_type = Column(String(30), nullable=False)
    severity = Column(String(10), nullable=False)
    
    # --- Details ---
    order_id = Column(String(50))
    gateway_ref = Column(String(50))
    expected_amount = Column(Float)
    actual_amount = Column(Float)
    difference = Column(Float)
    
    source = Column(String(20))   
    details = Column(Text)        
    
    # --- Relationship ---
    run = relationship("ReconciliationRun", back_populates="discrepancies")
    
    def __repr__(self) -> str:
        return (
            f"<Discrepancy(type={self.discrepancy_type}, "
            f"severity={self.severity}, "
            f"order_id={self.order_id})>"
        )