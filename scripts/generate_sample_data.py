import random
import string
import csv
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import (
    DATA_DIR,
    SAMPLE_TOTAL_TRANSACTIONS,
    SAMPLE_MISSING_IN_GATEWAY,
    SAMPLE_MISSING_IN_LEDGER,
    SAMPLE_AMOUNT_MISMATCHES,
    SAMPLE_DUPLICATES,
    GATEWAY_FEE_PERCENTAGE,
)

def generate_order_id(index: int) -> str:
    return f"ORD_{index:05d}"

def generate_gateway_ref(index: int) -> str:
    random.seed(index + 1000)
    chars = random.choices(string.ascii_uppercase + string.digits, k=8)
    return f"RPZ_{''.join(chars)}"

def generate_utr_number(index: int) -> str:
    random.seed(index + 2000)
    digits = ''.join(random.choices(string.digits, k=12))
    return f"UTR{digits}"

def generate_random_amount() -> float:
    return round(random.uniform(100, 50000), 2)

def generate_random_date(start_date: datetime, end_date: datetime) -> datetime:
    total_days = (end_date - start_date).days
    random_days = random.randint(0, total_days)
    return start_date + timedelta(days=random_days)

def calculate_gateway_fee(amount: float, fee_percentage: float) -> float:
    return round(amount * fee_percentage / 100, 2)

def generate_sample_data():
    random.seed(42)
    os.makedirs(DATA_DIR, exist_ok=True)
    
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 3, 31)
    
    print("📊 Generating base transactions...")
    
    ledger_records = []
    gateway_records = []
    bank_records = []
    
    statuses = ["SUCCESS", "SUCCESS", "SUCCESS", "SUCCESS", "FAILED", "REFUNDED"]
    payment_methods = ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NET_BANKING", "WALLET"]
    
    for i in range(1, SAMPLE_TOTAL_TRANSACTIONS + 1):
        order_id = generate_order_id(i)
        gateway_ref = generate_gateway_ref(i)
        utr = generate_utr_number(i)
        amount = generate_random_amount()
        txn_date = generate_random_date(start_date, end_date)
        status = random.choice(statuses)
        method = random.choice(payment_methods)
        
        fee = calculate_gateway_fee(amount, GATEWAY_FEE_PERCENTAGE) if status == "SUCCESS" else 0.0
        settlement_delay = random.randint(1, 3)
        settlement_date = txn_date + timedelta(days=settlement_delay)
        bank_credit = round(amount - fee, 2) if status == "SUCCESS" else 0.0
        
        ledger_records.append({
            "order_id": order_id,
            "customer_id": f"CUST_{random.randint(1000, 9999)}",
            "amount": amount,
            "currency": "INR",
            "status": status,
            "payment_method": method,
            "transaction_date": txn_date.strftime("%Y-%m-%d %H:%M:%S"),
            "gateway_ref": gateway_ref,
        })
        
        gateway_records.append({
            "gateway_ref": gateway_ref,
            "order_id": order_id,
            "amount": amount,
            "fee": fee,
            "net_amount": round(amount - fee, 2),
            "currency": "INR",
            "status": "CAPTURED" if status == "SUCCESS" else status,
            "payment_method": method,
            "transaction_date": txn_date.strftime("%Y-%m-%d %H:%M:%S"),
            "settlement_date": settlement_date.strftime("%Y-%m-%d") if status == "SUCCESS" else "",
        })
        
        if status == "SUCCESS":
            bank_records.append({
                "utr_number": utr,
                "reference": gateway_ref,
                "credit_amount": bank_credit,
                "debit_amount": 0.0,
                "balance": round(random.uniform(100000, 500000), 2),
                "transaction_date": settlement_date.strftime("%Y-%m-%d"),
                "description": f"PAYMENT SETTLEMENT - {order_id}",
            })

    print("🔧 Introducing intentional mismatches...")
    
    successful_indices = [
        i for i, r in enumerate(ledger_records) if r["status"] == "SUCCESS"
    ]
    
    missing_gateway_indices = set(random.sample(
        successful_indices, 
        min(SAMPLE_MISSING_IN_GATEWAY, len(successful_indices))
    ))
    
    remaining_indices = [i for i in successful_indices if i not in missing_gateway_indices]
    missing_ledger_indices = set(random.sample(
        remaining_indices,
        min(SAMPLE_MISSING_IN_LEDGER, len(remaining_indices))
    ))
    
    still_remaining = [
        i for i in remaining_indices if i not in missing_ledger_indices
    ]
    amount_mismatch_indices = set(random.sample(
        still_remaining,
        min(SAMPLE_AMOUNT_MISMATCHES, len(still_remaining))
    ))
    
    for idx in amount_mismatch_indices:
        offset = round(random.uniform(5, 500), 2) * random.choice([-1, 1])
        ledger_records[idx]["amount"] = round(ledger_records[idx]["amount"] + offset, 2)
        if ledger_records[idx]["amount"] < 0:
            ledger_records[idx]["amount"] = abs(ledger_records[idx]["amount"])
    
    duplicate_indices = random.sample(
        still_remaining,
        min(SAMPLE_DUPLICATES, len(still_remaining))
    )
    
    duplicate_ledger_records = []
    for idx in duplicate_indices:
        dup = dict(ledger_records[idx])
        original_date = datetime.strptime(dup["transaction_date"], "%Y-%m-%d %H:%M:%S")
        dup["transaction_date"] = (original_date + timedelta(seconds=random.randint(1, 60))).strftime("%Y-%m-%d %H:%M:%S")
        duplicate_ledger_records.append(dup)
    
    print("📝 Building final datasets...")
    
    final_gateway = [
        record for i, record in enumerate(gateway_records)
        if i not in missing_gateway_indices
    ]
    
    final_ledger = [
        record for i, record in enumerate(ledger_records)
        if i not in missing_ledger_indices
    ]
    
    final_ledger.extend(duplicate_ledger_records)
    random.shuffle(final_ledger)
    
    final_bank = [
        record for i, record in enumerate(bank_records)
        if i not in missing_gateway_indices
    ]
    
    print("💾 Writing CSV files...")
    
    write_csv(
        filepath=DATA_DIR / "internal_ledger.csv",
        records=final_ledger,
        fieldnames=["order_id", "customer_id", "amount", "currency", "status",
                     "payment_method", "transaction_date", "gateway_ref"]
    )
    
    write_csv(
        filepath=DATA_DIR / "gateway_transactions.csv",
        records=final_gateway,
        fieldnames=["gateway_ref", "order_id", "amount", "fee", "net_amount",
                     "currency", "status", "payment_method", "transaction_date",
                     "settlement_date"]
    )
    
    write_csv(
        filepath=DATA_DIR / "bank_statement.csv",
        records=final_bank,
        fieldnames=["utr_number", "reference", "credit_amount", "debit_amount",
                     "balance", "transaction_date", "description"]
    )
    
    print("\n" + "=" * 60)
    print("✅ Sample data generated successfully!")
    print("=" * 60)
    print(f"📁 Output directory: {DATA_DIR}")
    print(f"\n📋 Internal Ledger:       {len(final_ledger):,} records")
    print(f"📋 Gateway Transactions:  {len(final_gateway):,} records")
    print(f"📋 Bank Statement:        {len(final_bank):,} records")
    print(f"\n🔧 Intentional mismatches introduced:")
    print(f"   ❌ Missing from gateway: {len(missing_gateway_indices)}")
    print(f"   ❌ Missing from ledger:  {len(missing_ledger_indices)}")
    print(f"   ⚠️  Amount mismatches:   {len(amount_mismatch_indices)}")
    print(f"   🔄 Duplicates added:     {len(duplicate_indices)}")
    print("=" * 60)

def write_csv(filepath: Path, records: list[dict], fieldnames: list[str]):
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    print(f"   ✅ Saved: {filepath.name}")

if __name__ == "__main__":
    generate_sample_data()