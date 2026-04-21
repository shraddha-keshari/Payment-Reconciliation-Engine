# 🏦 Payment Reconciliation Engine

A **three-way payment reconciliation engine** built in Python that automatically matches transactions across an internal ledger, payment gateway (Razorpay/Stripe), and bank statement — identifying discrepancies like missing payments, amount mismatches, and duplicates.

---

## 🎯 What Problem Does This Solve?

When a company processes payments, the same transaction appears in **3 different systems**:
1. **Your app's database** (internal ledger)
2. **Payment gateway** (Razorpay, Stripe, etc.)
3. **Bank statement** (actual money movement)

These records often **don't match perfectly** due to:
- Settlement delays (T+1 to T+3 days)
- Gateway fees being deducted
- Missing webhooks or failed logging
- Duplicate entries from retry logic
- Different ID formats across systems

**Reconciliation** is the process of matching these records and flagging discrepancies. Every fintech company (Razorpay, CRED, PhonePe) does this daily for millions of transactions.

---

## ⚙️ How It Works

```
                    ┌──────────────────┐
                    │   CSV Data Files  │
                    │  (3 sources)      │
                    └────────┬─────────┘
                             │
                    ┌────────▼─────────┐
                    │  Data Ingestion   │
                    │  (Polars + Pandera)│
                    └────────┬─────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
     ┌────────▼───┐  ┌──────▼─────┐  ┌────▼────────┐
     │ Exact Match │  │ Fuzzy Match │  │ Rule-Based  │
     │ (Polars     │  │ (RapidFuzz) │  │ Match       │
     │  Joins)     │  │             │  │ (Fee logic) │
     └────────┬───┘  └──────┬─────┘  └────┬────────┘
              │              │              │
              └──────────────┼──────────────┘
                             │
                    ┌────────▼─────────┐
                    │  Classification   │
                    │  (Discrepancy     │
                    │   types)          │
                    └────────┬─────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
     ┌────────▼───┐  ┌──────▼─────┐  ┌────▼────────┐
     │  SQLite DB  │  │ HTML Report │  │  Streamlit  │
     │ (SQLAlchemy)│  │  (Jinja2)   │  │  Dashboard  │
     └────────────┘  └────────────┘  └─────────────┘
```

### Matching Pipeline:
1. **Exact Match** — Join records by transaction ID → catches ~80% of matches
2. **Fuzzy Match** — Use RapidFuzz to find similar reference numbers → catches ~10%
3. **Rule-Based Match** — Apply business rules (e.g., `amount - fee = bank_credit`) → catches ~5%
4. **Classify Remainder** — Everything unmatched is classified as a discrepancy

---

## 🛠️ Tech Stack

| Technology | Purpose |
|-----------|---------|
| **Python 3.11+** | Core language |
| **Polars** | High-performance DataFrame processing |
| **Pydantic** | Data validation for individual records |
| **Pandera** | DataFrame-level schema validation |
| **RapidFuzz** | Fuzzy string matching for reference IDs |
| **SQLAlchemy** | ORM for database operations |
| **SQLite** | Lightweight relational database |
| **Jinja2** | HTML report generation |
| **Streamlit** | Interactive dashboard |
| **Pytest** | Unit testing framework |

---

## 🚀 Quick Start

### 1. Clone & Setup
```bash
# Clone the repository
git clone <your-repo-url>
cd Payment_Reconciliation_Project

# Create a virtual environment
python3 -m venv venv
source venv/bin/activate  # On Mac/Linux

# Install dependencies
pip install -r requirements.txt
```

### 2. Generate Sample Data
```bash
python scripts/generate_sample_data.py
```
This creates realistic test data in `data/` with intentional mismatches.

### 3. Run Reconciliation
```bash
python main.py
```

### 4. View Results
- **HTML Report:** Open `output/reconciliation_report.html` in your browser
- **Streamlit Dashboard:** `streamlit run dashboard.py`

---

## 📊 Sample Output

The engine produces:
- **Match Rate:** Percentage of transactions matched across all 3 sources
- **Discrepancy Report:** Categorized list of mismatches
- **Summary Statistics:** Total processed, matched, mismatched, missing
- **Audit Trail:** Timestamped log of every reconciliation run

---

## 📁 Project Structure

```
Payment_Reconciliation_Project/
├── main.py                    # Entry point — run the pipeline
├── dashboard.py               # Streamlit dashboard
├── requirements.txt           # Python dependencies
├── data/                      # Input CSV files
├── src/
│   ├── config.py              # Settings & constants
│   ├── models/
│   │   ├── schemas.py         # Pydantic data models
│   │   └── database.py        # SQLAlchemy ORM models
│   ├── ingestion/
│   │   ├── loader.py          # CSV loading (Polars)
│   │   └── validator.py       # Data validation (Pandera)
│   ├── engine/
│   │   ├── matcher.py         # Exact + fuzzy matching
│   │   ├── rules.py           # Rule-based matching
│   │   └── classifier.py      # Discrepancy classification
│   ├── persistence/
│   │   ├── db_manager.py      # Database connection
│   │   └── repository.py      # CRUD operations
│   └── reporting/
│       ├── report_generator.py # HTML report builder
│       └── templates/         # Jinja2 templates
├── scripts/
│   └── generate_sample_data.py
├── tests/                     # Unit tests
└── output/                    # Generated reports
```

---

## 📝 License

This project is for educational and portfolio purposes.

---

## 👤 Author

**Your Name** — [Your LinkedIn](https://linkedin.com/in/yourprofile) | [Your GitHub](https://github.com/yourusername)
