<img width="1196" height="352" alt="Screenshot 2026-04-21 at 15 58 38" src="https://github.com/user-attachments/assets/c3fa314f-07a1-4079-af83-fbe091068171" />
<img width="1410" height="801" alt="Screenshot 2026-04-21 at 15 59 03" src="https://github.com/user-attachments/assets/90eb3cf3-ec86-44f8-a8eb-46bac5a95e36" />
<img width="1386" height="819" alt="Screenshot 2026-04-21 at 15 59 41" src="https://github.com/user-attachments/assets/c67cc529-d55c-4824-a78d-d28b364efb55" />
<img width="1421" height="660" alt="Screenshot 2026-04-21 at 16 00 18" src="https://github.com/user-attachments/assets/9ffe1bdd-8ffe-48d8-9b69-4b44caf58413" />
<img width="1470" height="835" alt="Screenshot 2026-04-21 at 16 16 11" src="https://github.com/user-attachments/assets/6819af7e-1c15-43dc-ad99-05eeefdc7f1c" />
<img width="1470" height="832" alt="Screenshot 2026-04-21 at 16 16 38" src="https://github.com/user-attachments/assets/af907b25-7fda-4d4d-bd7f-705bbfdaeca0" />


# 🏦 Payment Reconciliation Engine

![Python](https://img.shields.io/badge/Python-3.13-blue.svg)
![Polars](https://img.shields.io/badge/Polars-Fast%20DataFrames-orange.svg)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-red.svg)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-ORM-lightgrey.svg)
![Pytest](https://img.shields.io/badge/Pytest-Testing-yellow.svg)

A professional-grade data engineering pipeline designed to automate the three-way reconciliation of financial transactions. This engine ensures that records from an **Internal Application Ledger** perfectly match the transactions processed by a **Payment Gateway** (e.g., Stripe, Razorpay) and the settlements deposited into the **Bank Statement**.

## 📖 About the Project

In the FinTech and e-commerce industry, ensuring financial integrity is critical. Dropped webhooks, manual refunds, currency fluctuations, and gateway outages can cause discrepancies where the company thinks it received money, but the bank never settled it. 

This engine automates the tedious process of finding these discrepancies. It ingests thousands of records, applies multi-stage matching algorithms (including fuzzy matching for typos), classifies exactly why a transaction failed to match, persists the results, and generates actionable analytics.

### Key Features
- **High-Performance Ingestion:** Utilizes `Polars` to load and process CSV files up to 50x faster than traditional Pandas.
- **Strict Data Validation:** Employs `Pydantic` and `Pandera` to ensure incoming data meets financial schema constraints (e.g., non-negative amounts, valid dates, correct enums).
- **Multi-Stage Matching Pipeline:**
  1. **Exact Matching:** High-confidence outer joins on transaction references.
  2. **Fuzzy Matching:** Utilizes `RapidFuzz` (Levenshtein distance) to catch human typos or truncated reference IDs (e.g., `GW-REF-1234` vs `GW-REF-123X`).
  3. **Bank Statement Integration:** Links successfully matched transactions to final bank settlement UTR numbers.
- **Discrepancy Classification:** Automatically categorizes errors into actionable buckets (`MISSING_IN_GATEWAY`, `MISSING_IN_LEDGER`, `AMOUNT_MISMATCH`, `DUPLICATE`) with severity levels.
- **Persistence Layer:** Uses `SQLAlchemy` ORM and the Repository pattern to save historical reconciliation runs into a SQLite database.
- **Analytics & Reporting:**
  - An interactive **Streamlit** dashboard for visualizing match rates and exploring discrepancies.
  - Automated HTML report generation using **Jinja2**.

## 🛠️ Architecture & Implementation

The repository is modularized following best practices for production data applications:

```text
├── data/                  # Sample CSVs (Ledger, Gateway, Bank)
├── output/                # Generated Jinja2 HTML reports
├── scripts/               # Utility scripts (e.g., test data generation)
├── src/
│   ├── config.py          # Global thresholds and configurations
│   ├── ingestion/         # Polars loaders and data validators
│   ├── engine/            # The core matching, rules, and classification logic
│   ├── models/            # SQLAlchemy database models and Pydantic schemas
│   ├── persistence/       # DB connection managers and CRUD repositories
│   └── reporting/         # HTML report generators and Jinja templates
├── tests/                 # Pytest suite for the matching engine
├── dashboard.py           # Streamlit analytics application
└── main.py                # The central pipeline orchestrator
```

## 🚀 How to Run

### 1. Setup Environment
Clone the repository and install the dependencies:
```bash
git clone https://github.com/shraddha-keshari/Payment-Reconciliation-Engine.git
cd Payment-Reconciliation-Engine
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Generate Sample Data
Create a fresh set of sample data with intentional, randomized discrepancies:
```bash
python scripts/generate_sample_data.py
```

### 3. Run the Reconciliation Pipeline
Execute the end-to-end data pipeline to validate, match, and save results:
```bash
python main.py
```

### 4. Launch the Interactive Dashboard
Spin up the Streamlit analytics interface to visualize the results:
```bash
streamlit run dashboard.py
```
*The dashboard will be available at `http://localhost:8501`*

### 5. Run the Test Suite
Ensure the core matching engine logic remains sound:
```bash
pytest tests/
```

## 👨‍💻 Author
**Shraddha Keshari**
- [GitHub Profile](https://github.com/shraddha-keshari)
