# 🏦 Payment Reconciliation Engine — Primer

> This is your "just enough to get started" guide. Read through this, skim the resources, and then we build — just like you did with OHLCV.

---

## 1. What Even Is Reconciliation?

Imagine you run an online store. When a customer pays ₹500:

1. **Your app's database** logs: "Order #1234 — ₹500 received"
2. **Razorpay/Stripe** logs: "Payment #RPZ_789 — ₹500 settled"
3. **Your bank statement** shows: "Credit ₹485" (₹15 deducted as gateway fee)

**Reconciliation = matching these 3 records and verifying the money adds up.**

In real companies, this happens across **thousands of transactions daily**, and things go wrong constantly:
- Payments that your app recorded but the gateway didn't (or vice versa)
- Amounts that don't match (partial refunds, fees, currency conversion)
- Duplicate entries, delayed settlements, timezone mismatches

**Without reconciliation:** Money leaks, accounting is wrong, auditors get angry, company loses money.

### Real-world scale of this problem
- **Razorpay** processes 1B+ transactions — they reconcile every single one
- **Stripe** has entire teams for this
- **Every fintech startup** (CRED, PhonePe, Paytm) has reconciliation pipelines
- Even non-fintech companies (Amazon, Flipkart) reconcile seller payouts

---

## 2. The Core Concepts You Need

### 2.1 Transaction Lifecycle (5 min read)
Understand how a payment flows:
```
Customer → Payment Gateway → Acquiring Bank → Issuing Bank → Settlement → Your Bank
```
- **Authorization** — "Can this card pay ₹500?" (instant)
- **Capture** — "Actually charge the ₹500" (instant)
- **Settlement** — "Transfer the money to the merchant's bank" (T+1 to T+3 days)

> [!NOTE]
> The settlement delay is why reconciliation is hard — your app says "paid" on Monday, but the bank shows the credit on Wednesday.

### 2.2 Three-Way Matching
The core algorithm. You're matching records from:
| Source | What it has | Example |
|--------|------------|---------|
| **Internal Ledger** | Order ID, amount, timestamp, status | `ORD_1234, ₹500, 2024-01-15, SUCCESS` |
| **Payment Gateway** | Gateway ref, amount, fees, settlement date | `RPZ_789, ₹500, fee=₹15, settled=2024-01-17` |
| **Bank Statement** | UTR/ref number, credit amount, date | `UTR123456, ₹485, 2024-01-17` |

**Matching strategies:**
- **Exact match** — Transaction IDs match perfectly (ideal case)
- **Fuzzy match** — Amounts are close, dates are within a window, reference numbers are similar but not identical
- **Rule-based** — "If gateway amount minus fee equals bank credit, it's a match"

### 2.3 Discrepancy Types
When matches fail, you classify the problem:
| Type | Meaning | Example |
|------|---------|---------|
| **Missing in Gateway** | We recorded it, gateway didn't | App crash after user paid |
| **Missing in Ledger** | Gateway has it, we don't | Webhook failed |
| **Amount Mismatch** | Both exist but amounts differ | Partial refund not recorded |
| **Duplicate** | Same transaction recorded twice | Retry logic gone wrong |
| **Timing Mismatch** | Settled on unexpected date | Holiday delay |

---

## 3. Tech Stack — What Each Piece Does & Why

### 3.1 Polars (instead of Pandas) — Data Processing
**What:** A blazing-fast DataFrame library written in Rust.
**Why not Pandas?** Polars is 10-50x faster, uses less memory, and has a cleaner API. Using it signals to recruiters that you're aware of modern tooling.

**What to learn (30 min):**
- Reading CSVs: `pl.read_csv()`
- Filtering: `.filter()`
- Joins: `.join()` — this is the core of matching
- GroupBy: `.group_by().agg()` — for aggregating discrepancies
- Lazy evaluation: `pl.scan_csv().lazy()` — for large files

**Resource:**
- 📖 [Polars User Guide — Getting Started](https://docs.pola.rs/user-guide/getting-started/) — just the first 3 sections
- 📹 Quick comparison: search "Polars vs Pandas" on YouTube (any 10-min video)

---

### 3.2 Pydantic + Pandera — Data Validation
**What:** Pydantic validates individual records (like a single transaction). Pandera validates entire DataFrames (like "no column should have nulls").

**Why:** Raw financial data is messy. You validate BEFORE processing to catch garbage early.

**You already know Pydantic from the OHLCV project!** Same concept — define a schema, parse data through it.

**What to learn (15 min):**
- Pandera: Define DataFrame schemas with column types, value ranges, and custom checks

**Resource:**
- 📖 [Pandera docs — DataFrameSchema](https://pandera.readthedocs.io/en/stable/dataframe_schemas.html) — just the quickstart
- You already know Pydantic — nothing new here

---

### 3.3 RapidFuzz — Fuzzy String Matching
**What:** Library for fuzzy string comparison (e.g., "RAZORPAY_789" vs "RPZ-789" → 72% similar).
**Why:** Real-world references are inconsistent. Bank statements truncate names, gateways use different formats.

**What to learn (15 min):**
- `fuzz.ratio()` — basic similarity score (0-100)
- `fuzz.partial_ratio()` — handles substrings
- `process.extractOne()` — find best match from a list

**Resource:**
- 📖 [RapidFuzz README](https://github.com/rapidfuzz/RapidFuzz) — the examples section is all you need

---

### 3.4 SQLAlchemy + PostgreSQL — Persistence
**What:** SQLAlchemy is Python's SQL toolkit. PostgreSQL is the database.
**Why:** You store reconciliation results, audit trails, and historical data.

**What to learn (30 min):**
- Define tables as Python classes (ORM models)
- Basic CRUD: `session.add()`, `session.query()`, `session.commit()`
- Relationships: One-to-many (one reconciliation run has many results)

**Resource:**
- 📖 [SQLAlchemy 2.0 Tutorial](https://docs.sqlalchemy.org/en/20/tutorial/) — just "Working with Data" section
- You can also use **SQLite** during development (zero setup), swap to PostgreSQL later

---

### 3.5 Prefect — Workflow Orchestration
**What:** A framework to define, schedule, and monitor data pipelines.
**Why:** Reconciliation runs daily. You need scheduling, retries, logging, and a dashboard to monitor runs.

**What to learn (20 min):**
- `@flow` and `@task` decorators — that's literally it to start
- Prefect UI — comes free, shows run history and logs

**Resource:**
- 📖 [Prefect Quickstart](https://docs.prefect.io/latest/getting-started/quickstart/) — just the quickstart page

> [!TIP]
> Prefect is optional for v1. You can start with a simple `main.py` script and add orchestration later as a "phase 2" enhancement. Don't let it block you.

---

### 3.6 Jinja2 — Report Generation
**What:** A templating engine. You write an HTML template with placeholders, fill in data, get a beautiful report.
**Why:** The output of reconciliation is a report — matched transactions, mismatches, summary stats.

**What to learn (10 min):**
- `{{ variable }}` — insert data
- `{% for item in list %}` — loops
- `Template.render()` — generate the final HTML

**Resource:**
- 📖 [Jinja2 docs — Synopsis](https://jinja.palletsprojects.com/en/3.1.x/templates/#synopsis) — just the synopsis

---

## 4. Learning Path (Suggested Order)

```
Day 1-2: Understand reconciliation (this doc + resources below)
    ↓
Day 3:   Polars basics (read CSV, filter, join)
    ↓
Day 4:   RapidFuzz (fuzzy matching experiments)
    ↓
Day 5:   SQLAlchemy (basic ORM models, CRUD)
    ↓
Day 6+:  START BUILDING (we design architecture together)
```

> [!IMPORTANT]
> **Don't over-study.** You learned OHLCV by building it. Same approach here — ~5 days of background, then we start coding and you learn the rest on the job.

---

## 5. Quick Resources to Understand the Domain

### Must-read (skim in 30 min total):
1. **What is Payment Reconciliation?** — Search "payment reconciliation explained" on Google. Read any one fintech blog post (Razorpay, Stripe, or Cashfree usually have good ones)
2. **Three-way matching** — Search "3 way matching accounts payable". The accounting concept is the same, just applied to payments

### Optional but impressive if you mention in interviews:
3. **Double-entry bookkeeping** — The foundation of all financial systems. Every transaction has a debit and a credit. Search "double entry bookkeeping basics" (10-min read)
4. **Idempotency in payments** — Why you can't just retry a payment. Search "idempotency payments" (5-min read)

---

## 6. How This Maps to Your OHLCV Experience

You'll find this familiar:

| OHLCV Project | Recon Project | Same Concept |
|---------------|---------------|--------------|
| WebSocket data ingestion | CSV/API data ingestion | **Data ingestion layer** |
| Pydantic schemas for OHLCV | Pydantic schemas for transactions | **Schema validation** |
| Z-score anomaly detection | Fuzzy matching + rule engine | **Core business logic** |
| DuckDB storage | PostgreSQL + SQLAlchemy | **Persistence layer** |
| Streamlit dashboard | Jinja2 reports + optional Streamlit | **Presentation layer** |
| Async pipeline | Prefect orchestration | **Pipeline management** |

**You already have 70% of the mental model. The new bits are just the domain (payments) and a couple of new libraries (Polars, RapidFuzz, SQLAlchemy).**

---

## 7. When You're Ready to Build

Just say the word and we'll:
1. Design the architecture together
2. Set up the project structure
3. Generate realistic sample data (mock transactions with intentional mismatches)
4. Build it piece by piece — same workflow as OHLCV

