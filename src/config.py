from pathlib import Path

# ============================================================
# DIRECTORY PATHS
# ============================================================

# Resolves the absolute path to the project root relative to this file
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Input data source directory
DATA_DIR = PROJECT_ROOT / "data"

# Directory for generated HTML reports and exports
OUTPUT_DIR = PROJECT_ROOT / "output"

# Directory for Jinja2 HTML templates
TEMPLATES_DIR = PROJECT_ROOT / "src" / "reporting" / "templates"

# ============================================================
# DATABASE CONFIGURATION
# ============================================================

# SQLite connection string - defines where the DB file is created
DATABASE_URL = f"sqlite:///{PROJECT_ROOT / 'reconciliation.db'}"

# ============================================================
# INPUT FILE NAMES
# ============================================================

LEDGER_FILE = "internal_ledger.csv"         
GATEWAY_FILE = "gateway_transactions.csv"   
BANK_FILE = "bank_statement.csv"            

# ============================================================
# MATCHING CONFIGURATION
# ============================================================

# Similarity score threshold for RapidFuzz (0-100)
FUZZY_MATCH_THRESHOLD = 80

# Maximum days allowed between transaction and bank settlement
DATE_TOLERANCE_DAYS = 3

# Rupee tolerance for rounding errors in amount matching
AMOUNT_TOLERANCE = 1.0

# ============================================================
# SAMPLE DATA GENERATION
# ============================================================

SAMPLE_TOTAL_TRANSACTIONS = 1000
SAMPLE_MISSING_IN_GATEWAY = 50    
SAMPLE_MISSING_IN_LEDGER = 30     
SAMPLE_AMOUNT_MISMATCHES = 40     
SAMPLE_DUPLICATES = 20            

# ============================================================
# GATEWAY FEE CONFIGURATION
# ============================================================

# Standard fee percentage applied by the payment aggregator
GATEWAY_FEE_PERCENTAGE = 2.0  

# ============================================================
# REPORT CONFIGURATION
# ============================================================

REPORT_FILENAME = "reconciliation_report.html"
COMPANY_NAME = "Payment Reconciliation Engine"