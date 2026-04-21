# ============================================================
# __init__.py — Package Initializer for 'src'
#
# WHAT IS THIS FILE?
# In Python, a folder needs an __init__.py file to be treated
# as a "package" (a collection of modules you can import from).
# Without this file, Python won't let you do:
#   from src.config import DATABASE_URL
#
# This file can be empty, but we use it to define what gets
# exported when someone does "from src import *".
# ============================================================

"""
Payment Reconciliation Engine — Source Package

This package contains all the core modules for the reconciliation engine:
- models/       → Data schemas and database models
- ingestion/    → CSV loading and validation
- engine/       → Core matching and classification logic
- persistence/  → Database operations
- reporting/    → HTML report generation
"""
