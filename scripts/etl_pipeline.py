"""
ETL Pipeline — Olist E-Commerce (Brazilian) Data
=================================================
Converts 9 raw CSV files → clean, customer-level dataset for Tableau dashboarding.

Structure:
    1. EXTRACT  — read raw CSVs
    2. TRANSFORM — clean, engineer features, aggregate to customer level
    3. LOAD     — export final CSV
    4. RUN      — orchestrate the full pipeline

Usage:
    python scripts/etl_pipeline.py
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
# Resolve paths relative to the project root (works whether you
# run from the repo root or from the scripts/ directory)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))

RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")

OUTPUT_CUSTOMER_LEVEL = os.path.join(PROCESSED_DIR, "tableau_customer_level.csv")
OUTPUT_ORDER_LEVEL = os.path.join(PROCESSED_DIR, "tableau_order_level.csv")

# Churn threshold: customers with recency > this many days are flagged as "Churned"
CHURN_THRESHOLD_DAYS = 90


# ═════════════════════════════════════════════════════════════
# STEP 1 — EXTRACT
# ═════════════════════════════════════════════════════════════
def extract() -> dict[str, pd.DataFrame]:
    """
    Read all 9 raw Olist CSV files and return them in a dictionary.
    This is our single source of truth — no modifications are made here.
    """
    print("\n" + "=" * 65)
    print("  STEP 1: EXTRACT — Loading raw datasets")
    print("=" * 65)

    files = {
        "customers":            "olist_customers_dataset.csv",
        "geolocation":          "olist_geolocation_dataset.csv",
        "order_items":          "olist_order_items_dataset.csv",
        "payments":             "olist_order_payments_dataset.csv",
        "reviews":              "olist_order_reviews_dataset.csv",
        "orders":               "olist_orders_dataset.csv",
        "products":             "olist_products_dataset.csv",
        "sellers":              "olist_sellers_dataset.csv",
        "category_translation": "product_category_name_translation.csv",
    }

    datasets: dict[str, pd.DataFrame] = {}

    for name, filename in files.items():
        filepath = os.path.join(RAW_DIR, filename)
        if not os.path.exists(filepath):
            print(f"  ❌ MISSING: {filepath}")
            sys.exit(1)
        df = pd.read_csv(filepath)
        datasets[name] = df
        rows, cols = df.shape
        print(f"  ✅ {name:>25s}  →  {rows:>8,} rows × {cols} cols")

    total_rows = sum(df.shape[0] for df in datasets.values())
    print(f"\n  📊 Total raw records loaded: {total_rows:,}")
    return datasets


# ═════════════════════════════════════════════════════════════
# STEP 2 — TRANSFORM
# ═════════════════════════════════════════════════════════════
def transform(datasets: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Full transformation pipeline:
        2.1  Data Cleaning (duplicates, nulls, dtypes)
        2.2  Merge into unified DataFrame
        2.3  Recency feature engineering
        2.4  Customer-level aggregation
        2.5  Churn flag creation
        2.6  Column selection — keep only what Tableau needs
    """
    print("\n" + "=" * 65)
    print("  STEP 2: TRANSFORM — Cleaning & feature engineering")
    print("=" * 65)

    # Unpack datasets for readability
    customers     = datasets["customers"]
    geolocation   = datasets["geolocation"]
    order_items   = datasets["order_items"]
    payments      = datasets["payments"]
    reviews       = datasets["reviews"]
    orders        = datasets["orders"]
    products      = datasets["products"]
    sellers       = datasets["sellers"]
    cat_translate = datasets["category_translation"]

    # ─────────────────────────────────────────────────────────
    # 2.1  Data Cleaning
    # ─────────────────────────────────────────────────────────
    print("\n  ── 2.1 Data Cleaning ──")

    # Remove duplicate rows from each table
    for name, df in datasets.items():
        before = len(df)
        datasets[name] = df.drop_duplicates()
        dropped = before - len(datasets[name])
        if dropped > 0:
            print(f"    🗑  {name}: dropped {dropped:,} duplicate rows")

    # Re-unpack after dedup
    customers     = datasets["customers"]
    geolocation   = datasets["geolocation"]
    order_items   = datasets["order_items"]
    payments      = datasets["payments"]
    reviews       = datasets["reviews"]
    orders        = datasets["orders"]
    products      = datasets["products"]
    sellers       = datasets["sellers"]
    cat_translate = datasets["category_translation"]

    # ── Fix data types: convert timestamp columns to datetime ──
    timestamp_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    for col in timestamp_cols:
        if col in orders.columns:
            orders[col] = pd.to_datetime(orders[col], errors="coerce")
    print("    ✅ Timestamp columns converted to datetime")

    # ── Drop rows with null customer_id or order_id ──
    orders = orders.dropna(subset=["order_id", "customer_id"])
    print(f"    ✅ Orders after null-critical drop: {len(orders):,} rows")

    # ── Drop orders that were never delivered (no delivery date) ──
    orders = orders.dropna(subset=["order_delivered_customer_date"])
    print(f"    ✅ Orders with confirmed delivery: {len(orders):,} rows")

    # ─────────────────────────────────────────────────────────
    # 2.2  Pre-merge Aggregations (avoid row explosion)
    # ─────────────────────────────────────────────────────────
    print("\n  ── 2.2 Pre-merge Aggregations ──")

    # Geolocation: mean lat/lng per zip code prefix
    geo_agg = (
        geolocation
        .groupby("geolocation_zip_code_prefix", as_index=False)
        .agg(
            geolocation_lat=("geolocation_lat", "mean"),
            geolocation_lng=("geolocation_lng", "mean"),
        )
    )
    print(f"    📍 Geolocation: {len(geolocation):,} → {len(geo_agg):,} unique zips")

    # Payments: aggregate per order
    payment_value_agg = (
        payments
        .groupby("order_id", as_index=False)
        .agg(
            total_payment_value=("payment_value", "sum"),
            payment_installments_total=("payment_installments", "max"),
        )
    )
    dominant_payment = (
        payments
        .sort_values("payment_value", ascending=False)
        .drop_duplicates(subset="order_id", keep="first")[["order_id", "payment_type"]]
        .rename(columns={"payment_type": "dominant_payment_type"})
    )
    payments_agg = pd.merge(payment_value_agg, dominant_payment, on="order_id", how="left")
    print(f"    💳 Payments: {len(payments):,} → {len(payments_agg):,} order-level rows")

    # ─────────────────────────────────────────────────────────
    # 2.3  Core Merge Chain
    # ─────────────────────────────────────────────────────────
    print("\n  ── 2.3 Core Merge Chain ──")

    df = pd.merge(orders, customers, on="customer_id", how="left")          # + customers
    df = pd.merge(df, order_items, on="order_id", how="left")               # + order items
    df = pd.merge(df, products, on="product_id", how="left")                # + products
    df = pd.merge(df, sellers, on="seller_id", how="left")                  # + sellers
    df = pd.merge(df, payments_agg, on="order_id", how="left")              # + payments
    df = pd.merge(df, reviews, on="order_id", how="left")                   # + reviews

    # Geolocation
    df = pd.merge(
        df, geo_agg,
        left_on="customer_zip_code_prefix",
        right_on="geolocation_zip_code_prefix",
        how="left",
    )

    # Category translation (Portuguese → English)
    df = pd.merge(df, cat_translate, on="product_category_name", how="left")
    df["product_category_name_english"] = (
        df["product_category_name_english"]
        .fillna(df["product_category_name"])
        .fillna("Unknown")
    )

    print(f"    ✅ Unified DataFrame: {df.shape[0]:,} rows × {df.shape[1]} cols")

    # ── Remove exact duplicate rows from merged data ──
    before = len(df)
    df = df.drop_duplicates()
    print(f"    🗑  Dropped {before - len(df):,} duplicate rows after merge")

    # ── Drop rows missing price (unusable for revenue) ──
    df = df.dropna(subset=["price"])
    print(f"    ✅ Rows after dropping null price: {len(df):,}")

    # ─────────────────────────────────────────────────────────
    # 2.4  Feature Engineering — Order-Level
    # ─────────────────────────────────────────────────────────
    print("\n  ── 2.4 Feature Engineering (Order-Level) ──")

    # Total order value = price + freight
    df["order_value"] = df["price"] + df["freight_value"].fillna(0)

    # Delivery time in days
    df["delivery_time"] = (
        df["order_delivered_customer_date"] - df["order_purchase_timestamp"]
    ).dt.total_seconds() / 86400

    # Clamp negative delivery times to 0 (data errors)
    df["delivery_time"] = df["delivery_time"].clip(lower=0)

    # Late delivery flag
    df["is_late_delivery"] = np.where(
        df["order_delivered_customer_date"] > df["order_estimated_delivery_date"], 1, 0
    )

    print("    ✅ Created: order_value, delivery_time, is_late_delivery")

    # ─────────────────────────────────────────────────────────
    # 2.5  Recency Calculation
    # ─────────────────────────────────────────────────────────
    print("\n  ── 2.5 Recency (Time-Based Feature) ──")

    # Convert order_purchase_timestamp to date (already datetime from 2.1)
    dataset_max_date = df["order_purchase_timestamp"].max()
    print(f"    📅 Dataset reference date (max purchase): {dataset_max_date}")

    # Recency per customer: days from their latest purchase to the dataset end date
    recency_df = (
        df.groupby("customer_unique_id")["order_purchase_timestamp"]
        .max()
        .reset_index()
    )
    recency_df["recency"] = (
        dataset_max_date - recency_df["order_purchase_timestamp"]
    ).dt.total_seconds() / 86400

    recency_df = recency_df[["customer_unique_id", "recency"]]
    print(f"    ✅ Recency range: {recency_df['recency'].min():.1f} – {recency_df['recency'].max():.1f} days")

    # ─────────────────────────────────────────────────────────
    # 2.6  Customer-Level Aggregation
    # ─────────────────────────────────────────────────────────
    print("\n  ── 2.6 Customer-Level Aggregation ──")

    # (a) Total Orders + (b) Total Revenue + (d) Avg Delivery Time + Avg Review Score
    customer_kpis = (
        df.groupby("customer_unique_id")
        .agg(
            total_orders=("order_id", "nunique"),
            total_revenue=("total_payment_value", "sum"),
            avg_delivery_time=("delivery_time", "mean"),
            avg_review_score=("review_score", "mean"),
        )
        .reset_index()
    )

    # (c) Average Order Value
    customer_kpis["avg_order_value"] = (
        customer_kpis["total_revenue"] / customer_kpis["total_orders"]
    )

    print(f"    ✅ Unique customers: {len(customer_kpis):,}")
    print(f"    📦 Total orders range: {customer_kpis['total_orders'].min()} – {customer_kpis['total_orders'].max()}")
    print(f"    💰 Revenue range: ₹{customer_kpis['total_revenue'].min():.2f} – ₹{customer_kpis['total_revenue'].max():.2f}")

    # ─────────────────────────────────────────────────────────
    # 2.7  Merge Everything Together
    # ─────────────────────────────────────────────────────────
    print("\n  ── 2.7 Merge All Customer Features ──")

    # Merge recency
    customer_kpis = customer_kpis.merge(recency_df, on="customer_unique_id", how="left")

    # Top product category per customer
    top_cat = (
        df.groupby(["customer_unique_id", "product_category_name_english"])
        .size()
        .reset_index(name="count")
    )
    top_cat = (
        top_cat.sort_values("count", ascending=False)
        .drop_duplicates(subset=["customer_unique_id"])
        .rename(columns={"product_category_name_english": "top_category"})
        [["customer_unique_id", "top_category"]]
    )
    customer_kpis = customer_kpis.merge(top_cat, on="customer_unique_id", how="left")

    # Preferred payment type per customer
    pref_pay = (
        df.groupby(["customer_unique_id", "dominant_payment_type"])
        .size()
        .reset_index(name="count")
    )
    pref_pay = (
        pref_pay.sort_values("count", ascending=False)
        .drop_duplicates(subset=["customer_unique_id"])
        .rename(columns={"dominant_payment_type": "payment_type"})
        [["customer_unique_id", "payment_type"]]
    )
    customer_kpis = customer_kpis.merge(pref_pay, on="customer_unique_id", how="left")

    # Location: city and state
    location_df = (
        df.groupby("customer_unique_id")
        .agg(
            customer_city=("customer_city", "first"),
            customer_state=("customer_state", "first"),
        )
        .reset_index()
    )
    customer_kpis = customer_kpis.merge(location_df, on="customer_unique_id", how="left")

    print(f"    ✅ All features merged. Shape: {customer_kpis.shape}")

    # ─────────────────────────────────────────────────────────
    # 2.8  Churn Flag (🔥 VERY IMPORTANT)
    # ─────────────────────────────────────────────────────────
    print("\n  ── 2.8 Churn Flag ──")

    customer_kpis["churn_flag"] = np.where(
        customer_kpis["recency"] > CHURN_THRESHOLD_DAYS,
        "Churned",
        "Active",
    )

    churned = (customer_kpis["churn_flag"] == "Churned").sum()
    active = (customer_kpis["churn_flag"] == "Active").sum()
    churn_rate = churned / len(customer_kpis) * 100

    print(f"    🔴 Churned: {churned:,} ({churn_rate:.1f}%)")
    print(f"    🟢 Active:  {active:,} ({100 - churn_rate:.1f}%)")
    print(f"    ⏱  Threshold: {CHURN_THRESHOLD_DAYS} days")

    # ─────────────────────────────────────────────────────────
    # 2.9  Keep Only Required Columns
    # ─────────────────────────────────────────────────────────
    print("\n  ── 2.9 Final Column Selection ──")

    final_columns = [
        "customer_unique_id",   # Customer identifier
        "total_orders",         # Total distinct orders
        "total_revenue",        # Lifetime revenue
        "avg_order_value",      # Revenue / Orders
        "avg_delivery_time",    # Avg days to deliver
        "avg_review_score",     # Customer satisfaction
        "recency",              # Days since last purchase
        "churn_flag",           # "Churned" or "Active"
        "payment_type",         # Preferred payment method
        "top_category",         # Most purchased category
        "customer_city",        # City
        "customer_state",       # State
    ]

    # Keep only columns that exist (defensive)
    final_columns = [c for c in final_columns if c in customer_kpis.columns]
    customer_kpis = customer_kpis[final_columns]

    print(f"    ✅ Final dataset: {customer_kpis.shape[0]:,} rows × {customer_kpis.shape[1]} cols")
    print(f"    📋 Columns: {list(customer_kpis.columns)}")

    return customer_kpis


# ═════════════════════════════════════════════════════════════
# STEP 3 — LOAD
# ═════════════════════════════════════════════════════════════
def load(df: pd.DataFrame) -> None:
    """
    Export the cleaned, customer-level dataset to CSV.
    This is the file you connect in Tableau.
    """
    print("\n" + "=" * 65)
    print("  STEP 3: LOAD — Exporting to CSV")
    print("=" * 65)

    os.makedirs(PROCESSED_DIR, exist_ok=True)

    df.to_csv(OUTPUT_CUSTOMER_LEVEL, index=False)
    file_size_mb = os.path.getsize(OUTPUT_CUSTOMER_LEVEL) / (1024 * 1024)

    print(f"    ✅ Saved to: {OUTPUT_CUSTOMER_LEVEL}")
    print(f"    📦 File size: {file_size_mb:.2f} MB")
    print(f"    📊 Rows: {len(df):,}  |  Columns: {df.shape[1]}")

    # Quick sample preview
    print("\n    ── Sample Output (first 5 rows) ──")
    print(df.head().to_string(index=False))


# ═════════════════════════════════════════════════════════════
# STEP 4 — RUN PIPELINE
# ═════════════════════════════════════════════════════════════
def run_pipeline() -> None:
    """
    Orchestrate the full ETL pipeline: Extract → Transform → Load.
    """
    start_time = datetime.now()

    print("\n" + "🚀" * 25)
    print("  OLIST E-COMMERCE ETL PIPELINE")
    print("  Started at:", start_time.strftime("%Y-%m-%d %H:%M:%S"))
    print("🚀" * 25)

    # ── Extract ──
    raw_data = extract()

    # ── Transform ──
    customer_dataset = transform(raw_data)

    # ── Load ──
    load(customer_dataset)

    # ── Summary ──
    elapsed = (datetime.now() - start_time).total_seconds()

    print("\n" + "=" * 65)
    print("  ✅ PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 65)
    print(f"    ⏱  Duration: {elapsed:.1f} seconds")
    print(f"    📁 Output:   {OUTPUT_CUSTOMER_LEVEL}")
    print(f"    🔗 Connect this file in Tableau to power your dashboard.")
    print("=" * 65 + "\n")


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_pipeline()
