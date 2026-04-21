# ============================================================
# E-Commerce Data Generator
# Author  : Chereddy Prasantha Reddy
# Project : E-Commerce ETL Pipeline (Airflow + AWS Glue + S3 + Athena)
# GitHub  : github.com/prasanthachereddy/ecommerce-etl-pipeline
# ============================================================
# Purpose : Generates realistic e-commerce data across 5 tables
#           with ~3% intentional dirty data for ETL cleaning.
# Usage   : Called by Airflow DAG  → from data_generator import generate_data
#           Run from terminal      → python data_generator.py
# Install : pip install pandas faker
# ============================================================

import os
import logging
import random
import shutil
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

# ── LOGGING ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── CONFIG ───────────────────────────────────────────────────
# Change settings here — no need to touch the code below
CONFIG = {
    "target_order_items": 100000,   # 750=quick test | 10000=small | 100000=portfolio
    "error_rate":         0.03,     # ~3% dirty data injected into order_items
    "output_dir":         "/tmp/ecommerce",  # Airflow overrides this via parameter
    "locale":             "en_IN",  # Indian locale — names, cities, phone numbers
}

# ── STATIC DATA ──────────────────────────────────────────────
CATEGORIES = [
    {"category_id": 1, "category_name": "Electronics"},
    {"category_id": 2, "category_name": "Clothing"},
    {"category_id": 3, "category_name": "Home Appliances"},
    {"category_id": 4, "category_name": "Books"},
    {"category_id": 5, "category_name": "Sports"},
    {"category_id": 6, "category_name": "Beauty and Personal Care"},
    {"category_id": 7, "category_name": "Groceries"},
]

PRODUCTS_BY_CATEGORY = {
    "Electronics":              ["Smartphone", "Laptop", "Charger", "Headphones", "Tablet", "Smartwatch", "Camera"],
    "Clothing":                 ["T-Shirt", "Jeans", "Jacket", "Dress", "Shorts", "Sweater", "Cap"],
    "Home Appliances":          ["Refrigerator", "Washing Machine", "Microwave", "Air Conditioner", "Blender", "Mixer", "Vacuum Cleaner"],
    "Books":                    ["Novel", "Comics", "Cookbook", "Biography", "Science Book", "Travel Guide", "Children Book"],
    "Sports":                   ["Football", "Cricket Bat", "Tennis Racket", "Basketball", "Yoga Mat", "Dumbbells", "Helmet"],
    "Beauty and Personal Care": ["Shampoo", "Conditioner", "Facewash", "Lipstick", "Perfume", "Lotion", "Soap"],
    "Groceries":                ["Rice", "Wheat", "Sugar", "Oil", "Salt", "Milk", "Tea"],
}

PRICES_BY_CATEGORY = {
    "Electronics":              [65000, 30000, 2000,  8000,  3500,  20000, 18000],
    "Clothing":                 [700,   1800,  3500,  2200,  1500,  900,   1300],
    "Home Appliances":          [28000, 24000, 12000, 42000, 7000,  6000,  1500],
    "Books":                    [450,   600,   550,   900,   500,   650,   350],
    "Sports":                   [2200,  900,   1800,  700,   3000,  3500,  1500],
    "Beauty and Personal Care": [350,   450,   2200,  1800,  1600,  700,   400],
    "Groceries":                [450,   180,   60,    320,   250,   40,    30],
}

DISCOUNTS_BY_CATEGORY = {
    "Electronics":              [0, 0, 0, 5, 5, 10],
    "Clothing":                 [0, 0, 10, 10, 20, 30],
    "Home Appliances":          [0, 0, 0, 5, 10, 15],
    "Books":                    [0, 0, 0, 5, 5, 10],
    "Sports":                   [0, 0, 5, 10, 15, 20],
    "Beauty and Personal Care": [0, 0, 5, 10, 10, 20],
    "Groceries":                [0, 0, 0, 0, 5, 5],
}


# ── TABLE GENERATORS ─────────────────────────────────────────

def generate_categories() -> pd.DataFrame:
    """Returns the static categories table — 7 rows, never changes."""
    log.info("Generating categories...")
    return pd.DataFrame(CATEGORIES)


def generate_products() -> pd.DataFrame:
    """
    Returns the static products table — 49 rows (7 categories x 7 products).
    No category_name column — linked via category_id (foreign key).
    """
    log.info("Generating products...")
    rows = []
    product_id = 1
    for cat in CATEGORIES:
        cat_name = cat["category_name"]
        for i, name in enumerate(PRODUCTS_BY_CATEGORY[cat_name]):
            rows.append({
                "product_id":    product_id,
                "category_id":   cat["category_id"],
                "product_name":  name,
                "default_price": PRICES_BY_CATEGORY[cat_name][i],
            })
            product_id += 1
    return pd.DataFrame(rows)


def generate_customers(n: int, fake: Faker) -> pd.DataFrame:
    """
    Returns n customers with Indian locale data.
    4-digit suffix in email reduces collision risk to <1%.
    Timestamps are realistic — spread between 8am and 10pm.
    """
    log.info(f"Generating {n:,} customers...")
    rows = []
    for cust_id in range(1, n + 1):
        signup_date = fake.date_between(start_date="-90d", end_date="today")
        first_name  = fake.first_name()
        last_name   = fake.last_name()
        suffix      = random.randint(1000, 9999)
        email       = f"{first_name.lower()}{last_name.lower()}{suffix}@example.com"
        created_at  = datetime.combine(signup_date, datetime.min.time()) + timedelta(
            hours=random.randint(8, 22), minutes=random.randint(0, 59)
        )
        rows.append({
            "customer_id": cust_id,
            "first_name":  first_name,
            "last_name":   last_name,
            "email":       email,
            "phone":       fake.phone_number(),
            "city":        fake.city(),
            "signup_date": signup_date,
            "created_at":  created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at":  created_at.strftime("%Y-%m-%d %H:%M:%S"),
        })
    return pd.DataFrame(rows)


def generate_orders(customers_df: pd.DataFrame, n: int, fake: Faker) -> pd.DataFrame:
    """
    Returns n orders linked to existing customers.
    Order date is always >= customer signup date — referential integrity preserved.
    Status weights: 70% completed, 15% pending, 10% cancelled, 5% returned.
    """
    log.info(f"Generating {n:,} orders...")
    payment_methods = ["UPI", "Cash", "Card"]
    statuses        = ["completed", "pending", "cancelled", "returned"]
    status_weights  = [70, 15, 10, 5]
    rows = []
    for order_id in range(1, n + 1):
        customer   = customers_df.sample(1).iloc[0]
        order_date = fake.date_between(start_date=customer["signup_date"], end_date="today")
        created_at = datetime.combine(order_date, datetime.min.time()) + timedelta(
            hours=random.randint(8, 22), minutes=random.randint(0, 59)
        )
        rows.append({
            "order_id":       order_id,
            "customer_id":    int(customer["customer_id"]),
            "order_date":     order_date,
            "payment_method": random.choice(payment_methods),
            "order_status":   random.choices(statuses, weights=status_weights)[0],
            "created_at":     created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at":     created_at.strftime("%Y-%m-%d %H:%M:%S"),
        })
    return pd.DataFrame(rows)


def generate_order_items(
    orders_df:   pd.DataFrame,
    products_df: pd.DataFrame,
    target:      int
) -> pd.DataFrame:
    """
    Returns order items up to target count.
    First 2 products per category (popular items) get 5x higher chance — realistic.
    Each order gets 1-5 items — 2 items most common (35% weight).
    """
    log.info(f"Generating order items (target: {target:,})...")

    product_ids     = list(products_df["product_id"])
    product_lookup  = products_df.set_index("product_id").to_dict("index")
    cat_lookup      = {cat["category_id"]: cat["category_name"] for cat in CATEGORIES}
    # First 2 products per category are popular — 5x higher selection chance
    product_weights = [5 if i % 7 in [0, 1] else 1 for i in range(len(product_ids))]

    rows          = []
    order_item_id = 1

    for _, order in orders_df.iterrows():
        if order_item_id > target:
            break
        num_items = random.choices([1, 2, 3, 4, 5], weights=[25, 35, 25, 10, 5])[0]
        selected  = random.sample(product_ids, min(num_items, len(product_ids)))

        for pid in selected:
            if order_item_id > target:
                break
            prod        = product_lookup[pid]
            cat_name    = cat_lookup[prod["category_id"]]
            qty         = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 15, 7, 3])[0]
            price       = prod["default_price"]
            discount    = random.choice(DISCOUNTS_BY_CATEGORY[cat_name])
            final_price = round(price * (1 - discount / 100), 2)
            created_at  = datetime.combine(order["order_date"], datetime.min.time()) + timedelta(
                hours=random.randint(8, 22), minutes=random.randint(0, 59)
            )
            rows.append({
                "order_item_id": order_item_id,
                "order_id":      int(order["order_id"]),
                "product_id":    int(pid),
                "product_name":  prod["product_name"],
                "quantity":      qty,
                "unit_price":    price,
                "discount":      discount,
                "final_price":   final_price,
                "created_at":    created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "updated_at":    created_at.strftime("%Y-%m-%d %H:%M:%S"),
            })
            order_item_id += 1

    return pd.DataFrame(rows)


def inject_dirty_data(
    order_items_df: pd.DataFrame,
    orders_df:      pd.DataFrame,
    error_rate:     float
) -> tuple:
    """
    Injects 3 types of dirty records into order_items (and orders for future dates).
    These are intentional — the Glue job is expected to detect and clean them.
      - invalid_fk   : product_id 9999 does not exist in products table
      - negative_qty : physically impossible quantity
      - future_date  : order placed in the future
    """
    n_errors      = int(len(order_items_df) * error_rate)
    error_indices = random.sample(range(len(order_items_df)), n_errors)
    log.info(f"Injecting {n_errors:,} dirty records (~{int(error_rate * 100)}% of order items)...")

    for idx in error_indices:
        error_type = random.choice(["invalid_fk", "negative_qty", "future_date"])
        if error_type == "invalid_fk":
            order_items_df.at[idx, "product_id"]   = 9999
            order_items_df.at[idx, "product_name"] = "INVALID_PRODUCT"
        elif error_type == "negative_qty":
            order_items_df.at[idx, "quantity"]    = -random.randint(1, 5)
            order_items_df.at[idx, "final_price"] = 0
        elif error_type == "future_date":
            target_order_id = order_items_df.at[idx, "order_id"]
            future = (datetime.today() + timedelta(days=10)).date()
            orders_df.loc[orders_df["order_id"] == target_order_id, "order_date"] = future

    return order_items_df, orders_df


def validate(
    categories_df:  pd.DataFrame,
    products_df:    pd.DataFrame,
    customers_df:   pd.DataFrame,
    orders_df:      pd.DataFrame,
    order_items_df: pd.DataFrame
) -> None:
    """
    Validates row counts and key column integrity before saving.
    Raises ValueError immediately if any check fails — fail fast, not downstream.
    Note: FK violations in order_items are expected (dirty data injection).
    """
    log.info("Validating generated data...")
    checks = [
        (len(categories_df)  == 7,                  "Categories must have exactly 7 rows"),
        (len(products_df)    == 49,                  "Products must have exactly 49 rows"),
        (len(customers_df)   >  0,                   "Customers table is empty"),
        (len(orders_df)      >  0,                   "Orders table is empty"),
        (len(order_items_df) >  0,                   "Order items table is empty"),
        (customers_df["customer_id"].is_unique,       "Duplicate customer IDs found"),
        (orders_df["order_id"].is_unique,             "Duplicate order IDs found"),
        (order_items_df["order_item_id"].is_unique,   "Duplicate order item IDs found"),
        (customers_df["email"].notna().all(),         "Null emails found in customers"),
        (orders_df["customer_id"].notna().all(),      "Null customer IDs found in orders"),
        (order_items_df["unit_price"].ge(0).all(),    "Negative unit prices found"),
    ]
    for condition, message in checks:
        if not condition:
            raise ValueError(f"Validation failed: {message}")
    log.info("All validation checks passed.")


# ── MAIN ENTRY POINT ─────────────────────────────────────────

def generate_data(
    output_dir:   str = CONFIG["output_dir"],
    target_items: int = CONFIG["target_order_items"]
) -> str:
    """
    Generates a complete e-commerce dataset and saves CSVs to output_dir.

    Called by Airflow PythonOperator:
        from data_generator import generate_data
        generate_data(output_dir='/tmp/ecommerce', target_items=100000)

    Returns:
        output_dir (str) — pushed to XCom so Task 2 (S3 upload) knows where to look.

    Raises:
        ValueError  — if generated data fails validation checks
        Exception   — any unexpected error; partial output is cleaned up automatically
    """
    run_date = datetime.today().strftime("%Y%m%d")
    fake     = Faker(CONFIG["locale"])

    log.info("=" * 55)
    log.info("  E-Commerce Data Generator")
    log.info(f"  Run date     : {run_date}")
    log.info(f"  Target items : {target_items:,}")
    log.info(f"  Output dir   : {output_dir}")
    log.info("=" * 55)

    try:
        # Derive table sizes proportionally from target_order_items
        # avg items per order ≈ 2.35 — dividing by 2.0 gives a buffer to always hit target
        num_customers = max(200,  target_items // 7)
        num_orders    = max(300, int(target_items / 2.0))

        # ── Generate all 5 tables ──
        categories_df  = generate_categories()
        products_df    = generate_products()
        customers_df   = generate_customers(num_customers, fake)
        orders_df      = generate_orders(customers_df, num_orders, fake)
        order_items_df = generate_order_items(orders_df, products_df, target_items)

        # ── Inject intentional dirty data ──
        order_items_df, orders_df = inject_dirty_data(
            order_items_df, orders_df, CONFIG["error_rate"]
        )

        # ── Validate before saving ──
        validate(categories_df, products_df, customers_df, orders_df, order_items_df)

        # ── Save all tables as timestamped CSVs ──
        log.info(f"Saving CSV files to {output_dir}...")
        tables = {
            "categories":  categories_df,
            "products":    products_df,
            "customers":   customers_df,
            "orders":      orders_df,
            "order_items": order_items_df,
        }
        for name, df in tables.items():
            folder = os.path.join(output_dir, name)
            os.makedirs(folder, exist_ok=True)
            path = os.path.join(folder, f"{name}_{run_date}.csv")
            df.to_csv(path, index=False)
            log.info(f"  Saved {len(df):>8,} rows → {path}")

        # ── Summary ──
        log.info("=" * 55)
        log.info("  Data generated successfully!")
        log.info(f"  Categories  : {len(categories_df):>8,}")
        log.info(f"  Products    : {len(products_df):>8,}")
        log.info(f"  Customers   : {len(customers_df):>8,}")
        log.info(f"  Orders      : {len(orders_df):>8,}")
        log.info(f"  Order Items : {len(order_items_df):>8,}")
        log.info(f"  Dirty rows  : {int(len(order_items_df) * CONFIG['error_rate']):>8,}  (~3%)")
        log.info("=" * 55)

        return output_dir   # Airflow Task 2 picks this up via XCom

    except Exception as e:
        log.error(f"Data generation failed: {e}")
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
            log.info(f"Cleaned up partial output at {output_dir}")
        raise   # Re-raise so Airflow marks the task as FAILED — not silently skipped


# ── TERMINAL USE ─────────────────────────────────────────────
if __name__ == "__main__":
    # Run directly from terminal — uses CONFIG defaults
    # To test quickly: change target_items to 750
    generate_data(
        output_dir="data/raw",
        target_items=CONFIG["target_order_items"]
    )
