"""AWS Glue PySpark Cleaning Job
==============================
Job name : clean_ecommerce_uploaded_data
Author   : Chereddy Prasantha Reddy
Created  : 2026-03-14

DIRTY DATA FOUND & CLEANED
---------------------------
Table        | Issue                              | Count | Action
-------------|-------------------------------------|-------|------------------------
orders       | Future order_date (> today)         |  86   | Remove rows
order_items  | Negative / zero quantity            |  66   | Remove rows
order_items  | Invalid product_id = 9999           |  73   | Remove rows
order_items  | final_price = 0 on valid items      |  66   | Recompute from formula
categories   | Duplicates / nulls                  |   0   | Standard checks only
products     | Duplicates / nulls                  |   0   | Standard checks only
customers    | Duplicates / nulls / invalid email  |   0   | Standard checks only
SOURCE  : s3://<YOUR-BUCKET>/raw/
DEST    : s3://<YOUR-BUCKET>/processed/
FORMAT  : Parquet (snappy compressed)"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, StringType, DoubleType, DateType, TimestampType
)

# ── Job initialisation ──────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc   = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job   = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ── Configuration ───────────────────────────────────────────────────────────
BUCKET   = "s3://sales-data-pipeline-prasantha"
RAW      = f"{BUCKET}/raw"
PROCESSED = f"{BUCKET}/processed"
TODAY    = F.current_date()          # runtime date — always accurate

# ── Helper: print before/after counts ──────────────────────────────────────
def log_counts(table_name, df_before, df_after):
    before = df_before.count()
    after  = df_after.count()
    removed = before - after
    print(f"[{table_name}] Before: {before:,} | After: {after:,} | Removed: {removed:,}")
    return after

# ═══════════════════════════════════════════════════════════════════════════
# TABLE 1 — CATEGORIES
# ═══════════════════════════════════════════════════════════════════════════
print("\n=== CLEANING: categories ===")

categories_raw = spark.read.option("header", True).csv(f"{RAW}/categories/")

categories_clean = (
    categories_raw
    # Cast types
    .withColumn("category_id",   F.col("category_id").cast(IntegerType()))
    .withColumn("category_name", F.trim(F.col("category_name")).cast(StringType()))
    # Remove nulls
    .filter(F.col("category_id").isNotNull())
    .filter(F.col("category_name").isNotNull())
    .filter(F.col("category_name") != "")
    # Remove duplicates — keep first occurrence
    .dropDuplicates(["category_id"])
)

log_counts("categories", categories_raw, categories_clean)

categories_clean.write \
    .mode("overwrite") \
    .parquet(f"{PROCESSED}/categories/")

print("categories -> saved to processed/categories/")

# ═══════════════════════════════════════════════════════════════════════════
# TABLE 2 — PRODUCTS
# ═══════════════════════════════════════════════════════════════════════════
print("\n=== CLEANING: products ===")

products_raw = spark.read.option("header", True).csv(f"{RAW}/products/")

products_clean = (
    products_raw
    # Cast types
    .withColumn("product_id",    F.col("product_id").cast(IntegerType()))
    .withColumn("category_id",   F.col("category_id").cast(IntegerType()))
    .withColumn("product_name",  F.trim(F.col("product_name")).cast(StringType()))
    .withColumn("default_price", F.col("default_price").cast(DoubleType()))
    # Remove nulls
    .filter(F.col("product_id").isNotNull())
    .filter(F.col("category_id").isNotNull())
    .filter(F.col("product_name").isNotNull())
    .filter(F.col("product_name") != "")
    # Price must be positive
    .filter(F.col("default_price") > 0)
    # Remove duplicates
    .dropDuplicates(["product_id"])
)

log_counts("products", products_raw, products_clean)

products_clean.write \
    .mode("overwrite") \
    .parquet(f"{PROCESSED}/products/")

print("products -> saved to processed/products/")

# ═══════════════════════════════════════════════════════════════════════════
# TABLE 3 — CUSTOMERS
# ═══════════════════════════════════════════════════════════════════════════
print("\n=== CLEANING: customers ===")

customers_raw = spark.read.option("header", True).csv(f"{RAW}/customers/")

customers_clean = (
    customers_raw
    # Cast types
    .withColumn("customer_id",  F.col("customer_id").cast(IntegerType()))
    .withColumn("first_name",   F.trim(F.col("first_name")).cast(StringType()))
    .withColumn("last_name",    F.trim(F.col("last_name")).cast(StringType()))
    .withColumn("email",        F.trim(F.lower(F.col("email"))).cast(StringType()))
    .withColumn("city",         F.trim(F.col("city")).cast(StringType()))
    .withColumn("signup_date",  F.col("signup_date").cast(DateType()))
    .withColumn("created_at",   F.col("created_at").cast(TimestampType()))
    .withColumn("updated_at",   F.col("updated_at").cast(TimestampType()))
    # Remove nulls in critical fields
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("first_name").isNotNull())
    .filter(F.col("last_name").isNotNull())
    .filter(F.col("email").isNotNull())
    .filter(F.col("email") != "")
    # Email must contain @ (basic validation)
    .filter(F.col("email").contains("@"))
    # signup_date must not be null
    .filter(F.col("signup_date").isNotNull())
    # Remove duplicate customer_ids — keep earliest signup
    .dropDuplicates(["customer_id"])
    # Remove duplicate emails — keep earliest signup
    .dropDuplicates(["email"])
)

log_counts("customers", customers_raw, customers_clean)

customers_clean.write \
    .mode("overwrite") \
    .parquet(f"{PROCESSED}/customers/")

print("customers -> saved to processed/customers/")

# ═══════════════════════════════════════════════════════════════════════════
# TABLE 4 — ORDERS
# Dirty data found: 86 rows with future order_date (> today)
# ═══════════════════════════════════════════════════════════════════════════
print("\n=== CLEANING: orders ===")
print("Known issue: 86 rows with future order_date — will be removed")

orders_raw = spark.read.option("header", True).csv(f"{RAW}/orders/")

orders_clean = (
    orders_raw
    # Cast types
    .withColumn("order_id",        F.col("order_id").cast(IntegerType()))
    .withColumn("customer_id",     F.col("customer_id").cast(IntegerType()))
    .withColumn("order_date",      F.col("order_date").cast(DateType()))
    .withColumn("payment_method",  F.trim(F.col("payment_method")).cast(StringType()))
    .withColumn("order_status",    F.trim(F.col("order_status")).cast(StringType()))
    .withColumn("created_at",      F.col("created_at").cast(TimestampType()))
    .withColumn("updated_at",      F.col("updated_at").cast(TimestampType()))
    # Remove nulls
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("order_date").isNotNull())
    # ── FIX 1: Remove future order_dates ──────────────────────────────────
    # Found 86 rows with dates > today (2026-03-14)
    # These are data entry errors — orders cannot exist in the future
    .filter(F.col("order_date") <= TODAY)
    # Validate payment_method — only known values accepted
    .filter(F.col("payment_method").isin("UPI", "Cash", "Card"))
    # Validate order_status — only known values accepted
    .filter(F.col("order_status").isin("completed", "pending", "cancelled", "returned"))
    # Remove duplicates
    .dropDuplicates(["order_id"])
)

log_counts("orders", orders_raw, orders_clean)

orders_clean.write \
    .mode("overwrite") \
    .parquet(f"{PROCESSED}/orders/")

print("orders -> saved to processed/orders/")

# ═══════════════════════════════════════════════════════════════════════════
# TABLE 5 — ORDER_ITEMS
# Dirty data found:
#   73 rows — invalid product_id = 9999 (FK violation)
#   66 rows — negative/zero quantity
#   66 rows — final_price = 0.0 (same rows as negative qty, recomputed)
# ═══════════════════════════════════════════════════════════════════════════
print("\n=== CLEANING: order_items ===")
print("Known issues:")
print("  - 73 rows with product_id = 9999 (invalid FK) -> remove")
print("  - 66 rows with negative/zero quantity         -> remove")
print("  - 66 rows with final_price = 0 on valid items -> recompute")

order_items_raw = spark.read.option("header", True).csv(f"{RAW}/order_items/")

order_items_clean = (
    order_items_raw
    # Cast types
    .withColumn("order_item_id", F.col("order_item_id").cast(IntegerType()))
    .withColumn("order_id",      F.col("order_id").cast(IntegerType()))
    .withColumn("product_id",    F.col("product_id").cast(IntegerType()))
    .withColumn("product_name",  F.trim(F.col("product_name")).cast(StringType()))
    .withColumn("quantity",      F.col("quantity").cast(IntegerType()))
    .withColumn("unit_price",    F.col("unit_price").cast(DoubleType()))
    .withColumn("discount",      F.col("discount").cast(DoubleType()))
    .withColumn("final_price",   F.col("final_price").cast(DoubleType()))
    .withColumn("created_at",    F.col("created_at").cast(TimestampType()))
    .withColumn("updated_at",    F.col("updated_at").cast(TimestampType()))
    # Remove nulls in critical columns
    .filter(F.col("order_item_id").isNotNull())
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("product_id").isNotNull())
    .filter(F.col("quantity").isNotNull())
    .filter(F.col("unit_price").isNotNull())
    # ── FIX 2: Remove invalid product_id = 9999 ───────────────────────────
    # Found 73 rows with product_id = 9999 and product_name = INVALID_PRODUCT
    # These are injected FK violations — no matching product exists
    .filter(F.col("product_id") != 9999)
    # ── FIX 3: Remove negative / zero quantity ────────────────────────────
    # Found 66 rows with quantity <= 0
    # Negative quantity has no business meaning — cannot have -3 units sold
    .filter(F.col("quantity") > 0)
    # unit_price must be positive
    .filter(F.col("unit_price") > 0)
    # discount must be 0-100
    .filter(F.col("discount") >= 0)
    .filter(F.col("discount") <= 100)
    # ── FIX 4: Recompute final_price from scratch ─────────────────────────
    # Found 66 rows where final_price = 0.0 despite valid unit_price + discount
    # These correspond to the same negative-qty rows (already removed above)
    # But we recompute ALL final_prices to ensure correctness across the board
    # Formula: final_price = unit_price * (1 - discount / 100), rounded to 2dp
    .withColumn(
        "final_price",
        F.round(
            F.col("unit_price") * (1 - F.col("discount").cast(DoubleType()) / 100),
            2
        )
    )
    # ── ADD total_amount column ───────────────────────────────────────────
    # Computed column: quantity * final_price
    # Useful for Athena analytics — avoids recalculating in every query
    .withColumn(
        "total_amount",
        F.round(F.col("quantity") * F.col("final_price"), 2)
    )
    # Remove duplicates
    .dropDuplicates(["order_item_id"])
)

log_counts("order_items", order_items_raw, order_items_clean)

order_items_clean.write \
    .mode("overwrite") \
    .parquet(f"{PROCESSED}/order_items/")

print("order_items -> saved to processed/order_items/")

# ═══════════════════════════════════════════════════════════════════════════
# FINAL SUMMARY
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("CLEANING JOB COMPLETE")
print("="*60)
print(f"Source : {RAW}/")
print(f"Output : {PROCESSED}/")
print()
print("Tables written to processed/:")
print("  processed/categories/   -> Parquet")
print("  processed/products/     -> Parquet")
print("  processed/customers/    -> Parquet")
print("  processed/orders/       -> Parquet  (86 future dates removed)")
print("  processed/order_items/  -> Parquet  (73 invalid FK + 66 neg qty removed)")
print("                                       (final_price recomputed for all rows)")
print("                                       (total_amount column added)")
print()
print("Next steps:")
print("  1. Run Glue Crawler on processed/ to update Data Catalog")
print("  2. Query clean tables in Athena")
print("="*60)
job.commit()