# Day-1 Data Generator for DE Project
# Language: Python
# Libraries: pandas, random, faker, datetime

import pandas as pd
import random
import os
from faker import Faker
from datetime import datetime, timedelta

fake = Faker('en_IN')

# --------------------------
# ⚙️ Configuration
# --------------------------
TARGET_ORDER_ITEMS = 150000 # <- change this to scale: 750, 5000, 100000 etc.
num_customers      = max(200, TARGET_ORDER_ITEMS // 7)
num_orders         = max(300, TARGET_ORDER_ITEMS // 4)

# --------------------------
# 1️⃣ Categories
# --------------------------
categories = [
    {"category_id": 1, "category_name": "Electronics"},
    {"category_id": 2, "category_name": "Clothing"},
    {"category_id": 3, "category_name": "Home Appliances"},
    {"category_id": 4, "category_name": "Books"},
    {"category_id": 5, "category_name": "Sports"},
    {"category_id": 6, "category_name": "Beauty and Personal Care"},  # FIX 1 — removed &
    {"category_id": 7, "category_name": "Groceries"}
]
categories_df = pd.DataFrame(categories)

# --------------------------
# 2️⃣ Products
# --------------------------
category_products = {
    "Electronics":            ["Smartphone", "Laptop", "Charger", "Headphones", "Tablet", "Smartwatch", "Camera"],
    "Clothing":               ["T-Shirt", "Jeans", "Jacket", "Dress", "Shorts", "Sweater", "Cap"],
    "Home Appliances":        ["Refrigerator", "Washing Machine", "Microwave", "Air Conditioner", "Blender", "Mixer", "Vacuum Cleaner"],
    "Books":                  ["Novel", "Comics", "Cookbook", "Biography", "Science Book", "Travel Guide", "Children Book"],
    "Sports":                 ["Football", "Cricket Bat", "Tennis Racket", "Basketball", "Yoga Mat", "Dumbbells", "Helmet"],
    "Beauty and Personal Care": ["Shampoo", "Conditioner", "Facewash", "Lipstick", "Perfume", "Lotion", "Soap"],  # FIX 1
    "Groceries":              ["Rice", "Wheat", "Sugar", "Oil", "Salt", "Milk", "Tea"]
}

default_prices = {
    "Electronics":            [65000, 30000, 2000,  8000,  3500,  20000, 1200],
    "Clothing":               [700,   1800,  3500,  2200,  1500,  900,   1300],
    "Home Appliances":        [28000, 24000, 12000, 42000, 7000,  6000,  1500],
    "Books":                  [450,   600,   550,   900,   500,   650,   350],
    "Sports":                 [2200,  900,   1800,  700,   3000,  45000, 1500],
    "Beauty and Personal Care": [350, 450,   2200,  1800,  1600,  700,   400],  # FIX 1
    "Groceries":              [450,   180,   60,    320,   250,   40,    30]
}

discount_profiles = {
    "Electronics":            [0, 0, 0, 5, 5, 10],
    "Clothing":               [0, 0, 10, 10, 20, 30],
    "Home Appliances":        [0, 0, 0, 5, 10, 15],
    "Books":                  [0, 0, 0, 5, 5, 10],
    "Sports":                 [0, 0, 5, 10, 15, 20],
    "Beauty and Personal Care": [0, 0, 5, 10, 10, 20],  # FIX 1
    "Groceries":              [0, 0, 0, 0, 5, 5]
}

products = []
product_id = 1
for cat in categories:
    cat_name = cat["category_name"]
    for i, pname in enumerate(category_products[cat_name]):
        products.append({
            "product_id":    product_id,
            "category_id":   cat["category_id"],
            "product_name":  pname,
            "default_price": default_prices[cat_name][i],
            "category_name": cat_name
        })
        product_id += 1
products_df = pd.DataFrame(products)

# build lookup dict once for fast access inside loop
product_lookup = products_df.set_index("product_id").to_dict("index")

# --------------------------
# 3️⃣ Customers
# --------------------------
customers = []
for cust_id in range(1, num_customers + 1):
    signup_date = fake.date_between(start_date="-90d", end_date="today")
    first_name  = fake.first_name()
    last_name   = fake.last_name()
    num         = random.randint(10, 99)
    email       = f"{first_name.lower().replace(' ','')}{last_name.lower().replace(' ','')}{num}@example.com"

    # TIMESTAMP FIX — realistic spread timestamp based on signup_date
    created_at = datetime.combine(signup_date, datetime.min.time()) + timedelta(
        hours=random.randint(8, 22), minutes=random.randint(0, 59)
    )

    customers.append({
        "customer_id": cust_id,
        "first_name":  first_name,
        "last_name":   last_name,
        "email":       email,
        "city":        fake.city(),
        "signup_date": signup_date,
        "created_at":  created_at.strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at":  created_at.strftime("%Y-%m-%d %H:%M:%S")
    })
customers_df = pd.DataFrame(customers)

# --------------------------
# 4️⃣ Orders
# --------------------------
orders = []
order_id = 1
payment_methods = ["UPI", "Cash", "Card"]

statuses       = ["completed", "pending", "cancelled", "returned"]
status_weights = [70, 15, 10, 5]

for _ in range(num_orders):
    customer_row = customers_df.sample(1).iloc[0]
    order_date   = fake.date_between(
        start_date=customer_row["signup_date"],
        end_date="today"
    )

    # TIMESTAMP FIX — realistic spread timestamp based on order_date
    order_created_at = datetime.combine(order_date, datetime.min.time()) + timedelta(
        hours=random.randint(8, 22), minutes=random.randint(0, 59)
    )

    orders.append({
        "order_id":       order_id,
        "customer_id":    int(customer_row["customer_id"]),
        "order_date":     order_date,
        "payment_method": random.choice(payment_methods),
        "order_status":   random.choices(statuses, weights=status_weights)[0],
        "created_at":     order_created_at.strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at":     order_created_at.strftime("%Y-%m-%d %H:%M:%S")
    })
    order_id += 1
orders_df = pd.DataFrame(orders)

# --------------------------
# 5️⃣ Order Items
# --------------------------
order_items   = []
order_item_id = 1

product_ids     = list(products_df["product_id"])
product_weights = [5 if i % 7 in [0, 1] else 1 for i in range(len(product_ids))]

for _, order_row in orders_df.iterrows():
    if order_item_id > TARGET_ORDER_ITEMS:
        break

    num_items         = random.choices([1, 2, 3, 4, 5], weights=[25, 35, 25, 10, 5])[0]
    selected_products = list(set(random.choices(product_ids, weights=product_weights, k=num_items)))

    for pid in selected_products:
        if order_item_id > TARGET_ORDER_ITEMS:
            break

        prod         = product_lookup[pid]
        qty          = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 15, 7, 3])[0]
        price        = prod["default_price"]
        product_name = prod["product_name"]
        cat_name     = prod["category_name"]
        discount     = random.choice(discount_profiles[cat_name])
        final_price  = round(price * (1 - discount / 100), 2)

        # TIMESTAMP FIX — realistic spread timestamp based on order_date
        item_created_at = datetime.combine(order_row["order_date"], datetime.min.time()) + timedelta(
            hours=random.randint(8, 22), minutes=random.randint(0, 59)
        )

        order_items.append({
            "order_item_id": order_item_id,
            "order_id":      int(order_row["order_id"]),
            "product_id":    int(pid),
            "product_name":  product_name,
            "quantity":      qty,
            "unit_price":    price,
            "discount":      discount,
            "final_price":   final_price,
            "created_at":    item_created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at":    item_created_at.strftime("%Y-%m-%d %H:%M:%S")
        })
        order_item_id += 1

order_items_df = pd.DataFrame(order_items)

# --------------------------
# 6️⃣ Error Injection (~3%)
# --------------------------
num_errors    = int(len(order_items_df) * 0.03)
error_indices = random.sample(range(len(order_items_df)), num_errors)

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
        future = (datetime.today() + timedelta(days=10)).date()  # FIX 3 — consistent date type
        orders_df.loc[orders_df["order_id"] == target_order_id, "order_date"] = future

# --------------------------
# 7️⃣ Export CSVs — FIX 2: subfolder structure for Glue Crawler
# --------------------------
products_df = products_df.drop(columns=["category_name"])

base_path = "data/raw"
tables = {
    "categories":  categories_df,
    "products":    products_df,
    "customers":   customers_df,
    "orders":      orders_df,
    "order_items": order_items_df
}

for table_name, df in tables.items():
    folder = os.path.join(base_path, table_name)
    os.makedirs(folder, exist_ok=True)
    df.to_csv(os.path.join(folder, f"{table_name}.csv"), index=False)

print("Data generated successfully!")
print(f"   Categories : {len(categories_df)}")
print(f"   Products   : {len(products_df)}")
print(f"   Customers  : {len(customers_df)}")
print(f"   Orders     : {len(orders_df)}")
print(f"   Order Items: {len(order_items_df)}")
print(f"   Errors injected: {num_errors}")
print(f"\n   Files saved to: {base_path}/")
print(f"   S3 sync command:")
print(f"   aws s3 sync {base_path}/ s3://YOUR-BUCKET-NAME/raw/")
