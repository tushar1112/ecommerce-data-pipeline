import pandas as pd
import random
from faker import Faker
from datetime import datetime

fake = Faker("en_IN")

# =========================================================
# CONFIG
# =========================================================

BASE_PRODUCTS_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\products.csv"
CATEGORIES_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\categories.csv"
SUPPLIERS_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\suppliers.csv"

OUTPUT_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\incremental\products.csv"

SOURCE_SYSTEM = "PRODUCT_CATALOG_SYSTEM"

NUM_INSERTS = 30
NUM_UPDATES = 80
NUM_DELETES = 10

batch_datetime = datetime.now()

# =========================================================
# LOAD BASE DATA
# =========================================================

products_df = pd.read_csv(BASE_PRODUCTS_PATH)
categories_df = pd.read_csv(CATEGORIES_PATH)
suppliers_df = pd.read_csv(SUPPLIERS_PATH)

products_df = products_df[
    (products_df["product_id"].notna()) &
    (products_df["deleted_flag"] == False)
].copy()

categories_df = categories_df[
    categories_df["is_active"] == True
].copy()

suppliers_df = suppliers_df[
    suppliers_df["supplier_status"] == "ACTIVE"
].copy()

# =========================================================
# CLEAN VALID PRODUCT IDS
# =========================================================

products_df["product_num"] = pd.to_numeric(
    products_df["product_id"].str.extract(r"PROD(\d+)")[0],
    errors="coerce"
)

products_df = products_df[
    products_df["product_num"].notna()
].copy()

products_df["product_num"] = products_df["product_num"].astype(int)

max_product_num = products_df["product_num"].max()

# =========================================================
# HELPERS
# =========================================================

brands = ["Samsung", "Apple", "Dell", "HP", "Nike", "Puma", "Boat", "Sony"]

patterns = ["Pro", "Max", "Ultra", "Lite", "Plus", "Air", "Neo"]


def generate_product_id(num):
    return f"PROD{num:06d}"


def generate_sku(brand, product_id):
    return f"SKU-{brand[:3].upper()}-{product_id[-4:]}-{random.randint(1000,9999)}"


def generate_price():
    mrp = random.randint(500, 150000)
    discount_percent = random.randint(5, 40)
    selling_price = round(mrp - (mrp * discount_percent / 100), 2)
    cost_price = round(selling_price * random.uniform(0.6, 0.85), 2)
    return round(mrp, 2), selling_price, cost_price


def current_ts():
    return batch_datetime.strftime("%Y-%m-%d %H:%M:%S")


# =========================================================
# INSERT PRODUCTS
# =========================================================

insert_records = []

for i in range(1, NUM_INSERTS + 1):

    product_id = generate_product_id(max_product_num + i)

    category_row = categories_df.sample(1).iloc[0]
    supplier_row = suppliers_df.sample(1).iloc[0]

    brand = random.choice(brands)
    product_name = f"{brand} {random.choice(patterns)} {random.randint(100, 9999)}"

    mrp, selling_price, cost_price = generate_price()

    record = {
        "product_id": product_id,
        "product_name": product_name,
        "brand": brand,
        "category_id": category_row["category_id"],
        "supplier_id": supplier_row["supplier_id"],
        "sku": generate_sku(brand, product_id),

        "mrp": mrp,
        "selling_price": selling_price,
        "cost_price": cost_price,

        "product_weight_grams": random.randint(100, 10000),
        "product_rating": round(random.uniform(2.5, 5.0), 1),
        "total_reviews": random.randint(0, 5000),

        "launch_date": batch_datetime.strftime("%Y-%m-%d"),
        "product_status": "ACTIVE",
        "is_returnable": random.choice([True, False]),

        "created_at": current_ts(),
        "updated_at": current_ts(),
        "deleted_flag": False,
        "source_system": SOURCE_SYSTEM,
        "record_version": 1,
        "operation_type": "INSERT"
    }

    insert_records.append(record)

# =========================================================
# UPDATE PRODUCTS
# =========================================================

update_records = []

update_sample = products_df.sample(NUM_UPDATES, replace=False)

for _, row in update_sample.iterrows():

    record = row.drop(labels=["product_num"]).to_dict()

    update_type = random.choice([
        "price_change",
        "status_change",
        "supplier_change",
        "category_change",
        "rating_update"
    ])

    if update_type == "price_change":
        mrp, selling_price, cost_price = generate_price()
        record["mrp"] = mrp
        record["selling_price"] = selling_price
        record["cost_price"] = cost_price

    elif update_type == "status_change":
        record["product_status"] = random.choice([
            "ACTIVE",
            "OUT_OF_STOCK",
            "DISCONTINUED"
        ])

    elif update_type == "supplier_change":
        supplier_row = suppliers_df.sample(1).iloc[0]
        record["supplier_id"] = supplier_row["supplier_id"]

    elif update_type == "category_change":
        category_row = categories_df.sample(1).iloc[0]
        record["category_id"] = category_row["category_id"]

    elif update_type == "rating_update":
        record["product_rating"] = round(random.uniform(2.5, 5.0), 1)
        record["total_reviews"] = int(record["total_reviews"]) + random.randint(1, 200)

    record["updated_at"] = current_ts()
    record["record_version"] = int(record["record_version"]) + 1
    record["deleted_flag"] = False
    record["operation_type"] = "UPDATE"

    update_records.append(record)

# =========================================================
# DELETE PRODUCTS - SOFT DELETE
# =========================================================

delete_records = []

delete_sample = products_df.drop(update_sample.index).sample(NUM_DELETES, replace=False)

for _, row in delete_sample.iterrows():

    record = row.drop(labels=["product_num"]).to_dict()

    record["product_status"] = "DISCONTINUED"
    record["deleted_flag"] = True
    record["updated_at"] = current_ts()
    record["record_version"] = int(record["record_version"]) + 1
    record["operation_type"] = "DELETE"

    delete_records.append(record)

# =========================================================
# COMBINE
# =========================================================

incremental_df = pd.DataFrame(
    insert_records + update_records + delete_records
)

incremental_df = incremental_df.sample(frac=1).reset_index(drop=True)

# Ensure date format
incremental_df["launch_date"] = pd.to_datetime(
    incremental_df["launch_date"]
).dt.strftime("%Y-%m-%d")

incremental_df["created_at"] = pd.to_datetime(
    incremental_df["created_at"]
).dt.strftime("%Y-%m-%d %H:%M:%S")

incremental_df["updated_at"] = pd.to_datetime(
    incremental_df["updated_at"]
).dt.strftime("%Y-%m-%d %H:%M:%S")

incremental_df.to_csv(OUTPUT_PATH, index=False)

print("===================================")
print("products_increment.csv generated successfully")
print("INSERT records :", len(insert_records))
print("UPDATE records :", len(update_records))
print("DELETE records :", len(delete_records))
print("TOTAL records  :", len(incremental_df))
print("===================================")