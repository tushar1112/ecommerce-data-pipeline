# =========================================================
# inventory_generator.py
# =========================================================

import pandas as pd
import random
from faker import Faker
from datetime import timedelta

fake = Faker("en_IN")

# =========================================================
# CONFIGURATION
# =========================================================

SOURCE_SYSTEM = "WMS"

PRODUCTS_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\products.csv"
WAREHOUSES_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\warehouses.csv"
OUTPUT_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\inventory.csv"

# =========================================================
# LOAD REQUIRED TABLES
# =========================================================

products_df = pd.read_csv(PRODUCTS_PATH)
warehouses_df = pd.read_csv(WAREHOUSES_PATH)

products_df = products_df[
    (products_df["deleted_flag"] == False) &
    (products_df["product_status"] != "DISCONTINUED")
]

warehouses_df = warehouses_df[
    (warehouses_df["deleted_flag"] == False) &
    (warehouses_df["is_active"] == True)
]

product_ids = products_df["product_id"].tolist()
warehouse_ids = warehouses_df["warehouse_id"].tolist()

# =========================================================
# HELPER FUNCTIONS
# =========================================================

def generate_inventory_id(counter):
    return f"INV{counter:07d}"


def derive_inventory_status(available_quantity, reorder_level):
    if available_quantity <= 0:
        return "OUT_OF_STOCK"
    elif available_quantity <= reorder_level:
        return "LOW_STOCK"
    else:
        return "IN_STOCK"


def generate_metadata():
    created_at = fake.date_time_between(
        start_date="-2y",
        end_date="-6M"
    )

    updated_at = fake.date_time_between(
        start_date=created_at,
        end_date="now"
    )

    return {
        "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at": updated_at.strftime("%Y-%m-%d %H:%M:%S"),
        "deleted_flag": random.choices(
            [False, True],
            weights=[99, 1]
        )[0],
        "source_system": SOURCE_SYSTEM,
        "record_version": random.randint(1, 5)
    }


# =========================================================
# GENERATE INVENTORY RECORDS
# =========================================================

inventory = []
inventory_counter = 1

for product_id in product_ids:

    # Each product exists in 1 to 4 warehouses
    num_warehouses_for_product = random.choices(
        [1, 2, 3, 4],
        weights=[45, 30, 18, 7]
    )[0]

    selected_warehouses = random.sample(
        warehouse_ids,
        min(num_warehouses_for_product, len(warehouse_ids))
    )

    for warehouse_id in selected_warehouses:

        reorder_level = random.randint(10, 100)

        available_quantity = random.choices(
            [
                random.randint(0, 10),
                random.randint(11, 100),
                random.randint(101, 1000),
                random.randint(1001, 5000)
            ],
            weights=[10, 25, 45, 20]
        )[0]

        reserved_quantity = random.randint(
            0,
            min(available_quantity, 100)
        )

        inventory_status = derive_inventory_status(
            available_quantity,
            reorder_level
        )

        last_restock_date = fake.date_between(
            start_date="-1y",
            end_date="today"
        )

        metadata = generate_metadata()

        inventory_record = {
            "inventory_id": generate_inventory_id(inventory_counter),
            "product_id": product_id,
            "warehouse_id": warehouse_id,

            "available_quantity": available_quantity,
            "reserved_quantity": reserved_quantity,
            "reorder_level": reorder_level,
            "inventory_status": inventory_status,
            "last_restock_date": last_restock_date,
            "operation_type": "FULL_LOAD",
            **metadata
        }

        inventory.append(inventory_record)
        inventory_counter += 1


# =========================================================
# CREATE DATAFRAME
# =========================================================

df = pd.DataFrame(inventory)

# =========================================================
# DIRTY DATA INJECTION
# =========================================================

for index in df.index:

    # 1. Negative available quantity
    if random.random() < 0.005:
        df.at[index, "available_quantity"] = -random.randint(1, 50)

    # 2. Reserved quantity greater than available quantity
    if random.random() < 0.005:
        df.at[index, "reserved_quantity"] = (
            df.at[index, "available_quantity"] + random.randint(10, 100)
        )

    # 3. Missing warehouse_id
    if random.random() < 0.002:
        df.at[index, "warehouse_id"] = None

    # 4. Null inventory_status
    if random.random() < 0.002:
        df.at[index, "inventory_status"] = None


# =========================================================
# SAVE CSV
# =========================================================

df.to_csv(OUTPUT_PATH, index=False)

print("===================================")
print("inventory.csv generated successfully")
print("Total Records :", len(df))
print("===================================")