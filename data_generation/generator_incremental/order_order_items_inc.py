# =========================================================
# orders_order_items_incremental_generator.py
# =========================================================

import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker("en_IN")

# =========================================================
# CONFIG
# =========================================================

BASE_ORDERS_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\orders.csv"
BASE_ORDER_ITEMS_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\order_items.csv"
CUSTOMERS_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\customers.csv"
PRODUCTS_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\products.csv"
INVENTORY_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\inventory.csv"

ORDERS_OUTPUT_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\incremental\orders.csv"
ORDER_ITEMS_OUTPUT_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\incremental\order_items.csv"

SOURCE_SYSTEM = "OMS"

NUM_NEW_ORDERS = 200
NUM_UPDATED_ORDERS = 100
NUM_DELETED_ORDERS = 20

batch_datetime = datetime.now()


# =========================================================
# LOAD BASE DATA
# =========================================================

orders_df = pd.read_csv(BASE_ORDERS_PATH)
order_items_df = pd.read_csv(BASE_ORDER_ITEMS_PATH)
customers_df = pd.read_csv(CUSTOMERS_PATH)
products_df = pd.read_csv(PRODUCTS_PATH)
inventory_df = pd.read_csv(INVENTORY_PATH)

orders_df = orders_df[
    (orders_df["order_id"].notna()) &
    (orders_df["deleted_flag"] == False)
].copy()

order_items_df = order_items_df[
    (order_items_df["order_id"].notna()) &
    (order_items_df["deleted_flag"] == False)
].copy()

customers_df = customers_df[
    (customers_df["customer_id"].notna()) &
    (customers_df["deleted_flag"] == False)
].copy()

products_df = products_df[
    (products_df["product_id"].notna()) &
    (products_df["deleted_flag"] == False) &
    (products_df["product_status"] != "DISCONTINUED")
].copy()

inventory_df = inventory_df[
    (inventory_df["product_id"].notna()) &
    (inventory_df["warehouse_id"].notna()) &
    (inventory_df["deleted_flag"] == False)
].copy()


# =========================================================
# CLEAN VALID IDS
# =========================================================

orders_df["order_num"] = pd.to_numeric(
    orders_df["order_id"].astype(str).str.extract(r"ORD(\d+)")[0],
    errors="coerce"
)

order_items_df["order_item_num"] = pd.to_numeric(
    order_items_df["order_item_id"].astype(str).str.extract(r"OI(\d+)")[0],
    errors="coerce"
)

orders_df = orders_df[orders_df["order_num"].notna()].copy()
order_items_df = order_items_df[order_items_df["order_item_num"].notna()].copy()

orders_df["order_num"] = orders_df["order_num"].astype(int)
order_items_df["order_item_num"] = order_items_df["order_item_num"].astype(int)

max_order_num = orders_df["order_num"].max()
max_order_item_num = order_items_df["order_item_num"].max()


# =========================================================
# HELPERS
# =========================================================

def generate_order_id(num):
    return f"ORD{num:08d}"


def generate_order_item_id(num):
    return f"OI{num:09d}"


def current_ts():
    return batch_datetime.strftime("%Y-%m-%d %H:%M:%S")


def random_order_status():
    return random.choices(
        ["PLACED", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"],
        weights=[5, 10, 15, 55, 10, 5]
    )[0]


def get_payment_status(order_status):
    mapping = {
        "PLACED": "PENDING",
        "CONFIRMED": "SUCCESS",
        "SHIPPED": "SUCCESS",
        "DELIVERED": "SUCCESS",
        "CANCELLED": random.choice(["FAILED", "REFUNDED"]),
        "RETURNED": "REFUNDED"
    }
    return mapping.get(order_status, "PENDING")


def get_item_status(order_status):
    mapping = {
        "PLACED": "PROCESSING",
        "CONFIRMED": "PROCESSING",
        "SHIPPED": "SHIPPED",
        "DELIVERED": "DELIVERED",
        "CANCELLED": "CANCELLED",
        "RETURNED": "RETURNED"
    }
    return mapping.get(order_status, "PROCESSING")


def choose_customer():
    return customers_df.sample(1).iloc[0]


def choose_product_with_inventory():
    product_row = products_df.sample(1).iloc[0]
    product_id = product_row["product_id"]

    inv_records = inventory_df[inventory_df["product_id"] == product_id]

    if len(inv_records) == 0:
        warehouse_id = inventory_df.sample(1).iloc[0]["warehouse_id"]
    else:
        warehouse_id = inv_records.sample(1).iloc[0]["warehouse_id"]

    return product_row, warehouse_id


# =========================================================
# 1. INSERT NEW ORDERS + ORDER ITEMS
# =========================================================

new_orders = []
new_order_items = []

order_counter = max_order_num + 1
order_item_counter = max_order_item_num + 1

for _ in range(NUM_NEW_ORDERS):

    order_id = generate_order_id(order_counter)
    order_counter += 1

    customer = choose_customer()

    order_date = batch_datetime - timedelta(
        minutes=random.randint(1, 1440)
    )

    order_status = random_order_status()
    payment_status = get_payment_status(order_status)

    num_items = random.choices(
        [1, 2, 3, 4],
        weights=[55, 25, 15, 5]
    )[0]

    order_item_records = []

    for _ in range(num_items):

        product, warehouse_id = choose_product_with_inventory()

        quantity = random.choices(
            [1, 2, 3, 4],
            weights=[70, 20, 7, 3]
        )[0]

        unit_price = round(float(product["selling_price"]), 2)
        gross_amount = round(quantity * unit_price, 2)
        discount_amount = round(gross_amount * random.uniform(0, 0.20), 2)
        final_price = round(gross_amount - discount_amount, 2)

        order_item_record = {
            "order_id": order_id,
            "product_id": product["product_id"],
            "warehouse_id": warehouse_id,
            "order_item_id": generate_order_item_id(order_item_counter),

            "quantity": quantity,
            "unit_price": unit_price,
            "gross_amount": gross_amount,
            "discount_amount": discount_amount,
            "final_price": final_price,

            "item_status": get_item_status(order_status),
            "estimated_shipping_cost": round(random.uniform(30, 500), 2),

            "created_at": order_date.strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at": current_ts(),
            "deleted_flag": False,
            "source_system": SOURCE_SYSTEM,
            "record_version": 1,

            "operation_type": "INSERT"
        }

        order_item_records.append(order_item_record)
        order_item_counter += 1

    total_amount = round(sum(x["gross_amount"] for x in order_item_records), 2)
    total_discount_amount = round(sum(x["discount_amount"] for x in order_item_records), 2)
    final_payable_amount = round(sum(x["final_price"] for x in order_item_records), 2)

    expected_delivery_date = order_date + timedelta(days=random.randint(2, 10))

    delivered_date = None
    if order_status in ["DELIVERED", "RETURNED"]:
        delivered_date = expected_delivery_date + timedelta(days=random.randint(0, 3))

    order_record = {
        "customer_id": customer["customer_id"],
        "order_id": order_id,
        "order_date": order_date.strftime("%Y-%m-%d %H:%M:%S"),
        "expected_delivery_date": expected_delivery_date.strftime("%Y-%m-%d %H:%M:%S"),
        "delivered_date": delivered_date.strftime("%Y-%m-%d %H:%M:%S") if delivered_date else None,

        "total_amount": total_amount,
        "total_discount_amount": total_discount_amount,
        "final_payable_amount": final_payable_amount,

        "order_status": order_status,
        "payment_status": payment_status,

        "shipping_city": customer["city"],
        "shipping_state": customer["state"],
        "shipping_postal_code": customer["postal_code"],

        "order_source": random.choice(["ANDROID_APP", "IOS_APP", "WEBSITE"]),
        "device_type": random.choice(["MOBILE", "DESKTOP", "TABLET"]),

        "created_at": order_date.strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at": current_ts(),
        "deleted_flag": False,
        "source_system": SOURCE_SYSTEM,
        "record_version": 1,

        "operation_type": "INSERT"
    }

    new_orders.append(order_record)
    new_order_items.extend(order_item_records)


# =========================================================
# 2. UPDATE EXISTING ORDERS + RELATED ORDER ITEMS
# =========================================================

updated_orders = []
updated_order_items = []

update_sample = orders_df.sample(
    min(NUM_UPDATED_ORDERS, len(orders_df)),
    replace=False
)

for _, row in update_sample.iterrows():

    record = row.drop(labels=["order_num"]).to_dict()

    old_status = record["order_status"]

    possible_next_status = {
        "PLACED": ["CONFIRMED", "CANCELLED"],
        "CONFIRMED": ["SHIPPED", "CANCELLED"],
        "SHIPPED": ["DELIVERED", "RETURNED"],
        "DELIVERED": ["RETURNED"],
        "CANCELLED": ["CANCELLED"],
        "RETURNED": ["RETURNED"]
    }

    new_status = random.choice(
        possible_next_status.get(old_status, [old_status])
    )

    record["order_status"] = new_status
    record["payment_status"] = get_payment_status(new_status)

    if new_status in ["DELIVERED", "RETURNED"]:
        record["delivered_date"] = current_ts()

    record["updated_at"] = current_ts()
    record["record_version"] = int(record["record_version"]) + 1
    record["operation_type"] = "UPDATE"
    record["deleted_flag"] = False

    updated_orders.append(record)

    related_items = order_items_df[
        order_items_df["order_id"] == row["order_id"]
    ].copy()

    for _, item_row in related_items.iterrows():

        item_record = item_row.drop(labels=["order_item_num"]).to_dict()

        item_record["item_status"] = get_item_status(new_status)
        item_record["updated_at"] = current_ts()
        item_record["record_version"] = int(item_record["record_version"]) + 1
        item_record["operation_type"] = "UPDATE"
        item_record["deleted_flag"] = False

        updated_order_items.append(item_record)


# =========================================================
# 3. DELETE EXISTING ORDERS + RELATED ORDER ITEMS
# =========================================================

deleted_orders = []
deleted_order_items = []

remaining_orders = orders_df.drop(update_sample.index)

delete_sample = remaining_orders.sample(
    min(NUM_DELETED_ORDERS, len(remaining_orders)),
    replace=False
)

for _, row in delete_sample.iterrows():

    record = row.drop(labels=["order_num"]).to_dict()

    record["order_status"] = "CANCELLED"
    record["payment_status"] = "REFUNDED"
    record["deleted_flag"] = True
    record["updated_at"] = current_ts()
    record["record_version"] = int(record["record_version"]) + 1
    record["operation_type"] = "DELETE"

    deleted_orders.append(record)

    related_items = order_items_df[
        order_items_df["order_id"] == row["order_id"]
    ].copy()

    for _, item_row in related_items.iterrows():

        item_record = item_row.drop(labels=["order_item_num"]).to_dict()

        item_record["item_status"] = "CANCELLED"
        item_record["deleted_flag"] = True
        item_record["updated_at"] = current_ts()
        item_record["record_version"] = int(item_record["record_version"]) + 1
        item_record["operation_type"] = "DELETE"

        deleted_order_items.append(item_record)


# =========================================================
# COMBINE
# =========================================================

orders_increment_df = pd.DataFrame(
    new_orders + updated_orders + deleted_orders
)

order_items_increment_df = pd.DataFrame(
    new_order_items + updated_order_items + deleted_order_items
)

orders_increment_df = orders_increment_df.sample(frac=1).reset_index(drop=True)
order_items_increment_df = order_items_increment_df.sample(frac=1).reset_index(drop=True)


# =========================================================
# FORMAT DATE COLUMNS
# =========================================================

order_date_cols = [
    "order_date",
    "expected_delivery_date",
    "delivered_date",
    "created_at",
    "updated_at"
]

for c in order_date_cols:
    if c in orders_increment_df.columns:
        orders_increment_df[c] = pd.to_datetime(
            orders_increment_df[c],
            errors="coerce"
        ).dt.strftime("%Y-%m-%d %H:%M:%S")

item_date_cols = [
    "created_at",
    "updated_at"
]

for c in item_date_cols:
    if c in order_items_increment_df.columns:
        order_items_increment_df[c] = pd.to_datetime(
            order_items_increment_df[c],
            errors="coerce"
        ).dt.strftime("%Y-%m-%d %H:%M:%S")


# =========================================================
# SAVE
# =========================================================

orders_increment_df.to_csv(ORDERS_OUTPUT_PATH, index=False)
order_items_increment_df.to_csv(ORDER_ITEMS_OUTPUT_PATH, index=False)


# =========================================================
# PRINT SUMMARY
# =========================================================

print("===================================")
print("orders_increment.csv generated successfully")
print("INSERT orders :", len(new_orders))
print("UPDATE orders :", len(updated_orders))
print("DELETE orders :", len(deleted_orders))
print("TOTAL orders  :", len(orders_increment_df))
print("===================================")

print("order_items_increment.csv generated successfully")
print("INSERT items :", len(new_order_items))
print("UPDATE items :", len(updated_order_items))
print("DELETE items :", len(deleted_order_items))
print("TOTAL items  :", len(order_items_increment_df))
print("===================================")