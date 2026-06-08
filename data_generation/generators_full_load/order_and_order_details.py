# =========================================================
# orders_order_items_generator.py
# =========================================================

import pandas as pd
import random
from faker import Faker
from datetime import timedelta

fake = Faker("en_IN")

SOURCE_SYSTEM = "OMS"

CUSTOMERS_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\customers.csv"
PRODUCTS_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\products.csv"
INVENTORY_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\inventory.csv"

ORDERS_OUTPUT_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\orders.csv"
ORDER_ITEMS_OUTPUT_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\order_items.csv"

NUM_ORDERS = 100000

# =========================================================
# LOAD DATA
# =========================================================

customers_df = pd.read_csv(CUSTOMERS_PATH)
products_df = pd.read_csv(PRODUCTS_PATH)
inventory_df = pd.read_csv(INVENTORY_PATH)

customers_df = customers_df[
    (customers_df["deleted_flag"] == False) &
    (customers_df["customer_status"] == "ACTIVE") &
    (customers_df["customer_id"].notna())
]

products_df = products_df[
    (products_df["deleted_flag"] == False) &
    (products_df["product_status"] != "DISCONTINUED")
]

inventory_df = inventory_df[
    (inventory_df["deleted_flag"] == False) &
    (inventory_df["warehouse_id"].notna()) &
    (inventory_df["available_quantity"] > 0)
]

customer_records = customers_df[
    ["customer_id", "city", "state", "postal_code"]
].to_dict("records")

product_records = products_df[
    ["product_id", "selling_price"]
].to_dict("records")

inventory_map = (
    inventory_df
    .groupby("product_id")["warehouse_id"]
    .apply(list)
    .to_dict()
)

# =========================================================
# HELPER FUNCTIONS
# =========================================================

def generate_order_id(counter):
    return f"ORD{counter:08d}"


def generate_order_item_id(counter):
    return f"OI{counter:09d}"


def get_payment_status(order_status):
    mapping = {
        "PLACED": "PENDING",
        "CONFIRMED": "SUCCESS",
        "SHIPPED": "SUCCESS",
        "DELIVERED": "SUCCESS",
        "CANCELLED": random.choice(["FAILED", "REFUNDED"]),
        "RETURNED": "REFUNDED"
    }
    return mapping[order_status]


def get_item_status(order_status):
    mapping = {
        "PLACED": "PROCESSING",
        "CONFIRMED": "PROCESSING",
        "SHIPPED": "SHIPPED",
        "DELIVERED": "DELIVERED",
        "CANCELLED": "CANCELLED",
        "RETURNED": "RETURNED"
    }
    return mapping[order_status]


# =========================================================
# GENERATE ORDER_ITEMS FIRST
# =========================================================

order_items = []
order_item_counter = 1

for order_counter in range(1, NUM_ORDERS + 1):

    order_id = generate_order_id(order_counter)

    customer = random.choice(customer_records)

    order_date = fake.date_time_between(
        start_date="-2y",
        end_date="now"
    )

    order_status = random.choices(
        ["PLACED", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"],
        weights=[5, 10, 15, 55, 10, 5]
    )[0]

    payment_status = get_payment_status(order_status)

    num_items = random.choices(
        [1, 2, 3, 4],
        weights=[55, 25, 15, 5]
    )[0]

    selected_products = random.sample(
        product_records,
        min(num_items, len(product_records))
    )

    for product in selected_products:

        product_id = product["product_id"]

        if product_id not in inventory_map:
            continue

        warehouse_id = random.choice(inventory_map[product_id])

        quantity = random.choices(
            [1, 2, 3, 4, 5],
            weights=[70, 18, 7, 3, 2]
        )[0]

        unit_price = round(float(product["selling_price"]), 2)

        gross_amount = round(quantity * unit_price, 2)

        discount_amount = round(
            gross_amount * random.uniform(0.0, 0.20),
            2
        )

        final_price = round(gross_amount - discount_amount, 2)

        updated_at = fake.date_time_between(
            start_date=order_date,
            end_date="now"
        )

        record = {
            "order_item_id": generate_order_item_id(order_item_counter),
            "order_id": order_id,
            "customer_id": customer["customer_id"],

            "product_id": product_id,
            "warehouse_id": warehouse_id,

            "quantity": quantity,
            "unit_price": unit_price,
            "gross_amount": gross_amount,
            "discount_amount": discount_amount,
            "final_price": final_price,

            "item_status": get_item_status(order_status),
            "estimated_shipping_cost": round(random.uniform(30, 500), 2),

            "order_date": order_date.strftime("%Y-%m-%d %H:%M:%S"),
            "order_status": order_status,
            "payment_status": payment_status,

            "shipping_city": customer["city"],
            "shipping_state": customer["state"],
            "shipping_postal_code": customer["postal_code"],

            "created_at": order_date.strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at": updated_at.strftime("%Y-%m-%d %H:%M:%S"),
            "deleted_flag": False,
            "source_system": SOURCE_SYSTEM,
            "record_version": random.randint(1, 5),
            "operation_type": "FULL_LOAD",
        }

        # small dirty data injection
        if random.random() < 0.002:
            record["quantity"] = -1

        if random.random() < 0.002:
            record["product_id"] = "INVALID_PRODUCT"

        if random.random() < 0.002:
            record["final_price"] = -100

        order_items.append(record)
        order_item_counter += 1


order_items_df = pd.DataFrame(order_items)

# =========================================================
# CREATE ORDERS FROM ORDER_ITEMS
# =========================================================

order_summary_df = (
    order_items_df
    .assign(
        item_total_amount=lambda df: df["quantity"] * df["unit_price"]
    )
    .groupby("order_id")
    .agg(
        customer_id=("customer_id", "first"),
        order_date=("order_date", "first"),

        total_amount=("item_total_amount", "sum"),
        total_discount_amount=("discount_amount", "sum"),
        final_payable_amount=("final_price", "sum"),

        order_status=("order_status", "first"),
        payment_status=("payment_status", "first"),

        shipping_city=("shipping_city", "first"),
        shipping_state=("shipping_state", "first"),
        shipping_postal_code=("shipping_postal_code", "first"),

        created_at=("created_at", "first"),
        updated_at=("updated_at", "max"),
        deleted_flag=("deleted_flag", "first"),
        source_system=("source_system", "first"),
        record_version=("record_version", "max")
    )
    .reset_index()
)

# =========================================================
# ADD ORDER-LEVEL FIELDS
# =========================================================

order_summary_df["expected_delivery_date"] = pd.to_datetime(
    order_summary_df["order_date"]
) + pd.to_timedelta(
    [random.randint(2, 10) for _ in range(len(order_summary_df))],
    unit="D"
)

order_summary_df["delivered_date"] = None

delivered_mask = order_summary_df["order_status"].isin(
    ["DELIVERED", "RETURNED"]
)

order_summary_df.loc[delivered_mask, "delivered_date"] = (
    order_summary_df.loc[delivered_mask, "expected_delivery_date"]
    + pd.to_timedelta(
        [random.randint(0, 3) for _ in range(delivered_mask.sum())],
        unit="D"
    )
)

order_summary_df["order_source"] = random.choices(
    ["ANDROID_APP", "IOS_APP", "WEBSITE"],
    weights=[55, 20, 25],
    k=len(order_summary_df)
)

order_summary_df["device_type"] = random.choices(
    ["MOBILE", "DESKTOP", "TABLET"],
    weights=[75, 20, 5],
    k=len(order_summary_df)
)

# round amount fields
order_summary_df["total_amount"] = order_summary_df["total_amount"].round(2)
order_summary_df["total_discount_amount"] = order_summary_df["total_discount_amount"].round(2)
order_summary_df["final_payable_amount"] = order_summary_df["final_payable_amount"].round(2)

# format dates
order_summary_df["order_date"] = pd.to_datetime(
    order_summary_df["order_date"],
    errors="coerce"
).dt.strftime("%Y-%m-%d %H:%M:%S")

order_summary_df["expected_delivery_date"] = pd.to_datetime(
    order_summary_df["expected_delivery_date"],
    errors="coerce"
).dt.strftime("%Y-%m-%d %H:%M:%S")

order_summary_df["delivered_date"] = pd.to_datetime(
    order_summary_df["delivered_date"],
    errors="coerce"
).dt.strftime("%Y-%m-%d %H:%M:%S")

order_summary_df["created_at"] = pd.to_datetime(
    order_summary_df["created_at"],
    errors="coerce"
).dt.strftime("%Y-%m-%d %H:%M:%S")

order_summary_df["updated_at"] = pd.to_datetime(
    order_summary_df["updated_at"],
    errors="coerce"
).dt.strftime("%Y-%m-%d %H:%M:%S")

order_summary_df["delivered_date"] = order_summary_df["delivered_date"].replace("None", None)

order_summary_df["operation_type"] = "FULL_LOAD"

# =========================================================
# FINAL ORDERS COLUMN ORDER
# =========================================================

orders_df = order_summary_df[
    [
        "order_id",
        "customer_id",
        "order_date",
        "expected_delivery_date",
        "delivered_date",

        "total_amount",
        "total_discount_amount",
        "final_payable_amount",

        "order_status",
        "payment_status",

        "shipping_city",
        "shipping_state",
        "shipping_postal_code",

        "order_source",
        "device_type",

        "created_at",
        "updated_at",
        "deleted_flag",
        "source_system",
        "record_version"
    ]
]

# =========================================================
# REMOVE ORDER-LEVEL COLUMNS FROM ORDER_ITEMS
# optional: keep order_items clean
# =========================================================

order_items_df = order_items_df[
    [
        "order_item_id",
        "order_id",
        "product_id",
        "warehouse_id",

        "quantity",
        "unit_price",
        "gross_amount",
        "discount_amount",
        "final_price",

        "item_status",
        "estimated_shipping_cost",

        "created_at",
        "updated_at",
        "deleted_flag",
        "source_system",
        "record_version"
    ]
]

# =========================================================
# SAVE FILES
# =========================================================

orders_df.to_csv(ORDERS_OUTPUT_PATH, index=False)
order_items_df.to_csv(ORDER_ITEMS_OUTPUT_PATH, index=False)

print("===================================")
print("orders.csv and order_items.csv generated successfully")
print("Orders Records      :", len(orders_df))
print("Order Items Records :", len(order_items_df))
print("===================================")