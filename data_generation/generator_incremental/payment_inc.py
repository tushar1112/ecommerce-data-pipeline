# =========================================================
# payments_incremental_generator.py
# =========================================================

import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker("en_IN")

# =========================================================
# CONFIG
# =========================================================

BASE_PAYMENTS_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\payments.csv"
BASE_ORDERS_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\orders.csv"
ORDERS_INCREMENT_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\incremental\orders.csv"

OUTPUT_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\incremental\payments.csv"

SOURCE_SYSTEM = "PAYMENT_GATEWAY"

NUM_UPDATES = 80
NUM_DELETES = 10

batch_datetime = datetime.now()

# =========================================================
# LOAD DATA
# =========================================================

payments_df = pd.read_csv(BASE_PAYMENTS_PATH)
orders_df = pd.read_csv(BASE_ORDERS_PATH)
orders_inc_df = pd.read_csv(ORDERS_INCREMENT_PATH)

payments_df = payments_df[
    (payments_df["payment_id"].notna()) &
    (payments_df["deleted_flag"] == False)
].copy()

orders_df = orders_df[
    (orders_df["order_id"].notna()) &
    (orders_df["deleted_flag"] == False)
].copy()

# =========================================================
# CLEAN VALID PAYMENT IDS
# =========================================================

payments_df["payment_num"] = pd.to_numeric(
    payments_df["payment_id"].astype(str).str.extract(r"PAY(\d+)")[0],
    errors="coerce"
)

payments_df = payments_df[
    payments_df["payment_num"].notna()
].copy()

payments_df["payment_num"] = payments_df["payment_num"].astype(int)

max_payment_num = payments_df["payment_num"].max()

# =========================================================
# HELPERS
# =========================================================

def generate_payment_id(num):
    return f"PAY{num:09d}"


def current_ts():
    return batch_datetime.strftime("%Y-%m-%d %H:%M:%S")


def generate_transaction_reference():
    return "TXN" + str(random.randint(10**11, 10**12 - 1))


def choose_payment_method():
    return random.choice(["UPI", "CARD", "NET_BANKING", "WALLET", "COD"])


def choose_provider(payment_method):
    providers = {
        "UPI": ["PhonePe", "GooglePay", "Paytm"],
        "CARD": ["Razorpay", "PayU", "Stripe"],
        "NET_BANKING": ["BillDesk", "PayU"],
        "WALLET": ["Paytm", "AmazonPay"],
        "COD": ["COD"]
    }
    return random.choice(providers[payment_method])


def payment_status_from_order_status(order_status):
    mapping = {
        "PLACED": "PENDING",
        "CONFIRMED": "SUCCESS",
        "SHIPPED": "SUCCESS",
        "DELIVERED": "SUCCESS",
        "CANCELLED": "REFUNDED",
        "RETURNED": "REFUNDED"
    }
    return mapping.get(order_status, "PENDING")


# =========================================================
# 1. INSERT PAYMENTS FOR NEW ORDERS
# =========================================================

new_payment_records = []

new_orders_df = orders_inc_df[
    (orders_inc_df["operation_type"] == "INSERT") &
    (orders_inc_df["deleted_flag"] == False)
].copy()

payment_counter = max_payment_num + 1

for _, order in new_orders_df.iterrows():

    payment_id = generate_payment_id(payment_counter)
    payment_counter += 1

    payment_method = choose_payment_method()
    payment_provider = choose_provider(payment_method)

    payment_status = payment_status_from_order_status(order["order_status"])

    payment_amount = round(float(order["final_payable_amount"]), 2)

    refund_amount = 0.0
    refund_date = None

    if payment_status == "REFUNDED":
        refund_amount = payment_amount
        refund_date = current_ts()

    payment_date = pd.to_datetime(
        order["order_date"],
        errors="coerce"
    )

    if pd.isna(payment_date):
        payment_date = batch_datetime

    payment_date = payment_date + timedelta(minutes=random.randint(1, 30))

    record = {
        "payment_id": payment_id,
        "order_id": order["order_id"],
        "customer_id": order["customer_id"],

        "payment_date": payment_date.strftime("%Y-%m-%d %H:%M:%S"),

        "payment_method": payment_method,
        "payment_provider": payment_provider,

        "payment_amount": payment_amount,
        "currency": "INR",

        "payment_status": payment_status,

        "transaction_reference_id": (
            generate_transaction_reference()
            if payment_status in ["SUCCESS", "REFUNDED"]
            else None
        ),

        "refund_amount": refund_amount,
        "refund_date": refund_date,

        "created_at": current_ts(),
        "updated_at": current_ts(),
        "deleted_flag": False,
        "source_system": SOURCE_SYSTEM,
        "record_version": 1,

        "operation_type": "INSERT"
    }

    new_payment_records.append(record)

# =========================================================
# 2. UPDATE EXISTING PAYMENTS
# =========================================================

update_records = []

update_sample = payments_df.sample(
    min(NUM_UPDATES, len(payments_df)),
    replace=False
)

for _, row in update_sample.iterrows():

    record = row.drop(labels=["payment_num"]).to_dict()

    old_status = str(record["payment_status"]).strip().upper()

    if old_status == "PENDING":
        new_status = random.choice(["SUCCESS", "FAILED"])

    elif old_status == "SUCCESS":
        new_status = random.choices(
            ["SUCCESS", "REFUNDED", "PARTIALLY_REFUNDED"],
            weights=[80, 10, 10]
        )[0]

    elif old_status == "FAILED":
        new_status = random.choice(["FAILED", "SUCCESS"])

    else:
        new_status = old_status

    record["payment_status"] = new_status

    payment_amount = float(record["payment_amount"])

    if new_status == "SUCCESS":
        record["transaction_reference_id"] = (
            record["transaction_reference_id"]
            if pd.notna(record["transaction_reference_id"])
            else generate_transaction_reference()
        )
        record["refund_amount"] = 0.0
        record["refund_date"] = None

    elif new_status == "FAILED":
        record["transaction_reference_id"] = None
        record["refund_amount"] = 0.0
        record["refund_date"] = None

    elif new_status == "REFUNDED":
        record["refund_amount"] = payment_amount
        record["refund_date"] = current_ts()

    elif new_status == "PARTIALLY_REFUNDED":
        record["refund_amount"] = round(
            payment_amount * random.uniform(0.1, 0.8),
            2
        )
        record["refund_date"] = current_ts()

    record["updated_at"] = current_ts()
    record["record_version"] = int(record["record_version"]) + 1
    record["deleted_flag"] = False
    record["operation_type"] = "UPDATE"

    update_records.append(record)

# =========================================================
# 3. DELETE / VOID PAYMENTS
# =========================================================

delete_records = []

remaining_payments = payments_df.drop(update_sample.index)

delete_sample = remaining_payments.sample(
    min(NUM_DELETES, len(remaining_payments)),
    replace=False
)

for _, row in delete_sample.iterrows():

    record = row.drop(labels=["payment_num"]).to_dict()

    record["payment_status"] = "VOIDED"
    record["deleted_flag"] = True
    record["updated_at"] = current_ts()
    record["record_version"] = int(record["record_version"]) + 1
    record["operation_type"] = "DELETE"

    delete_records.append(record)

# =========================================================
# COMBINE
# =========================================================

payments_increment_df = pd.DataFrame(
    new_payment_records + update_records + delete_records
)

payments_increment_df = payments_increment_df.sample(
    frac=1
).reset_index(drop=True)

# =========================================================
# FORMAT DATES
# =========================================================

date_cols = [
    "payment_date",
    "refund_date",
    "created_at",
    "updated_at"
]

for c in date_cols:
    if c in payments_increment_df.columns:
        payments_increment_df[c] = pd.to_datetime(
            payments_increment_df[c],
            errors="coerce"
        ).dt.strftime("%Y-%m-%d %H:%M:%S")

# =========================================================
# SAVE
# =========================================================

payments_increment_df.to_csv(OUTPUT_PATH, index=False)

# =========================================================
# SUMMARY
# =========================================================

print("===================================")
print("payments_increment.csv generated successfully")
print("INSERT payments :", len(new_payment_records))
print("UPDATE payments :", len(update_records))
print("DELETE payments :", len(delete_records))
print("TOTAL payments  :", len(payments_increment_df))
print("===================================")