# =========================================================
# payments_generator.py
# =========================================================

import pandas as pd
import random
from faker import Faker
from datetime import timedelta

fake = Faker("en_IN")

SOURCE_SYSTEM = "PAYMENT_GATEWAY"

ORDERS_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\orders.csv"
OUTPUT_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\payments.csv"

orders_df = pd.read_csv(ORDERS_PATH)

orders_df = orders_df[
    (orders_df["deleted_flag"] == False) &
    (orders_df["order_id"].notna()) &
    (orders_df["customer_id"].notna())
]

orders_records = orders_df[
    [
        "order_id",
        "customer_id",
        "order_date",
        "final_payable_amount",
        "order_status",
        "payment_status"
    ]
].to_dict("records")

payments = []
payment_counter = 1


def generate_payment_id(counter):
    return f"PAY{counter:09d}"


def generate_transaction_ref():
    return "TXN" + str(random.randint(1000000000, 9999999999))


def get_payment_method():
    return random.choices(
        ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NET_BANKING", "WALLET", "CASH_ON_DELIVERY"],
        weights=[35, 20, 18, 10, 10, 7]
    )[0]


def get_payment_provider(payment_method):
    providers = {
        "UPI": ["PhonePe", "Google Pay", "Paytm", "BHIM"],
        "CREDIT_CARD": ["HDFC Bank", "ICICI Bank", "Axis Bank", "SBI Card"],
        "DEBIT_CARD": ["HDFC Bank", "ICICI Bank", "Axis Bank", "SBI"],
        "NET_BANKING": ["HDFC NetBanking", "ICICI NetBanking", "SBI NetBanking"],
        "WALLET": ["Paytm Wallet", "Amazon Pay", "Mobikwik"],
        "CASH_ON_DELIVERY": ["COD"]
    }
    return random.choice(providers[payment_method])


for order in orders_records:

    order_date = pd.to_datetime(order["order_date"])
    final_amount = round(float(order["final_payable_amount"]), 2)

    payment_status = order["payment_status"]

    # Most orders have 1 payment attempt.
    # Some failed orders can have multiple attempts.
    num_attempts = 1

    if payment_status in ["FAILED", "PENDING"] and random.random() < 0.25:
        num_attempts = random.choice([2, 3])

    for attempt in range(1, num_attempts + 1):

        payment_method = get_payment_method()
        payment_provider = get_payment_provider(payment_method)

        payment_date = order_date + timedelta(
            minutes=random.randint(1, 120)
        )

        refund_amount = 0
        refund_date = None

        current_payment_status = payment_status

        if attempt < num_attempts:
            current_payment_status = "FAILED"

        if payment_status == "REFUNDED":
            refund_amount = final_amount
            refund_date = payment_date + timedelta(
                days=random.randint(1, 15)
            )

        payment_amount = final_amount

        if current_payment_status == "FAILED":
            payment_amount = 0

        record = {
            "payment_id": generate_payment_id(payment_counter),
            "order_id": order["order_id"],
            "customer_id": order["customer_id"],

            "payment_date": payment_date.strftime("%Y-%m-%d %H:%M:%S"),
            "payment_method": payment_method,
            "payment_provider": payment_provider,

            "payment_amount": payment_amount,
            "currency": "INR",

            "payment_status": current_payment_status,
            "transaction_reference_id": generate_transaction_ref(),

            "refund_amount": refund_amount,
            "refund_date": (
                refund_date.strftime("%Y-%m-%d %H:%M:%S")
                if refund_date else None
            ),

            "created_at": payment_date.strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at": (
                refund_date.strftime("%Y-%m-%d %H:%M:%S")
                if refund_date else payment_date.strftime("%Y-%m-%d %H:%M:%S")
            ),
            "deleted_flag": False,
            "source_system": SOURCE_SYSTEM,
            "record_version": random.randint(1, 4),
            "operation_type": "FULL_LOAD",
        }

        # Dirty data injection
        if random.random() < 0.002:
            record["payment_amount"] = -100

        if random.random() < 0.002:
            record["transaction_reference_id"] = None

        if random.random() < 0.002:
            record["payment_status"] = "UNKNOWN"

        payments.append(record)
        payment_counter += 1


payments_df = pd.DataFrame(payments)

payments_df.to_csv(OUTPUT_PATH, index=False)

print("===================================")
print("payments.csv generated successfully")
print("Total Records :", len(payments_df))
print("===================================")