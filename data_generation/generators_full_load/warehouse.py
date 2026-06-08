# =========================================================
# warehouses_generator.py
# =========================================================

# importing required libraries

import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker('en_IN')

# =========================================================
# CONFIGURATION
# =========================================================

SOURCE_SYSTEM = "WMS"

OUTPUT_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\warehouses.csv"

NUM_WAREHOUSES = 15

# =========================================================
# LOCATION MAPPING
# =========================================================

location_mapping = {

    "Mumbai": {
        "state": "Maharashtra",
        "postal_codes": ["400001", "400002", "400003"]
    },

    "Pune": {
        "state": "Maharashtra",
        "postal_codes": ["411001", "411002"]
    },

    "Bangalore": {
        "state": "Karnataka",
        "postal_codes": ["560001", "560002"]
    },

    "Hyderabad": {
        "state": "Telangana",
        "postal_codes": ["500001", "500002"]
    },

    "Chennai": {
        "state": "Tamil Nadu",
        "postal_codes": ["600001", "600002"]
    },

    "Delhi": {
        "state": "Delhi",
        "postal_codes": ["110001", "110002"]
    },

    "Kolkata": {
        "state": "West Bengal",
        "postal_codes": ["700001", "700002"]
    },

    "Ahmedabad": {
        "state": "Gujarat",
        "postal_codes": ["380001", "380002"]
    },

    "Lucknow": {
        "state": "Uttar Pradesh",
        "postal_codes": ["226001", "226002"]
    },

    "Bhopal": {
        "state": "Madhya Pradesh",
        "postal_codes": ["462001", "462002"]
    }
}

# =========================================================
# WAREHOUSE TYPES
# =========================================================

warehouse_types = [

    "Fulfillment Center",

    "Regional Warehouse",

    "Sortation Center",

    "Dark Store"
]

# =========================================================
# HELPER VARIABLES
# =========================================================

warehouses = []

warehouse_counter = 1

# =========================================================
# GENERATE WAREHOUSE ID
# =========================================================

def generate_warehouse_id(counter):

    return f"WH{counter:04d}"

# =========================================================
# GENERATE PHONE NUMBER
# =========================================================

def generate_phone():

    first_digit = random.choice(['6', '7', '8', '9'])

    remaining_digits = ''.join(
        [str(random.randint(0, 9)) for _ in range(9)]
    )

    return first_digit + remaining_digits

# =========================================================
# GENERATE METADATA
# =========================================================

def generate_metadata():

    created_at = fake.date_time_between(
        start_date='-5y',
        end_date='-1y'
    )

    updated_at = fake.date_time_between(
        start_date=created_at,
        end_date='now'
    )

    return {

        "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),

        "updated_at": updated_at.strftime("%Y-%m-%d %H:%M:%S"),

        "deleted_flag": random.choices(
            [False, True],
            weights=[98, 2]
        )[0],

        "source_system": SOURCE_SYSTEM,

        "record_version": random.randint(1, 3)
    }

# =========================================================
# GENERATE WAREHOUSE RECORDS
# =========================================================

cities = list(location_mapping.keys())

for _ in range(NUM_WAREHOUSES):

    city = random.choice(cities)

    state = location_mapping[city]["state"]

    postal_code = random.choice(
        location_mapping[city]["postal_codes"]
    )

    warehouse_type = random.choice(warehouse_types)

    warehouse_id = generate_warehouse_id(
        warehouse_counter
    )

    warehouse_name = f"{city} {warehouse_type}"

    metadata = generate_metadata()

    warehouse_record = {

        "warehouse_id": warehouse_id,

        "warehouse_name": warehouse_name,

        "warehouse_type": warehouse_type,

        "city": city,

        "state": state,

        "postal_code": postal_code,

        "warehouse_capacity": random.randint(
            5000,
            50000
        ),

        "manager_name": fake.name(),

        "contact_number": generate_phone(),

        "is_active": random.choices(
            [True, False],
            weights=[95, 5]
        )[0],

        "operation_type": "FULL_LOAD",

        **metadata
    }

    warehouses.append(warehouse_record)

    warehouse_counter += 1

# =========================================================
# CREATE DATAFRAME
# =========================================================

df = pd.DataFrame(warehouses)

# =========================================================
# DIRTY DATA INJECTION
# =========================================================

for index in df.index:

    # 1. Trailing spaces in warehouse_name
    if random.random() < 0.02:

        df.at[index, "warehouse_name"] = (
            str(df.at[index, "warehouse_name"]) + "  "
        )

    # 2. Invalid contact number
    if random.random() < 0.01:

        df.at[index, "contact_number"] = random.choice([
            "123",
            "INVALID_PHONE",
            "99999"
        ])

    # 3. Null manager name
    if random.random() < 0.01:

        df.at[index, "manager_name"] = None

# =========================================================
# SAVE CSV
# =========================================================

df.to_csv(OUTPUT_PATH, index=False)

# =========================================================
# SUCCESS MESSAGE
# =========================================================

print("===================================")
print("warehouses.csv generated successfully")
print("Total Records :", len(df))
print("===================================")