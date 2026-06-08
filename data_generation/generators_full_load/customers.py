# =========================================================
# customers_generator.py
# =========================================================

import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker("en_IN")

# =========================================================
# CONFIGURATION
# =========================================================

SOURCE_SYSTEM = "CRM_SYSTEM"

OUTPUT_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\customers.csv"

NUM_CUSTOMERS = 100000

# =========================================================
# LOCATION MAPPING
# Replace this with your own state -> city mapping if already made
# =========================================================

state_city_map = {
    "Maharashtra": ["Mumbai", "Pune", "Nagpur", "Nashik", "Aurangabad","Kolhapur"],
    "Karnataka": ["Bengaluru", "Mysuru", "Hubli", "Mangaluru", "Belgaum"],
    "Tamil Nadu": ["Chennai", "Coimbatore", "Madurai", "Salem", "Trichy"],
    "Gujarat": ["Ahmedabad", "Surat", "Vadodara", "Rajkot", "Bhavnagar"],
    "Uttar Pradesh": ["Lucknow", "Kanpur", "Noida", "Agra", "Varanasi"],
    "West Bengal": ["Kolkata", "Howrah", "Durgapur", "Siliguri", "Asansol"],
    "Telangana": ["Hyderabad", "Warangal", "Nizamabad", "Khammam", "Karimnagar"],
    "Rajasthan": ["Jaipur", "Jodhpur", "Udaipur", "Kota", "Ajmer"],
    "Kerala": ["Kochi", "Thiruvananthapuram", "Kozhikode", "Thrissur", "Kollam"],
    "Andhra Pradesh": ["Visakhapatnam", "Vijayawada", "Guntur", "Nellore", "Tirupati"],
    "Madhya Pradesh": ["Indore", "Bhopal", "Gwalior", "Jabalpur", "Ujjain"],
    "Bihar": ["Patna", "Gaya", "Bhagalpur", "Muzaffarpur", "Purnia"],
    "Punjab": ["Ludhiana", "Amritsar", "Jalandhar", "Patiala", "Bathinda"],
    "Haryana": ["Gurugram", "Faridabad", "Panipat", "Ambala", "Karnal"],
    "Odisha": ["Bhubaneswar", "Cuttack", "Rourkela", "Berhampur", "Sambalpur"],
    "Chhattisgarh": ["Raipur", "Bhilai", "Bilaspur", "Korba", "Rajnandgaon"],
    "Assam": ["Guwahati", "Dibrugarh", "Silchar", "Jorhat", "Nagaon"],
    "Jharkhand": ["Ranchi", "Jamshedpur", "Dhanbad", "Bokaro", "Deoghar"],
    "Uttarakhand": ["Dehradun", "Haridwar", "Roorkee", "Haldwani", "Rishikesh"],
    "Goa": ["Panaji", "Margao", "Vasco da Gama", "Mapusa", "Ponda"],
    "Delhi": ["Delhi"]
}

# =========================================================
# HELPER FUNCTIONS
# =========================================================

def generate_customer_id(counter):
    return f"CUST{counter:06d}"


def generate_phone():
    first_digit = random.choice(["6", "7", "8", "9"])
    remaining_digits = "".join(str(random.randint(0, 9)) for _ in range(9))
    return first_digit + remaining_digits


def generate_location():
    state = random.choice(list(state_city_map.keys()))
    city = random.choice(list(state_city_map[state]))
    postal_code = fake.postcode()
    return city, state, postal_code


def generate_metadata(signup_datetime):
    created_at = signup_datetime

    updated_at = fake.date_time_between(
        start_date=created_at,
        end_date="now"
    )

    return {
        "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at": updated_at.strftime("%Y-%m-%d %H:%M:%S"),
        "deleted_flag": random.choices(
            [False, True],
            weights=[98, 2]
        )[0],
        "source_system": SOURCE_SYSTEM,
        "record_version": random.randint(1, 5)
    }


# =========================================================
# GENERATE CUSTOMERS
# =========================================================

customers = []

for i in range(1, NUM_CUSTOMERS + 1):

    dob = fake.date_of_birth(
        minimum_age=16,
        maximum_age=80
    )

    signup_start = dob + timedelta(days=16 * 365)

    first_signup_date = fake.date_time_between(
        start_date=signup_start,
        end_date="now"
    )

    last_login_date = fake.date_time_between(
        start_date=first_signup_date,
        end_date="now"
    )

    last_purchase_date = None

    if random.random() < 0.65:
        last_purchase_date = fake.date_time_between(
            start_date=first_signup_date,
            end_date="now"
        )

    city, state, postal_code = generate_location()

    customer_status = random.choices(
        ["ACTIVE", "INACTIVE", "SUSPENDED"],
        weights=[85, 12, 3]
    )[0]

    signup_channel = random.choices(
        ["WEBSITE", "ANDROID_APP", "IOS_APP", "STORE"],
        weights=[30, 45, 20, 5]
    )[0]

    gender = random.choices(
        ["Male", "Female", "Other"],
        weights=[49, 49, 2]
    )[0]

    metadata = generate_metadata(first_signup_date)

    record = {
        "customer_id": generate_customer_id(i),

        "first_name": fake.first_name(),
        "last_name": fake.last_name(),

        "gender": gender,
        "dob": dob.strftime("%Y-%m-%d"),

        "email": fake.unique.email(),
        "phone": generate_phone(),
        "alternate_phone": generate_phone() if random.random() < 0.25 else None,

        "address": fake.street_address(),
        "city": city,
        "state": state,
        "postal_code": postal_code,
        "country": "India",

        "customer_status": customer_status,
        "signup_channel": signup_channel,
        "marketing_opt_in": random.choices(
            [True, False],
            weights=[70, 30]
        )[0],

        "first_signup_date": first_signup_date.strftime("%Y-%m-%d %H:%M:%S"),
        "last_login_date": last_login_date.strftime("%Y-%m-%d %H:%M:%S"),
        "last_purchase_date": (
            last_purchase_date.strftime("%Y-%m-%d %H:%M:%S")
            if last_purchase_date else None
        ),
        "operation_type": "FULL_LOAD",

        **metadata
    }

    # =====================================================
    # DIRTY DATA INJECTION
    # =====================================================

    # 1. Leading/trailing spaces in names
    if random.random() < 0.005:
        record["first_name"] = "  " + record["first_name"]

    if random.random() < 0.005:
        record["last_name"] = record["last_name"] + "  "

    # 2. Invalid customer_id
    if random.random() < 0.005:
        record["customer_id"] = random.choice(
            [None, "12345", "INVALID_ID"]
        )

    # 3. Missing or inconsistent gender
    if random.random() < 0.015:
        record["gender"] = random.choice(
            [None, "male", "FEMALE", "unknown"]
        )

    # 4. Null email
    if random.random() < 0.03:
        record["email"] = None

    # 5. Invalid email
    if random.random() < 0.015:
        record["email"] = random.choice(
            ["abc@", "user.com", "invalid_email", ""]
        )

    # 6. Duplicate email
    if random.random() < 0.015 and len(customers) > 0:
        record["email"] = random.choice(customers)["email"]

    # 7. Invalid phone
    if random.random() < 0.015:
        record["phone"] = random.choice(
            ["123", "abcd1234", "999"]
        )

    # 8. Future DOB
    if random.random() < 0.005:
        record["dob"] = "2030-01-01"

    # 9. Signup before DOB
    if random.random() < 0.005:
        invalid_signup = dob - timedelta(days=random.randint(100, 1000))
        record["first_signup_date"] = invalid_signup.strftime("%Y-%m-%d %H:%M:%S")

    # 10. Null address fields
    if random.random() < 0.01:
        record["city"] = None
        record["state"] = None
        record["postal_code"] = None

    customers.append(record)


# =========================================================
# CREATE DATAFRAME
# =========================================================

df = pd.DataFrame(customers)

# =========================================================
# SAVE CSV
# =========================================================

df.to_csv(OUTPUT_PATH, index=False)

print("===================================")
print("customers.csv generated successfully")
print("Total Records :", len(df))
print("===================================")