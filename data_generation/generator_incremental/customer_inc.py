import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker("en_IN")

# =========================================================
# CONFIG
# =========================================================

BASE_CUSTOMERS_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\customers.csv"

OUTPUT_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\incremental\customers.csv"

SOURCE_SYSTEM = "CRM_SYSTEM"

NUM_INSERTS = 50
NUM_UPDATES = 100
NUM_DELETES = 20

batch_datetime = datetime.now()

# =========================================================
# LOCATION MAPPING
# Replace with your actual state-city mapping
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


def generate_phone():
    return random.choice(["6", "7", "8", "9"]) + "".join(
        str(random.randint(0, 9)) for _ in range(9)
    )


def generate_location():
    state = random.choice(list(state_city_map.keys()))
    city = random.choice(state_city_map[state])
    postal_code = fake.postcode()
    return city, state, postal_code


# =========================================================
# LOAD BASE CUSTOMERS
# =========================================================

customers_df = pd.read_csv(BASE_CUSTOMERS_PATH)

customers_df = customers_df[
    (customers_df["customer_id"].notna()) &
    (customers_df["deleted_flag"] == False)
].copy()

# =========================================================
# FIND NEXT CUSTOMER ID
# =========================================================

customers_df = customers_df[
    customers_df["customer_id"].str.match(r"^CUST\d+$", na=False)
].copy()

customers_df["customer_num"] = (
    customers_df["customer_id"]
    .str.replace("CUST", "", regex=False)
    .astype(int)
)

max_customer_num = customers_df["customer_num"].max()

# =========================================================
# INSERT RECORDS
# =========================================================

insert_records = []

for i in range(1, NUM_INSERTS + 1):

    new_customer_num = max_customer_num + i
    customer_id = f"CUST{new_customer_num:06d}"

    dob = fake.date_of_birth(minimum_age=18, maximum_age=75)

    signup_date = batch_datetime - timedelta(
        minutes=random.randint(1, 1440)
    )

    city, state, postal_code = generate_location()

    record = {
        "customer_id": customer_id,
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "gender": random.choice(["Male", "Female", "Other"]),
        "dob": dob.strftime("%Y-%m-%d"),
        "email": fake.unique.email(),
        "phone": generate_phone(),
        "alternate_phone": generate_phone() if random.random() < 0.25 else None,

        "address": fake.street_address(),
        "city": city,
        "state": state,
        "postal_code": postal_code,
        "country": "India",

        "customer_status": "ACTIVE",
        "signup_channel": random.choice(["WEBSITE", "ANDROID_APP", "IOS_APP"]),
        "marketing_opt_in": random.choice([True, False]),

        "first_signup_date": signup_date.strftime("%Y-%m-%d %H:%M:%S"),
        "last_login_date": signup_date.strftime("%Y-%m-%d %H:%M:%S"),
        "last_purchase_date": None,

        "created_at": signup_date.strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at": batch_datetime.strftime("%Y-%m-%d %H:%M:%S"),
        "deleted_flag": False,
        "source_system": SOURCE_SYSTEM,
        "record_version": 1,

        "operation_type": "INSERT"
    }

    insert_records.append(record)

# =========================================================
# UPDATE RECORDS
# =========================================================

update_records = []

update_sample = customers_df.sample(NUM_UPDATES)

for _, row in update_sample.iterrows():

    record = row.drop(labels=["customer_num"]).to_dict()

    update_type = random.choice([
        "email",
        "phone",
        "address",
        "marketing",
        "last_login"
    ])

    if update_type == "email":
        record["email"] = fake.unique.email()

    elif update_type == "phone":
        record["phone"] = generate_phone()

    elif update_type == "address":
        city, state, postal_code = generate_location()
        record["address"] = fake.street_address()
        record["city"] = city
        record["state"] = state
        record["postal_code"] = postal_code

    elif update_type == "marketing":
        record["marketing_opt_in"] = not bool(record["marketing_opt_in"])

    elif update_type == "last_login":
        record["last_login_date"] = batch_datetime.strftime("%Y-%m-%d %H:%M:%S")

    record["updated_at"] = batch_datetime.strftime("%Y-%m-%d %H:%M:%S")
    record["record_version"] = int(record["record_version"]) + 1
    record["deleted_flag"] = False
    record["operation_type"] = "UPDATE"

    update_records.append(record)

# =========================================================
# DELETE RECORDS - SOFT DELETE
# =========================================================

delete_records = []

delete_sample = customers_df.sample(NUM_DELETES)

for _, row in delete_sample.iterrows():

    record = row.drop(labels=["customer_num"]).to_dict()

    record["customer_status"] = "INACTIVE"
    record["deleted_flag"] = True
    record["updated_at"] = batch_datetime.strftime("%Y-%m-%d %H:%M:%S")
    record["record_version"] = int(record["record_version"]) + 1
    record["operation_type"] = "DELETE"

    delete_records.append(record)

# =========================================================
# COMBINE CDC RECORDS
# =========================================================

incremental_df = pd.DataFrame(
    insert_records + update_records + delete_records
)

# Optional shuffle to look realistic
incremental_df = incremental_df.sample(frac=1).reset_index(drop=True)

# =========================================================
# SAVE
# =========================================================

incremental_df.to_csv(OUTPUT_PATH, index=False)

print("===================================")
print("customers_increment.csv generated successfully")
print("INSERT records :", len(insert_records))
print("UPDATE records :", len(update_records))
print("DELETE records :", len(delete_records))
print("TOTAL records  :", len(incremental_df))
print("===================================")