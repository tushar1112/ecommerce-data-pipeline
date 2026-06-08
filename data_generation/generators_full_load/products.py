# =========================================================
# products_generator.py
# =========================================================

# =========================================================
# IMPORT REQUIRED LIBRARIES
# =========================================================

import pandas as pd
import random
from faker import Faker
from datetime import datetime

fake = Faker('en_IN')

# =========================================================
# CONFIGURATION
# =========================================================

SOURCE_SYSTEM = "PRODUCT_CATALOG_SYSTEM"

OUTPUT_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\products.csv"

NUM_PRODUCTS = 20000

# =========================================================
# LOAD MASTER TABLES
# =========================================================

# Load categories
categories_df = pd.read_csv(
    r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\categories.csv"
)

# Load suppliers
suppliers_df = pd.read_csv(
    r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\suppliers.csv"
)

# =========================================================
# KEEP ONLY ACTIVE CATEGORIES
# =========================================================

categories_df = categories_df[
    categories_df["is_active"] == True
]

# =========================================================
# PRODUCT TEMPLATES
# =========================================================

# Category Name → Brands + Product Patterns

product_templates = {

    "Mobiles": {

        "brands": [
            "Samsung",
            "Apple",
            "OnePlus",
            "Xiaomi",
            "Realme",
            "Motorola"
        ],

        "patterns": [
            "Galaxy s17","Galaxy s17 pro",
            "iPhone 12","iPhone 16","iPhone 15 pro",
            "Nord",
            "Redmi",
            "Note",
            "infinity",
            "Z fold"
        ]
    },

    "Laptops": {

        "brands": [
            "Dell",
            "HP",
            "Lenovo",
            "ASUS",
            "Acer",
            "Apple",
            "MSI"
        ],

        "patterns": [
            "Predator",
            "ROG",
            "Victus",
            "Legion",
            "Inspiron",
            "Pavallion"
        ]
    },

    "Headphones": {

        "brands": [
            "Boat",
            "Sony",
            "JBL",
            "Realme",
            "Noise"
        ],

        "patterns": [
            "BassX",
            "AirBeat",
            "SoundPro",
            "Wave",
            "Pods"
        ]
    },

    "Men Clothing": {

        "brands": [
            "Levis",
            "Wrangler",
            "Pepe Jeans",
            "Spykar",
            "HRX",
            "Peter England",
            "H&M"
        ],

        "patterns": [
            "Slim Fit",
            "Regular Fit",
            "Relaxed Fit"
        ]
    },

    "Women Clothing": {

        "brands": [
            "Levis",
            "Wrangler",
            "Pepe Jeans",
            "Spykar",
            "HRX",
            "Peter England",
            "H&M"
        ],

        "patterns": [
            "Slim Fit",
            "Regular Fit",
            "Relaxed Fit"
        ]
    },

    "Footwear": {

        "brands": [
            "Nike",
            "Adidas",
            "Puma",
            "Reebok",
            "Bata"
        ],

        "patterns": [
            "Runner",
            "AirMax",
            "Sprint",
            "Zoom"
        ]
    },

    "Skincare": {
        "brands": [
            "Lakmé",
            "Mamaearth",
            "Nykaa Cosmetics",
            "Lotus Herbals",
            "The Derma Co"
        ],
        "patterns": [
            "Matte",
            "Dewy Glow",
            "Waterproof Gel",
            "Vitamin C Enriched",
            "Sunscream s 50",
            "Acne cream",
            "Sheet-mask"
        ]
    },

    "Haircare": {
        "brands": [
            "Cetaphil",
            "Mamaearth",
            "Clinic plus",
            "Head & shoulder",
            "The Derma Co",
            "Johnson & Johnson"
        ],
        "patterns": [
            "Anti-dendruff",
            "onion shampoo",
            "body gel"
        ]
    },

    "Fitness Equipment": {
        "brands": [
            "Cosco",
            "Alphax",
            "Yonex",
            "Nivia",
            "Adidas",
        ],
        "patterns": [
            "A13","B62",
            "C83","pro-series","Max-repultion"
        ]
    },
}

# =========================================================
# HELPER VARIABLES
# =========================================================

products = []

product_counter = 1

# =========================================================
# GENERATE PRODUCT ID
# =========================================================

def generate_product_id(counter):

    return f"PROD{counter:06d}"

# =========================================================
# GENERATE SKU
# =========================================================

def generate_sku(brand, category):

    brand_part = brand[:3].upper()

    category_part = category[:3].upper()

    random_number = random.randint(1000, 9999)

    return f"SKU-{brand_part}-{category_part}-{random_number}"

# =========================================================
# GENERATE PRICE DETAILS
# =========================================================

def generate_pricing():

    mrp = random.randint(500, 150000)

    discount_percent = random.randint(5, 40)

    selling_price = mrp - (
        mrp * discount_percent / 100
    )

    cost_price = round(
        selling_price * random.uniform(0.6, 0.85),
        2
    )

    return (

        round(mrp, 2),

        round(selling_price, 2),

        round(cost_price, 2)
    )

# =========================================================
# GENERATE PRODUCT RATING
# =========================================================

def generate_rating():

    return round(
        random.uniform(2.5, 5.0),
        1
    )

# =========================================================
# GENERATE METADATA
# =========================================================

def generate_metadata():

    created_at = fake.date_time_between(
        start_date='-3y',
        end_date='-1y'
    )

    updated_at = fake.date_time_between(
        start_date=created_at,
        end_date='now'
    )

    return {

        "created_at": created_at.strftime(
            "%Y-%m-%d %H:%M:%S"
        ),

        "updated_at": updated_at.strftime(
            "%Y-%m-%d %H:%M:%S"
        ),

        "deleted_flag": random.choices(
            [False, True],
            weights=[98, 2]
        )[0],

        "source_system": SOURCE_SYSTEM,

        "record_version": random.randint(1, 5)
    }

# =========================================================
# FILTER LEVEL-2 AND LEVEL-3 CATEGORIES
# =========================================================

valid_categories = categories_df[
    categories_df["hierarchy_level"] >= 2
]

# =========================================================
# GENERATE PRODUCTS
# =========================================================

for _ in range(NUM_PRODUCTS):

    # ---------------------------------------------
    # RANDOM CATEGORY SELECTION
    # ---------------------------------------------

    category_row = valid_categories.sample(1).iloc[0]

    category_id = category_row["category_id"]

    category_name = category_row["category_name"]

    # ---------------------------------------------
    # HANDLE UNKNOWN CATEGORY TEMPLATES
    # ---------------------------------------------

    if category_name not in product_templates:

        continue

    template = product_templates[category_name]

    brand = random.choice(
        template["brands"]
    )

    pattern = random.choice(
        template["patterns"]
    )

    model_number = random.randint(100, 9999)

    product_name = (
        f"{brand} {pattern} {model_number}"
    )

    # ---------------------------------------------
    # SUPPLIER SELECTION
    # ---------------------------------------------

    supplier_row = suppliers_df.sample(1).iloc[0]

    supplier_id = supplier_row["supplier_id"]

    # ---------------------------------------------
    # SKU
    # ---------------------------------------------

    sku = generate_sku(
        brand,
        category_name
    )

    # ---------------------------------------------
    # PRICING
    # ---------------------------------------------

    mrp, selling_price, cost_price = (
        generate_pricing()
    )

    # ---------------------------------------------
    # PRODUCT STATUS
    # ---------------------------------------------

    product_status = random.choices(

        [
            "ACTIVE",
            "OUT_OF_STOCK",
            "DISCONTINUED"
        ],

        weights=[85, 10, 5]

    )[0]

    # ---------------------------------------------
    # RETURNABLE FLAG
    # ---------------------------------------------

    is_returnable = random.choices(

        [True, False],

        weights=[90, 10]

    )[0]

    # ---------------------------------------------
    # METADATA
    # ---------------------------------------------

    metadata = generate_metadata()

    # ---------------------------------------------
    # FINAL PRODUCT RECORD
    # ---------------------------------------------

    product_record = {

        "product_id": generate_product_id(
            product_counter
        ),

        "product_name": product_name,

        "brand": brand,

        "category_id": category_id,

        "supplier_id": supplier_id,

        "sku": sku,

        "mrp": mrp,

        "selling_price": selling_price,

        "cost_price": cost_price,

        "product_weight_grams":
            random.randint(100, 10000),

        "product_rating":
            generate_rating(),

        "total_reviews":
            random.randint(0, 50000),

        "launch_date":
            fake.date_between(
                start_date='-3y',
                end_date='today'
            ),

        "product_status":
            product_status,

        "is_returnable":
            is_returnable,
        "operation_type": "FULL_LOAD",

        **metadata
    }

    # ---------------------------------------------
    # APPEND RECORD
    # ---------------------------------------------

    products.append(product_record)

    product_counter += 1

# =========================================================
# CREATE DATAFRAME
# =========================================================

df = pd.DataFrame(products)

# =========================================================
# DIRTY DATA INJECTION
# =========================================================

for index in df.index:

    # ---------------------------------------------
    # 1. Trailing spaces in product name
    # ---------------------------------------------

    if random.random() < 0.02:

        df.at[index, "product_name"] = (

            str(df.at[index, "product_name"]) + "  "
        )

    # ---------------------------------------------
    # 2. Null brand
    # ---------------------------------------------

    if random.random() < 0.01:

        df.at[index, "brand"] = None

    # ---------------------------------------------
    # 3. Invalid pricing
    # ---------------------------------------------

    if random.random() < 0.01:

        df.at[index, "selling_price"] = -100

    # ---------------------------------------------
    # 4. Invalid rating
    # ---------------------------------------------

    if random.random() < 0.01:

        df.at[index, "product_rating"] = 7.5

# =========================================================
# SAVE CSV
# =========================================================
# =========================================================
# FIX DATE FORMATS BEFORE SAVE
# =========================================================

df["launch_date"] = pd.to_datetime(df["launch_date"]).dt.strftime("%Y-%m-%d")
df["created_at"] = pd.to_datetime(df["created_at"]).dt.strftime("%Y-%m-%d %H:%M:%S")
df["updated_at"] = pd.to_datetime(df["updated_at"]).dt.strftime("%Y-%m-%d %H:%M:%S")

# =========================================================
# SAVE CSV
# =========================================================

df.to_csv(OUTPUT_PATH, index=False)

# =========================================================
# SUCCESS MESSAGE
# =========================================================

print("===================================")

print("products.csv generated successfully")

print("Total Records :", len(df))

print("===================================")