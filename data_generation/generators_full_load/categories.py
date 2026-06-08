# =========================================================
# categories_generator.py
# =========================================================

# importing required libraries
import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

# =========================================================
# CONFIGURATION
# =========================================================

SOURCE_SYSTEM = "PRODUCT_CATALOG_SYSTEM"

OUTPUT_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\categories.csv"

# =========================================================
# CATEGORY HIERARCHY
# =========================================================

category_hierarchy = {

    "Electronics": {
        "Mobiles": [
            "Android Phones",
            "iphones"
        ],

        "Laptops": [
            "Gaming",
            "Business"
        ],

        "Headphones": ["Gaming","Wired","Wire-less","Neckbands"],
        "Smart Watches": ["Fitness","Sports"]
    },

    "Fashion": {
        "Men Clothing": ["Casual Shirts", "Formal Suits", "Jeans", "T-Shirts",
            "Trousers", "Jackets", "Sweatshirts", "Hoodies",
            "Shorts", "Innerwear","Socks"],
        "Women Clothing": ["Sarees","Suits","Nightsuits","Kurti","Jeans","Bra","Panties",
            "Lehenga","Bra","Palazzo"],
        "Footwear": ["Flip-flops","Sneakers","Chappal","Sandals","Heels","Jutti","Boots"]
    },

    "Home & Kitchen": {
        "Furniture": ["Sofas","Dining Table","Chair","Double Bed","Single Bed","Wardrobes"],
        "Kitchen Appliances": ["Air Fryers", "Microwave","Kettles","Toasters","Blenders","Dishwashers"],
        "Home Decor": ["Artificial Plants","Wall Clocks","Photo Frames","Ceramic Vases","Table Lamps"]
    },

    "Beauty": {
        "Skincare": ["Face Serums", "Hydrating Moisturizers", "Sunscreen SPF 50", "Foaming Cleansers",
            "Sheet Masks", "Under-Eye Creams"],
        "Haircare": ["Anti-Dandruff Shampoos", "Conditioners", "Hair Serums", "Hair Oils",
            "Scalp Scrubs"]
    },

    "Sports": {
        "Fitness Equipment": ["Motorized Treadmills", "Adjustable Dumbbells", "Resistance Bands", "Yoga Mats",
            "Ankle Weights", "Kettlebells", "Pull-up Bars", "Exercise Bikes",
            "Ab Rollers", "Foam Rollers"],
        "Cricket": ["English Willow Bats", "Kashmir Willow Bats", "Leather Cricket Balls", "Batting Pads",
            "Wicket Keeping Gloves", "Cricket Helmets", "Kit Bags with Wheels", "Cricket Spikes Shoes",
            "Stumps & Bails", "Thigh & Arm Guards"],
    }
}

# =========================================================
# HELPER VARIABLES
# =========================================================

categories = []

category_counter = 1

now = datetime.now()

# =========================================================
# GENERATE CATEGORY ID
# =========================================================

def generate_category_id(counter):

    return f"CAT{counter:04d}"

# =========================================================
# GENERATE METADATA
# =========================================================

def generate_metadata():

    created_at = fake.date_time_between(
        start_date='-3y',
        end_date='now'
    )
    days_between_now_and_creation = (datetime.now() - created_at).days

    max_days_offset = min(365, days_between_now_and_creation)

    updated_at = created_at + timedelta(
        days=random.randint(0, max_days_offset)
    )

    metadata = {

        "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),

        "updated_at": updated_at.strftime("%Y-%m-%d %H:%M:%S"),

        "deleted_flag": random.choices(
            [False, True],
            weights=[98, 2]
        )[0],

        "source_system": SOURCE_SYSTEM,
        "record_version":random.randint(1, 3)
    }

    return metadata

# =========================================================
# GENERATE LEVEL 1 CATEGORIES
# =========================================================

parent_category_map = {}

for parent_category in category_hierarchy:

    category_id = generate_category_id(category_counter)

    parent_category_map[parent_category] = category_id

    metadata = generate_metadata()

    categories.append({

        "category_id": category_id,

        "category_name": parent_category,

        "parent_category_id": None,

        "hierarchy_level": 1,

        "is_active": True,

        "operation_type": "FULL_LOAD",

        **metadata
    })

    category_counter += 1

# =========================================================
# GENERATE LEVEL 2 AND LEVEL 3 CATEGORIES
# =========================================================

for parent_category, subcategories in category_hierarchy.items():

    parent_category_id = parent_category_map[parent_category]

    # ---------------- LEVEL 2 ----------------

    for subcategory, sub_subcategories in subcategories.items():

        subcategory_id = generate_category_id(category_counter)

        metadata = generate_metadata()

        categories.append({

            "category_id": subcategory_id,

            "category_name": subcategory,

            "parent_category_id": parent_category_id,

            "hierarchy_level": 2,

            "is_active": True,

            **metadata
        })

        category_counter += 1

        # ---------------- LEVEL 3 ----------------

        for sub_subcategory in sub_subcategories:

            sub_subcategory_id = generate_category_id(category_counter)

            metadata = generate_metadata()

            categories.append({

                "category_id": sub_subcategory_id,

                "category_name": sub_subcategory,

                "parent_category_id": subcategory_id,

                "hierarchy_level": 3,

                "is_active": True,

                **metadata
            })

            category_counter += 1

# =========================================================
# CREATE DATAFRAME
# =========================================================

df = pd.DataFrame(categories)

# =========================================================
# DIRTY DATA INJECTION
# =========================================================

for index in df.index:

    # 1. Trailing spaces in category_name
    if random.random() < 0.02:

        df.at[index, "category_name"] = (
            str(df.at[index, "category_name"]) + "  "
        )

    # 2. Null category_name
    if random.random() < 0.01:

        df.at[index, "category_name"] = None

    # 3. Inactive categories
    if random.random() < 0.03:

        df.at[index, "is_active"] = False

# =========================================================
# SAVE CSV
# =========================================================

df.to_csv(OUTPUT_PATH, index=False)

print("===================================")
print("categories.csv generated successfully")
print("Total Records :", len(df))
print("===================================")

