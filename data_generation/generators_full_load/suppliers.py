# =========================================================
# suppliers_generator.py
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

SOURCE_SYSTEM = "PROCUREMENT_SYSTEM"

OUTPUT_PATH = r"C:\Users\motir\Desktop\E-commerce Project\data_generation\output\suppliers.csv"

# =========================================================
# SUPPLIER MASTER DATA
# =========================================================

supplier_data = {
    # --- Electronics & Tech Supply Chain ---
    "Electronics": [
    ("Tech Distributors Pvt Ltd", "Distributor"),
    ("National Electronics Hub", "Wholesaler"),
    ("Sharma Retail Suppliers", "Retail Vendor"),
    ("Alpha Component Logistics", "Distributor"),
    ("Silicon Valley Importers", "Distributor"),
    ("Nexus Digital Wholesalers LLP", "Wholesaler"),
    ("Matrix Telecomm Solutions", "Wholesaler"),
    ("FutureTech Direct Sales", "Retail Vendor"),
    ("Quantum Semiconductors India", "Distributor"),
    ("Apex PC Components Group", "Wholesaler"),
    ("Omega Mobile Accessories", "Marketplace Seller"),
    ("ByteSize Gadgets Inc.", "Marketplace Seller"),
    ("LaserLine Optoelectronics", "Distributor"),
    ("CloudScale Hardware Network", "Wholesaler"),
    ("Vanguard Audio Systems", "Retail Vendor"),
    ("CoreCompute Server Supplies", "Distributor"),
    ("SonicBoom Sound Equipment", "Wholesaler"),
    ("Optima Network Infrastructure", "Wholesaler"),
    ("Delta Semiconductor Corp", "Distributor"),
    ("VoltEdge Power Solutions", "Retail Vendor"),
    ("CircuitCrafters Electronics", "Marketplace Seller"),
    ("MacroTech Display Panels", "Distributor"),
    ("MicroChip Wholesale Depot", "Wholesaler"),
    ("Titan Battery Technologies", "Wholesaler"),
    ("Sigma Imaging Products LLP", "Distributor"),
    ("Vertex SmartHome Systems", "Retail Vendor"),
    ("Infinity Smart Appliances", "Wholesaler"),
    ("Broadband Supply Partners", "Distributor"),
    ("TeleConnect Hardware Co.", "Wholesaler"),
    ("CyberShield Device Vendors", "Retail Vendor"),
    ("ElectroGlow LED Manufacturing", "Distributor"),
    ("PrimePix Camera Distro", "Wholesaler"),
    ("Kinetic Energy Batteries", "Distributor"),
    ("Magna Sound Distribution", "Distributor"),
    ("Pinnacle Computer Trade", "Wholesaler"),
    ("Zeta Gadget Import House", "Distributor"),
    ("SuperConductor Supply Co", "Wholesaler"),
    ("Nova Gaming Gear Enterprise", "Marketplace Seller"),
    ("TechNova Fulfillment Hub", "Internal Warehouse Supplier"),
    ("TechPort Terminal", "Internal Warehouse Supplier")
    ],
    # --- Fashion, Apparel & Textile ---
    "Fashion": [
    ("Urban Fashion Traders", "Distributor"),
    ("Vogue Apparel Wholesale", "Wholesaler"),
    ("TrendSetter Garments Ltd", "Wholesaler"),
    ("Sutra Weaves & Textiles", "Distributor"),
    ("Classic Threads Retailers", "Retail Vendor"),
    ("Metro Lifestyle Vendors", "Retail Vendor"),
    ("DenimCraft Manufacturing Co", "Distributor"),
    ("Elegant Silks & Fabrics", "Wholesaler"),
    ("ActiveWear Garment Hub", "Wholesaler"),
    ("KidZonia Clothing Supply", "Retail Vendor"),
    ("KnitWear Industries Inc.", "Distributor"),
    ("StyleQuotient Apparel Group", "Distributor"),
    ("Monarch Leather Goods", "Wholesaler"),
    ("Footwear Express Logistics", "Distributor"),
    ("SoleMate Shoe Manufacturers", "Wholesaler"),
    ("Velvet Touch Boutique Supply", "Retail Vendor"),
    ("Golden Thread Handlooms", "Retail Vendor"),
    ("Royal Heritage Textiles", "Distributor"),
    ("Urban Outfitters B2B", "Wholesaler"),
    ("EcoFabric Organic Cotton", "Distributor"),
    ("Prime Stitch Garment Factory", "Wholesaler"),
    ("Signature Label Trading", "Distributor"),
    ("Elite Couture Suppliers", "Wholesaler"),
    ("SmartFit Uniform Vendors", "Retail Vendor"),
    ("FabIndia Bulk Handlooms", "Distributor"),
    ("Blossom Kids Clothing LLP", "Wholesaler"),
    ("TrueBlue Indigo Mills", "Distributor"),
    ("StitchInTime Alterations Co", "Retail Vendor"),
    ("FastFashion Supply Network", "Marketplace Seller"),
    ("ChicBoutique Apparel Seller", "Marketplace Seller"),
    ("Western Garments Trading", "Wholesaler"),
    ("DesiWeaves Traditional Co", "Wholesaler"),
    ("WinterWarm Woolen Mills", "Distributor"),
    ("ActiveStride Athletic Gear", "Wholesaler"),
    ("ThreadBare Mill Outlets", "Retail Vendor"),
    ("Nova Silk Route Traders", "Distributor"),
    ("QuickKart Marketplace Seller", "Marketplace Seller"),
    ("Mega Supply Chain Pvt Ltd", "Wholesaler"),
    ("Flipkart Fulfillment Partner", "Internal Warehouse Supplier"),
    ("AlphaFreight CrossDock Services", "Internal Warehouse Supplier"),
    ("DirectToHome Retail Logistics", "Marketplace Seller"),
    ("Modern Hosiery Wholesalers", "Wholesaler"),
    ("Crest Footwear Distributors", "Distributor"),
    ("Textile CrossDock", "Internal Warehouse Supplier"),
    ("Apparel Sort Center", "Internal Warehouse Supplier")
    ],
    # --- Home, Kitchen & Furniture ---
    "Home & Kitchen":[
    ("Elite Home Products", "Wholesaler"),
    ("Heritage Timber Furniture", "Distributor"),
    ("Modern Kitchen Appliances Ltd", "Wholesaler"),
    ("Royal Decor & Craft Houses", "Distributor"),
    ("ComfortSeating Chair Corp", "Wholesaler"),
    ("SteelLine Culinary Utensils", "Distributor"),
    ("CeramicVibe Potteries Group", "Retail Vendor"),
    ("FurnishCo Living Solutions", "Retail Vendor"),
    ("LuxeBedding Linens & Sheets", "Wholesaler"),
    ("SmartSpace Modular Systems", "Distributor"),
    ("KitchenKing Cookware Trading", "Wholesaler"),
    ("GlassWare Premium Outlets", "Retail Vendor"),
    ("OakWood Hardwood Furnishings", "Distributor"),
    ("SofaStyles Manufacturing LLP", "Wholesaler"),
    ("HomeGlow Lighting Enterprise", "Distributor"),
    ("EcoClean Home Consumables", "Retail Vendor"),
    ("Classic Rugs & Carpets", "Wholesaler"),
    ("UrbanNest Interior Decors", "Marketplace Seller"),
    ("ChefsChoice Restaurant Supply", "Distributor"),
    ("TableTop Ceramic Wholesale", "Wholesaler"),
    ("OrganizedLiving Plasticware", "Retail Vendor"),
    ("BathLuxury Fittings & Tiles", "Wholesaler"),
    ("DreamSleep Mattress Co.", "Distributor"),
    ("StainlessCraft Heavy Metal", "Wholesaler"),
    ("Artisan Woodworks Collective", "Retail Vendor"),
    ("BrightLights Electricals", "Wholesaler"),
    ("CozyCushion Home Furnishings", "Marketplace Seller"),
    ("SwiftStorage Utility Boxes", "Distributor"),
    ("GreenPlast Containers Ltd", "Wholesaler"),
    ("TerraCotta EarthWare Group", "Retail Vendor"),
    ("ModularKitchen Hardware Labs", "Distributor"),
    ("EverLasting Plastics House", "Wholesaler"),
    ("PrimeDecor Window Curtains", "Wholesaler"),
    ("Metropolitan Hardware Supply", "Distributor"),
    ("PureWater Filter Tech Group", "Retail Vendor"),
    ("Grandeur Marble & Granite", "Wholesaler"),
    ("Zodiac Clock Manufacturing", "Marketplace Seller"),
    ("RusticCraft Wooden Designs", "Wholesaler"),
    ("Home Appliance Ingestion", "Internal Warehouse Supplier"),
    ("Furniture Freight Yard", "Internal Warehouse Supplier"),
    ("ValuePlus Variety Wholesale", "Wholesaler"),
    ("QuickPack Bundling Services", "Internal Warehouse Supplier"),
    ("OmniChannel Inventory Partners", "Marketplace Seller")
    ],
    # --- Beauty, Wellness & Personal Care ---
    "Beauty":[
    ("BeautyCare Wholesale LLP", "Distributor"),
    ("NatureGlow Herbal Cosmetics", "Distributor"),
    ("BioDerma Skincare Labs", "Wholesaler"),
    ("AromaTherapy Essential Oils", "Wholesaler"),
    ("ProHair Salon Supplies", "Retail Vendor"),
    ("PureOrganics Wellness Corp", "Distributor"),
    ("LuxeCosmetics Trading Co", "Wholesaler"),
    ("SkinDeep Dermatological Gear", "Distributor"),
    ("Radiant Skin Care Agencies", "Wholesaler"),
    ("Glamour World Makeup Distro", "Distributor"),
    (" HerbalVeda Ayurvedic Bulk", "Wholesaler"),
    ("MediCure Personal Hygiene", "Retail Vendor"),
    ("ScentMaster Perfumeries", "Distributor"),
    ("VelvetSkin Body Lotions", "Wholesaler"),
    ("OrganicRoots Wellness Hub", "Marketplace Seller"),
    ("FreshBreath Oral Care Ltd", "Wholesaler"),
    ("MiracleGlow Face Serums", "Marketplace Seller"),
    ("Infinity Salon Systems", "Distributor"),
    ("NutriBio Dietary Supplements", "Wholesaler"),
    ("NaturalEssence Soap Mills", "Retail Vendor"),
    ("DermaCare Clinical Supplies", "Wholesaler"),
    ("Elite Grooming Accessories", "Retail Vendor"),
    ("Aura Wellness Trade Network", "Distributor"),
    ("Lotus Petals Cosmetics", "Wholesaler"),
    ("PrimeHeal FirstAid Supplies", "Wholesaler"),
    ("Royal Fragrance Import House", "Distributor"),
    ("PureSilk Bath Products", "Retail Vendor"),
    ("GlowNation Beauty Merchants", "Marketplace Seller"),
    ("BioNutrient Vitamin Trade", "Wholesaler"),
    ("ClearSkin Laser & Spa Supply", "Distributor"),
    ("EcoFriendly Bamboo Brushes", "Marketplace Seller"),
    ("Advanced Trichology Labs", "Wholesaler"),
    ("Saffron Fields Extract Co.", "Distributor"),
    ("BabySafe Hypoallergenic Products", "Wholesaler"),
    ("SpaLuxury Essential Oils", "Retail Vendor"),
    ("ForeverYoung AntiAging Goods", "Wholesaler"),
    ("ColorPalette Nail Polishes", "Marketplace Seller"),
    ("SpringFresh Body Sprays", "Distributor"),
    ("Chennai Beauty Sortation Vault", "Internal Warehouse Supplier"),
    ("Hyderabad Pharma & Cosmetics Depot", "Internal Warehouse Supplier"),
    ("GlobalTrade Import Consortium", "Distributor"),
    ("ExpressCargo Wholesale Traders", "Wholesaler"),
    ("SmartChoice Online Vendors", "Marketplace Seller"),
    ("PrimeConsumables Supply Corp", "Wholesaler"),
    ("SuperSaver FMCG Distributors", "Distributor"),
    ("StarB2B Commercial Trading", "Wholesaler"),
    ("NationWide Multi-Category Sellers", "Marketplace Seller")
    ],
    # --- Sports, Fitness & Outdoors ---
    "Sports": [
    ("SportsZone Retailers", "Retail Vendor"),
    ("ProFit Gym Equipment Trade", "Distributor"),
    ("IronMuscle Weightlifting Co", "Wholesaler"),
    ("Olympic Sports Wholesalers", "Wholesaler"),
    ("AdventureGear Camping Supplies", "Distributor"),
    ("WillowCraft Cricket Bats Ltd", "Distributor"),
    ("Stamina Fitness Machines", "Wholesaler"),
    ("WaterSports Marine Logistics", "Distributor"),
    ("TrackAndField Athletic Goods", "Wholesaler"),
    ("CycleSpeed Bicycle Assembly", "Distributor"),
    ("TrekKer Hiking & Mountain Gear", "Wholesaler"),
    ("Apex Gym Floorings & Mats", "Wholesaler"),
    ("SmashRacket Tennis Agencies", "Retail Vendor"),
    ("GoalPost Football Logistics", "Distributor"),
    ("AquaSwim Pool Accessories", "Retail Vendor"),
    ("LeatherBalls Sporting Goods", "Wholesaler"),
    ("FitLife Wearable Trackers", "Marketplace Seller"),
    ("WildCountry Survival Kits", "Distributor"),
    ("Bullseye Archery Equipment", "Wholesaler"),
    ("Velocity Roller Skates", "Retail Vendor"),
    ("AerobicFlex Yoga Brands", "Marketplace Seller"),
    ("Champion Trophies & Medals", "Retail Vendor"),
    ("RuggedTerrain Outdoor Boots", "Distributor"),
    ("StrikeRate Cricket Gear", "Wholesaler"),
    ("MarineFloat Kayaks & Canoes", "Wholesaler"),
    ("PowerPulse Energy Nutrition", "Marketplace Seller"),
    ("Everest Mountaineering Imports", "Distributor"),
    ("NetPlay Badminton Systems", "Wholesaler"),
    ("SwiftRun Athletic Footwear", "Wholesaler"),
    ("GameDay Premium Team Jerseys", "Retail Vendor"),
    ("Precision Golf Clubs & Balls", "Distributor"),
    ("DeepBlue Scuba Accessories", "Wholesaler"),
    ("BicycleBros Commuter Cycles", "Marketplace Seller"),
    ("AirLine Paragliding Equipment", "Wholesaler"),
    ("DefenseShield Martial Arts", "Retail Vendor"),
    ("AllWeather Sports Nets", "Wholesaler"),
    ("FlexiBands Resistance Tubes", "Marketplace Seller"),
    ("CombatZone Boxing Gloves", "Wholesaler"),
    ("Sports Mega-Hub", "Internal Warehouse Supplier"),
    ("Sports Ingestion Center", "Internal Warehouse Supplier"),
    ("BlueDart Logistics Storage Node", "Internal Warehouse Supplier"),
    ("Delhivery Sorting Base Alpha", "Internal Warehouse Supplier"),
    ("E-ComExpress Fulfilment Pod", "Internal Warehouse Supplier"),
    ("E-Retail Inventory Solutions", "Marketplace Seller"),
    ("GlobalSourcing Asia-Pac", "Distributor"),
    ]
}

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
# HELPER VARIABLES
# =========================================================

suppliers = []

supplier_counter = 1

now = datetime.now()

# =========================================================
# GENERATE SUPPLIER ID
# =========================================================

def generate_supplier_id(counter):

    return f"SUP{counter:04d}"

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
        start_date='-10y',
        end_date='now'
    )
    days_between_now_and_creation = (datetime.now() - created_at).days

    max_days_offset = min(365*2, days_between_now_and_creation)

    updated_at = created_at + timedelta(
        days=random.randint(0, max_days_offset)
    )

    return {

        "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),

        "updated_at": updated_at.strftime("%Y-%m-%d %H:%M:%S"),

        "deleted_flag": random.choices(
            [False, True],
            weights=[97, 3]
        )[0],

        "source_system": SOURCE_SYSTEM,

        "record_version": random.randint(1, 3)
    }

# =========================================================
# GENERATE CONTRACT DATES
# =========================================================

def generate_contract_dates():

    contract_start = fake.date_between(
        start_date='-5y',
        end_date='-1y'
    )

    contract_end = contract_start + timedelta(
        days=random.randint(365, 1825)
    )

    return contract_start, contract_end

# =========================================================
# GENERATE SUPPLIER RECORDS
# =========================================================

for category, supplier_list in supplier_data.items():

    for supplier_name, supplier_type in supplier_list:

        supplier_id = generate_supplier_id(supplier_counter)

        contract_start, contract_end = generate_contract_dates()

        metadata = generate_metadata()

        supplier_status = random.choices(

            ["ACTIVE", "INACTIVE", "BLACKLISTED"],

            weights=[85, 10, 5]

        )[0]

        state = random.choice(list(state_city_map.keys()))

        city = random.choice(list(state_city_map[state]))

        supplier_record = {

            "supplier_id": supplier_id,

            "supplier_name": supplier_name,

            "supplier_type": supplier_type,

            "contact_person": fake.name(),

            "email": fake.company_email(),

            "phone": generate_phone(),

            "city": city,

            "state": state,

            "country": "India",

            "postal_code": fake.postcode(),

            "supplier_status": supplier_status,

            "contract_start_date": contract_start,

            "contract_end_date": contract_end,

            "operation_type": "FULL_LOAD",

            **metadata
        }

        suppliers.append(supplier_record)

        supplier_counter += 1

# =========================================================
# CREATE DATAFRAME
# =========================================================

df = pd.DataFrame(suppliers)

# =========================================================
# DIRTY DATA INJECTION
# =========================================================

for index in df.index:

    # 1. Trailing spaces in supplier name
    if random.random() < 0.02:

        df.at[index, "supplier_name"] = (
            str(df.at[index, "supplier_name"]) + "  "
        )

    # 2. Null email
    if random.random() < 0.02:

        df.at[index, "email"] = None

    # 3. Invalid phone number
    if random.random() < 0.01:

        df.at[index, "phone"] = random.choice([
            "123",
            "INVALID_PHONE",
            "99999"
        ])

    # 4. Contract end before contract start
    if random.random() < 0.01:

        df.at[index, "contract_end_date"] = fake.date_between(
            start_date='-6y',
            end_date='-5y'
        )

# =========================================================
# SAVE CSV
# =========================================================

df.to_csv(OUTPUT_PATH, index=False)

# =========================================================
# SUCCESS MESSAGE
# =========================================================

print("===================================")
print("suppliers.csv generated successfully")
print("Total Records :", len(df))
print("===================================")