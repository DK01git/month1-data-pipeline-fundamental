# =============================================================
# generate_realistic_data.py
# Generates production-realistic sample data for retail pipeline
#
# Design principles:
# - Store type determines product range (Flagship > Standard > Express > Kiosk)
# - Category determines price range and quantity patterns
# - Category determines realistic discount and payment distributions
# - Bad rows reflect Pain Point 3: inconsistent formats per store group
# - Product names are realistic, not "Item N"
# - RUN_DATE is always yesterday to match Airflow data_interval_start
#
# Row counts (unchanged from challenge spec):
#   store_locations:    50
#   product_catalog:   200
#   sales_transactions: 10,050 (10,000 valid + 50 bad)
# =============================================================

import boto3
import io
import random
from botocore.exceptions import ClientError
from datetime import date, timedelta

import pandas as pd
import numpy as np

# ---------------------------------------------------
# Config
# ---------------------------------------------------
# Dynamic date — always yesterday to match Airflow data_interval_start
RUN_DATE = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
# RUN_DATE = "2026-04-19"
BUCKET   = "raw-data"
ENDPOINT = "http://localhost:9000"

print(f"Generating realistic data for: {RUN_DATE}")

random.seed(42)
np.random.seed(42)

s3 = boto3.client(
    "s3",
    endpoint_url          = ENDPOINT,
    aws_access_key_id     = "minioadmin",
    aws_secret_access_key = "minioadmin",
)

try:
    s3.create_bucket(Bucket=BUCKET)
    print(f"✔ Bucket '{BUCKET}' created.")
except ClientError as e:
    code = e.response["Error"]["Code"]
    if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
        print(f"✔ Bucket '{BUCKET}' already exists.")
    else:
        raise

def upload_df(df, key):
    """Upload DataFrame as pipe-delimited CSV to MinIO."""
    buf = io.StringIO()
    df.to_csv(buf, sep="|", index=False)
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    print(f"✔ Uploaded: {key}  ({len(df):,} rows)")


# =============================================================
# STORE LOCATIONS — 50 stores
# Realistic: store names use proper retail naming convention
# Store type distribution reflects real retail chain
# =============================================================

# Sri Lanka cities with province and region
CITY_DATA = [
    ("Colombo",       "Western",        "South",    "high"),
    ("Kandy",         "Central",        "Central",  "medium"),
    ("Galle",         "Southern",       "South",    "medium"),
    ("Jaffna",        "Northern",       "North",    "low"),
    ("Negombo",       "Western",        "South",    "medium"),
    ("Matara",        "Southern",       "South",    "low"),
    ("Kurunegala",    "North Western",  "West",     "low"),
    ("Anuradhapura",  "North Central",  "North",    "low"),
    ("Ratnapura",     "Sabaragamuwa",   "Central",  "low"),
    ("Badulla",       "Uva",            "Central",  "low"),
    ("Trincomalee",   "Eastern",        "East",     "low"),
    ("Batticaloa",    "Eastern",        "East",     "low"),
    ("Hambantota",    "Southern",       "South",    "low"),
    ("Vavuniya",      "Northern",       "North",    "low"),
    ("Nuwara Eliya",  "Central",        "Central",  "low"),
]

# Store type distribution:
# 5 Flagship (high footfall cities only)
# 20 Standard
# 10 Express
# 10 Kiosk
# 5 Warehouse
STORE_TYPE_DIST = (
    ["Flagship"]  * 5  +
    ["Standard"]  * 20 +
    ["Express"]   * 10 +
    ["Kiosk"]     * 10 +
    ["Warehouse"] * 5
)
random.shuffle(STORE_TYPE_DIST)

# Realistic store name suffixes
STORE_SUFFIXES = [
    "City Centre", "Mall", "Junction", "Plaza", "Superstore",
    "Express", "Metro", "Park", "Central", "Outlet",
    "Hypermart", "Bazaar", "Square", "Hub", "Point",
]

stores_data = []
for i in range(1, 51):
    city_info   = CITY_DATA[i % len(CITY_DATA)]
    store_type  = STORE_TYPE_DIST[i - 1]
    suffix      = STORE_SUFFIXES[i % len(STORE_SUFFIXES)]
    opening     = date(2010, 1, 1) + timedelta(days=random.randint(0, 4380))

    # Flagship only in high/medium footfall cities
    if store_type == "Flagship" and city_info[3] == "low":
        store_type = "Standard"

    stores_data.append({
        "store_id":     f"STR{i:03d}",
        "store_name":   f"RetailCo {city_info[0]} {suffix}",
        "city":         city_info[0],
        "state":        city_info[1],
        "region":       city_info[2],
        "store_type":   store_type,
        "opening_date": opening.strftime("%Y-%m-%d"),
    })

stores_df = pd.DataFrame(stores_data)

# Build store_type lookup for transaction generation
store_type_map = {
    row["store_id"]: row["store_type"]
    for row in stores_data
}

upload_df(stores_df, f"retail-data/{RUN_DATE}/store_locations.csv")


# =============================================================
# PRODUCT CATALOG — 200 products
# Realistic: product names, category-appropriate pricing
# =============================================================

# Realistic product names per subcategory
PRODUCT_NAMES = {
    "Phones":      ["Galaxy A54", "iPhone 14", "Redmi Note 12", "Reno 8",
                    "Pixel 7a", "Nord CE 3", "Moto G84", "P50 Pro"],
    "Laptops":     ["VivoBook 15", "IdeaPad 3", "Inspiron 15", "Pavilion 14",
                    "MacBook Air", "Surface Laptop", "ZBook 15", "Swift 3"],
    "Accessories": ["USB-C Hub", "Wireless Mouse", "Keyboard Combo", "Screen Guard",
                    "Phone Case", "Laptop Bag", "Power Bank", "Earbuds"],
    "Audio":       ["TWS Earphones", "Neckband Pro", "Over-Ear Headset",
                    "Bluetooth Speaker", "Soundbar 2.1", "Studio Monitor"],
    "Men":         ["Slim Fit Jeans", "Polo T-Shirt", "Formal Shirt",
                    "Chino Trousers", "Casual Shorts", "Hoodie XL"],
    "Women":       ["Floral Kurti", "Palazzo Set", "Casual Dress",
                    "Denim Jacket", "Printed Saree", "Crop Top"],
    "Kids":        ["School Uniform Set", "Cotton T-Shirt Pack", "Denim Shorts",
                    "Cartoon Pyjama", "Winter Jacket", "Sports Shoes"],
    "Sports":      ["Running Shoes", "Training Shorts", "Compression Tee",
                    "Yoga Mat", "Gym Gloves", "Sports Bottle"],
    "Beverages":   ["Mineral Water 1L", "Orange Juice 1L", "Green Tea Pack",
                    "Energy Drink", "Coconut Water", "Mango Nectar"],
    "Snacks":      ["Mixed Nuts 200g", "Chocolate Bar", "Rice Crackers",
                    "Potato Chips", "Granola Bar", "Biscuit Pack"],
    "Fresh":       ["Tomatoes 1kg", "Carrots 500g", "Spinach Bunch",
                    "Banana Dozen", "Apple Pack 6", "Cucumber 3pk"],
    "Frozen":      ["Frozen Peas 500g", "Fish Fillet 400g", "Chicken Nuggets",
                    "Mixed Veg 1kg", "Ice Cream 1L", "Frozen Roti 10pk"],
    "Furniture":   ["Office Chair", "Bookshelf 5-Tier", "Dining Table 4-Seater",
                    "Wardrobe 2-Door", "TV Cabinet", "Study Desk"],
    "Appliances":  ["Rice Cooker 1.8L", "Blender 600W", "Iron Box 2200W",
                    "Ceiling Fan 56in", "Water Purifier", "Microwave 20L"],
    "Decor":       ["Photo Frame Set", "Wall Clock Modern", "Table Lamp LED",
                    "Cushion Cover 4pk", "Artificial Plant", "Canvas Print"],
    "Garden":      ["Garden Hose 15m", "Plant Pot Set", "Pruning Shears",
                    "Lawn Fertilizer", "Insect Spray", "Seed Mix Pack"],
    "Pharmacy":    ["Paracetamol 10pk", "Vitamin C 60tabs", "Antacid 20tabs",
                    "Antiseptic Cream", "Bandage Roll", "Hand Sanitizer"],
    "Beauty":      ["Moisturizer SPF30", "Shampoo 400ml", "Body Lotion 200ml",
                    "Lipstick Matte", "Foundation 30ml", "Face Wash 100ml"],
    "Fitness":     ["Whey Protein 1kg", "Resistance Band Set", "Jump Rope Pro",
                    "Foam Roller", "Dumbbell 5kg", "Fitness Tracker"],
    "Wellness":    ["Herbal Tea 20bag", "Fish Oil 60caps", "Probiotic 30caps",
                    "Turmeric Powder", "Aloe Vera Gel", "Essential Oil Set"],
}

# Category-appropriate price ranges (cost_price min, max)
PRICE_RANGES = {
    "Electronics": (2000, 150000),
    "Clothing":    (500,  8000),
    "Food":        (50,   2000),
    "Home":        (1000, 75000),
    "Health":      (200,  5000),
}

# Markup multiplier per category
MARKUP = {
    "Electronics": (1.15, 1.35),  # tight margins
    "Clothing":    (1.80, 2.80),  # high margins
    "Food":        (1.10, 1.25),  # low margins
    "Home":        (1.40, 2.00),  # medium margins
    "Health":      (1.50, 2.20),  # medium-high margins
}

CATEGORIES = {
    "Electronics": ["Phones",    "Laptops",    "Accessories", "Audio"],
    "Clothing":    ["Men",       "Women",      "Kids",        "Sports"],
    "Food":        ["Beverages", "Snacks",     "Fresh",       "Frozen"],
    "Home":        ["Furniture", "Appliances", "Decor",       "Garden"],
    "Health":      ["Pharmacy",  "Beauty",     "Fitness",     "Wellness"],
}

BRANDS = {
    "Electronics": ["Samsung", "Apple", "Xiaomi", "Oppo", "Sony", "LG"],
    "Clothing":    ["Nike", "Adidas", "H&M", "Zara", "LocalWear", "SportX"],
    "Food":        ["Nestlé", "Unilever", "Maliban", "Munchee", "Elephant", "Keells"],
    "Home":        ["IKEA", "Singer", "Philips", "Haier", "Abans", "Samsung"],
    "Health":      ["Johnson", "Himalaya", "Unilever", "Pfizer", "Dabur", "Hemas"],
}

SUPPLIERS = [f"SUP{i:03d}" for i in range(1, 21)]

products_data = []
product_counter = {}  # track name usage to avoid duplicates

for i in range(1, 201):
    cat_idx  = (i - 1) % len(CATEGORIES)
    cat      = list(CATEGORIES.keys())[cat_idx]
    subcats  = CATEGORIES[cat]
    subcat   = subcats[(i - 1) % len(subcats)]

    # Get realistic product name
    name_pool = PRODUCT_NAMES.get(subcat, [f"{subcat} Product"])
    name_key  = f"{subcat}_{i % len(name_pool)}"
    if name_key not in product_counter:
        product_counter[name_key] = 0
    product_counter[name_key] += 1
    suffix     = f" v{product_counter[name_key]}" if product_counter[name_key] > 1 else ""
    prod_name  = name_pool[i % len(name_pool)] + suffix

    # Realistic pricing
    price_min, price_max = PRICE_RANGES[cat]
    cost       = round(random.uniform(price_min, price_max), 2)
    markup_min, markup_max = MARKUP[cat]
    list_price = round(cost * random.uniform(markup_min, markup_max), 2)

    products_data.append({
        "product_id":   f"PRD{i:04d}",
        "product_name": prod_name,
        "category":     cat,
        "subcategory":  subcat,
        "brand":        random.choice(BRANDS[cat]),
        "cost_price":   cost,
        "list_price":   list_price,
        "supplier_id":  SUPPLIERS[i % len(SUPPLIERS)],
    })

products_df = pd.DataFrame(products_data)

# Build product metadata lookup for transaction generation
product_meta = {
    row["product_id"]: {
        "category":   row["category"],
        "list_price": row["list_price"],
    }
    for row in products_data
}

upload_df(products_df, f"retail-data/{RUN_DATE}/product_catalog.csv")


# =============================================================
# PRODUCT RANGE PER STORE TYPE
# Flagship: all 200 products
# Standard: 120-150 products (drops niche items)
# Express:  60-80 products (Food, Health, basics only)
# Kiosk:    20-30 products (fast-moving only)
# Warehouse: 150-180 products (bulk focus)
# =============================================================

ALL_PRODUCTS = [f"PRD{i:04d}" for i in range(1, 201)]

# Products by category for Express/Kiosk filtering
FOOD_PRODUCTS    = [p for p in ALL_PRODUCTS
                    if product_meta[p]["category"] == "Food"]
HEALTH_PRODUCTS  = [p for p in ALL_PRODUCTS
                    if product_meta[p]["category"] == "Health"]
CLOTHING_PRODUCTS= [p for p in ALL_PRODUCTS
                    if product_meta[p]["category"] == "Clothing"]
ELECTRONICS_PRODUCTS = [p for p in ALL_PRODUCTS
                         if product_meta[p]["category"] == "Electronics"]

def get_store_products(store_type):
    """Return realistic product list for store type."""
    if store_type == "Flagship":
        return ALL_PRODUCTS

    elif store_type == "Standard":
        # Drops ~25% of products — mostly niche electronics and luxury home
        return random.sample(ALL_PRODUCTS, random.randint(140, 160))

    elif store_type == "Express":
        # Focus: Food, Health, basic Clothing
        pool = FOOD_PRODUCTS + HEALTH_PRODUCTS + random.sample(CLOTHING_PRODUCTS, 10)
        return list(set(pool))

    elif store_type == "Kiosk":
        # Only fast-moving: Snacks, Beverages, basic Health
        pool = (
            random.sample(FOOD_PRODUCTS, min(15, len(FOOD_PRODUCTS))) +
            random.sample(HEALTH_PRODUCTS, min(10, len(HEALTH_PRODUCTS)))
        )
        return list(set(pool))

    elif store_type == "Warehouse":
        # Bulk focus — most products but less variety in Clothing
        return random.sample(ALL_PRODUCTS, random.randint(150, 175))

    return ALL_PRODUCTS

# Pre-compute product list per store
store_products = {
    store_id: get_store_products(store_type)
    for store_id, store_type in store_type_map.items()
}


# =============================================================
# CATEGORY-BASED TRANSACTION PATTERNS
# =============================================================

# Quantity ranges per category
QUANTITY_RANGE = {
    "Electronics": (1, 2),   # rarely buy 3+ electronics
    "Clothing":    (1, 4),   # buy a few items
    "Food":        (1, 12),  # bulk food purchases
    "Home":        (1, 3),   # furniture/appliances
    "Health":      (1, 6),   # health products
}

# Discount distribution per category
# (discount_value, weight)
DISCOUNT_DIST = {
    "Electronics": [(0, 60), (5, 25), (10, 12), (15, 3)],
    "Clothing":    [(0, 20), (10, 25), (20, 30), (30, 20), (40, 5)],
    "Food":        [(0, 85), (5, 12), (10, 3)],
    "Home":        [(0, 40), (5, 25), (10, 20), (15, 10), (20, 5)],
    "Health":      [(0, 55), (5, 25), (10, 15), (15, 5)],
}

# Payment method distribution per category
PAYMENT_DIST = {
    "Electronics": [("CARD", 50), ("CREDIT", 35), ("DIGITAL_WALLET", 12), ("CASH", 3)],
    "Clothing":    [("CARD", 55), ("CASH", 20), ("DIGITAL_WALLET", 20), ("CREDIT", 5)],
    "Food":        [("CARD", 45), ("CASH", 35), ("DIGITAL_WALLET", 18), ("CREDIT", 2)],
    "Home":        [("CARD", 40), ("CREDIT", 45), ("DIGITAL_WALLET", 10), ("CASH", 5)],
    "Health":      [("CARD", 50), ("CASH", 25), ("DIGITAL_WALLET", 20), ("CREDIT", 5)],
}

def weighted_choice(choices):
    """Pick from weighted list of (value, weight) tuples."""
    values  = [c[0] for c in choices]
    weights = [c[1] for c in choices]
    return random.choices(values, weights=weights, k=1)[0]


# =============================================================
# CUSTOMER POOL
# 60% repeat customers, 40% new customers, 20% anonymous
# =============================================================
REPEAT_CUSTOMERS = [f"CUST{i:05d}" for i in range(1, 30001)]   # 30K loyal customers
NEW_CUSTOMERS    = [f"CUST{i:05d}" for i in range(30001, 50001)] # 20K new customers

def get_customer_id():
    r = random.random()
    if r < 0.20:
        return None                                    # anonymous
    elif r < 0.68:                                     # 60% of identified = repeat
        return random.choice(REPEAT_CUSTOMERS)
    else:                                              # 40% of identified = new
        return random.choice(NEW_CUSTOMERS)


# =============================================================
# SALES TRANSACTIONS — 10,000 valid + 50 bad rows
# =============================================================

TOTAL_VALID = 10_000
BAD_ROWS    = 50

# Store transaction volume weights — Flagship gets more transactions
STORE_VOLUME_WEIGHTS = {
    "Flagship":  250,
    "Standard":  120,
    "Express":    60,
    "Kiosk":      25,
    "Warehouse":  80,
}

# Build weighted store selection
store_ids      = list(store_type_map.keys())
store_weights  = [STORE_VOLUME_WEIGHTS[store_type_map[s]] for s in store_ids]

transactions_data = []

for i in range(1, TOTAL_VALID + 1):
    # Weighted store selection — Flagship stores get more transactions
    store_id    = random.choices(store_ids, weights=store_weights, k=1)[0]
    store_type  = store_type_map[store_id]

    # Store-appropriate product selection
    available_products = store_products[store_id]
    product_id  = random.choice(available_products)
    category    = product_meta[product_id]["category"]
    list_price  = product_meta[product_id]["list_price"]

    # Category-appropriate quantity
    qty_min, qty_max = QUANTITY_RANGE[category]
    quantity    = random.randint(qty_min, qty_max)

    # Realistic price — close to list price with minor variation
    unit_price  = round(list_price * random.uniform(0.95, 1.05), 2)

    # Category-appropriate discount
    discount    = weighted_choice(DISCOUNT_DIST[category])

    # Category-appropriate payment method
    payment     = weighted_choice(PAYMENT_DIST[category])

    # Customer
    customer_id = get_customer_id()

    transactions_data.append({
        "transaction_id":   f"TXN{i:07d}",
        "store_id":         store_id,
        "product_id":       product_id,
        "quantity":         quantity,
        "unit_price":       unit_price,
        "discount_pct":     discount,
        "transaction_date": RUN_DATE,
        "payment_method":   payment,
        "customer_id":      customer_id,
    })


# =============================================================
# BAD ROWS — reflect Pain Point 3: inconsistent formats
# per store group (different POS systems)
#
# Store Group A (STR001-STR020): standard format
# Store Group B (STR021-STR035): date format issues
# Store Group C (STR036-STR050): missing fields, negative values
# =============================================================

bad_types = [
    # Group A errors — null transaction IDs (POS export bug)
    {
        "transaction_id":   None,
        "store_id":         f"STR{random.randint(1, 20):03d}",
        "product_id":       f"PRD{random.randint(1, 200):04d}",
        "quantity":         random.randint(1, 5),
        "unit_price":       round(random.uniform(100, 1000), 2),
        "discount_pct":     0,
        "transaction_date": RUN_DATE,
        "payment_method":   "CARD",
        "customer_id":      None,
    },
    # Group B errors — zero quantity (POS data entry error)
    {
        "transaction_id":   f"TXNB{random.randint(10000,99999)}",
        "store_id":         f"STR{random.randint(21, 35):03d}",
        "product_id":       f"PRD{random.randint(1, 200):04d}",
        "quantity":         0,
        "unit_price":       round(random.uniform(100, 1000), 2),
        "discount_pct":     0,
        "transaction_date": RUN_DATE,
        "payment_method":   "CASH",
        "customer_id":      None,
    },
    # Group C errors — negative unit price (system integration error)
    {
        "transaction_id":   f"TXNC{random.randint(10000,99999)}",
        "store_id":         f"STR{random.randint(36, 50):03d}",
        "product_id":       f"PRD{random.randint(1, 200):04d}",
        "quantity":         random.randint(1, 3),
        "unit_price":       -abs(round(random.uniform(10, 500), 2)),
        "discount_pct":     0,
        "transaction_date": RUN_DATE,
        "payment_method":   "CARD",
        "customer_id":      None,
    },
]

for j in range(BAD_ROWS):
    bad_row = dict(bad_types[j % 3])  # cycle through 3 error types
    # Randomise store within group on each iteration
    if j % 3 == 0:
        bad_row["store_id"] = f"STR{random.randint(1, 20):03d}"
        bad_row["transaction_id"] = None
    elif j % 3 == 1:
        bad_row["store_id"] = f"STR{random.randint(21, 35):03d}"
        bad_row["transaction_id"] = f"TXNB{random.randint(10000,99999)}"
        bad_row["quantity"] = 0
    else:
        bad_row["store_id"] = f"STR{random.randint(36, 50):03d}"
        bad_row["transaction_id"] = f"TXNC{random.randint(10000,99999)}"
        bad_row["unit_price"] = -abs(round(random.uniform(10, 500), 2))

    bad_row["product_id"] = f"PRD{random.randint(1, 200):04d}"
    transactions_data.append(bad_row)

# Shuffle — bad rows distributed throughout file
random.shuffle(transactions_data)

sales_df = pd.DataFrame(transactions_data)
upload_df(sales_df, f"retail-data/{RUN_DATE}/sales_transactions.csv")


# =============================================================
# SUMMARY
# =============================================================
store_type_counts = {}
for s in stores_data:
    store_type_counts[s["store_type"]] = store_type_counts.get(s["store_type"], 0) + 1

cat_counts = {}
for p in products_data:
    cat_counts[p["category"]] = cat_counts.get(p["category"], 0) + 1

print(f"""
Summary
═══════════════════════════════════════════════
store_locations:      {len(stores_df):>6,} rows
  {store_type_counts}

product_catalog:      {len(products_df):>6,} rows
  {cat_counts}

sales_transactions:   {len(sales_df):>6,} rows
  valid rows:         {TOTAL_VALID:>6,}
  bad rows:           {BAD_ROWS:>6,}
    Group A (STR001-020): null transaction_id
    Group B (STR021-035): zero quantity
    Group C (STR036-050): negative unit_price

Run date: {RUN_DATE}
Verify at: http://localhost:9001
═══════════════════════════════════════════════
""")