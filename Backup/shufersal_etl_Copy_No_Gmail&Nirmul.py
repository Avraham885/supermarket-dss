import os
import requests
import pandas as pd
import gzip
import xml.etree.ElementTree as ET
from io import BytesIO
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from datetime import datetime

# טעינת חיבור למסד הנתונים
load_dotenv()
db_url = os.getenv("SUPABASE_DATABASE_URL")
engine = create_engine(db_url)

CHAIN_ID = "7290027600007"
CHAIN_NAME = "שופרסל"
BASE_URL = "http://prices.shufersal.co.il/"

# רשימת המעקב שלך
WATCHLIST_STORES = ["001", "042", "116", "205", "300", "002"]

# הגדרת נתיבים ל-Data Lake המקומי
TODAY_STR = datetime.now().strftime('%Y-%m-%d')
BASE_ETL_DIR = "ETL_Process_Shufersal"
STORES_DIR = os.path.join(BASE_ETL_DIR, "Stores_Shufersal", TODAY_STR)
PRICES_DIR = os.path.join(BASE_ETL_DIR, "Prices_Shufersal", TODAY_STR)

os.makedirs(STORES_DIR, exist_ok=True)
os.makedirs(PRICES_DIR, exist_ok=True)

def get_download_links():
    print("[INFO] Connecting to Shufersal website to fetch links...")
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    links = []
    found_targets = set()
    targets_needed = 1 + len(WATCHLIST_STORES)
    
    # סריקת דפים (20 פריטים לדף לפי התמונה שלך)
    for page_num in range(1, 251):
        print(f"[INFO] Scanning page {page_num}...")
        response = requests.get(f"{BASE_URL}?page={page_num}", headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        for tr in soup.find_all('tr'):
            a_tag = tr.find('a', href=True)
            if a_tag:
                row_text = tr.get_text(separator=' ', strip=True)
                if CHAIN_ID in row_text:
                    for word in row_text.split():
                        if CHAIN_ID in word:
                            is_target = False
                            if f"Stores{CHAIN_ID}" in word and "Stores" not in found_targets:
                                is_target = True
                                found_targets.add("Stores")
                            for store in WATCHLIST_STORES:
                                target = f"PriceFull{CHAIN_ID}-{store}"
                                if target in word and target not in found_targets:
                                    is_target = True
                                    found_targets.add(target)
                                    break
                            
                            if is_target:
                                url = a_tag['href']
                                if url.startswith('/'): url = BASE_URL.rstrip('/') + url
                                links.append((word, url))
                                print(f"  [+] Found: {word}")
                            break
        
        if len(found_targets) >= targets_needed: break
    return links

def fast_parse_xml(file_path, item_tag):
    """שיטת Streaming לעיבוד XML מהיר ללא זלילת זיכרון"""
    items = []
    print(f"  [PROCESS] Streaming {item_tag}s from XML...")
    with gzip.open(file_path, 'rb') as f:
        # סריקה איטרטיבית של האלמנטים
        context = ET.iterparse(f, events=('end',))
        for event, elem in context:
            if elem.tag == item_tag or elem.tag.lower() == item_tag.lower() or elem.tag.endswith(item_tag):
                # הפיכת אלמנט למילון פשוט
                item_data = {child.tag: child.text for child in elem}
                items.append(item_data)
                # ניקוי האלמנט מהזיכרון מיד לאחר העיבוד
                elem.clear()
    return pd.DataFrame(items)

def run_full_etl():
    print("======================================")
    print("[START] Starting STREAMING ETL for Shufersal...")
    print("======================================")
    
    # יצירת הרשת במסד הנתונים אם לא קיימת
    with engine.begin() as conn:
        conn.execute(text(f"""
            INSERT INTO "Dim_Chains" (chain_id, chain_name) 
            VALUES ('{CHAIN_ID}', '{CHAIN_NAME}') 
            ON CONFLICT (chain_id) DO NOTHING;
        """))

    all_links = get_download_links()
    
    # הפרדה בין קבצי סניפים לקבצי מחירים כדי להבטיח סדר נכון
    stores_links = [l for l in all_links if "Stores" in l[0]]
    price_links = [l for l in all_links if "PriceFull" in l[0]]
    
    print(f"[INFO] Found {len(stores_links)} store files and {len(price_links)} price files.")

    # שלב א' - טיפול בכל קבצי הסניפים (חובה שיקרה קודם!)
    for fname, url in stores_links:
        print(f"\n[STEP] Processing Stores: {fname}")
        local_path = os.path.join(STORES_DIR, fname + ".gz")
        resp = requests.get(url)
        with open(local_path, 'wb') as f: f.write(resp.content)
        
        df = fast_parse_xml(local_path, 'STORE')
        df.columns = [c.upper() for c in df.columns]
        df = df.rename(columns={'STOREID': 'StoreId', 'STORENAME': 'StoreName', 'CITY': 'City'})
        
        with engine.begin() as conn:
            # עדכון ערים
            cities = df[['City']].drop_duplicates().rename(columns={'City': 'city_name'})
            cities['region'] = 'לא מוגדר'
            for idx, row in cities.iterrows():
                conn.execute(text('INSERT INTO "Dim_City" (city_name, region) VALUES (:city_name, :region) ON CONFLICT (city_name) DO NOTHING'), row.to_dict())
            
            # עדכון סניפים
            df['store_id'] = CHAIN_ID + "-" + df['StoreId'].astype(str).str.zfill(3)
            df['chain_id'] = CHAIN_ID
            stores_to_db = df[['store_id', 'chain_id', 'StoreName', 'City']].rename(columns={'StoreName': 'store_name', 'City': 'city'})
            for idx, row in stores_to_db.iterrows():
                conn.execute(text('INSERT INTO "Dim_Stores" (store_id, chain_id, store_name, city) VALUES (:store_id, :chain_id, :store_name, :city) ON CONFLICT (store_id) DO NOTHING'), row.to_dict())
        print(f"  [SUCCESS] Dim_Stores and Dim_City updated.")

    # שלב ב' - עכשיו אפשר להעלות מחירים בראש שקט
    for fname, url in price_links:
        print(f"\n[STEP] Processing Prices: {fname}")
        local_path = os.path.join(PRICES_DIR, fname + ".gz")
        resp = requests.get(url)
        with open(local_path, 'wb') as f: f.write(resp.content)
        
        df = fast_parse_xml(local_path, 'Item')
        
        # עדכון Dim_Products (בלי ליצור כפילויות)
        products = df[['ItemCode', 'ItemName', 'ManufacturerName']].drop_duplicates(subset=['ItemCode']).copy()
        products = products.rename(columns={'ItemCode': 'barcode', 'ItemName': 'item_name', 'ManufacturerName': 'manufacturer'})
        products['category'] = 'כללי'
        
        # עדכון Fact_Prices
        prices = df[['ItemCode', 'PriceUpdateDate', 'ItemPrice']].copy()
        prices = prices.rename(columns={'ItemCode': 'barcode', 'PriceUpdateDate': 'sample_date', 'ItemPrice': 'price'})
        prices['chain_id'] = CHAIN_ID
        store_num = fname.split('-')[1].split('_')[0] if '-' in fname else "001"
        prices['store_id'] = f"{CHAIN_ID}-{store_num}"
        prices['sample_date'] = pd.to_datetime(prices['sample_date'])

        print(f"  [DB] Injecting {len(prices)} rows to Supabase...")
        with engine.begin() as conn:
            # הזרקה מהירה של מוצרים לטבלה זמנית ואז Upsert מהיר ב-SQL
            # זה מחליף את הלולאה האיטית ששלחה בקשות אחת-אחת
            products.to_sql('temp_products', conn, if_exists='replace', index=False)
            conn.execute(text("""
                INSERT INTO "Dim_Products" (barcode, item_name, category, manufacturer)
                SELECT barcode, item_name, category, manufacturer FROM temp_products
                ON CONFLICT (barcode) DO NOTHING;
            """))
            conn.execute(text("DROP TABLE temp_products;"))
            
            # הזרקת מחירים (כבר בשיטת טורבו)
            prices.to_sql('Fact_Prices', conn, if_exists='append', index=False, chunksize=1000, method='multi')
            
            # הזרקת מחירים
            prices.to_sql('Fact_Prices', conn, if_exists='append', index=False, chunksize=1000, method='multi')
        print(f"  [SUCCESS] Store {store_num} prices inserted.")

    print("\n======================================")
    print("[DONE] 🎉 All data processed successfully!")
    print("======================================")

if __name__ == "__main__":
    run_full_etl()