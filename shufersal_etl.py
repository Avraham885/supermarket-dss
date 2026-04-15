import os
import requests
import pandas as pd
import gzip
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import traceback

# ==========================================
# CONFIGURATION
# ==========================================
db_url = os.environ.get("SUPABASE_DATABASE_URL")
if not db_url:
    raise ValueError("Missing SUPABASE_DATABASE_URL environment variable")

engine = create_engine(db_url)

CHAIN_ID = "7290027600007"
CHAIN_NAME = "שופרסל"
BASE_URL = "http://prices.shufersal.co.il/"

WATCHLIST_STORES = ["001", "042", "116", "205", "300", "002"]

DATA_DIR = "ETL_Process_Shufersal"
STORES_DIR = os.path.join(DATA_DIR, "stores")
PRICES_DIR = os.path.join(DATA_DIR, "prices")
os.makedirs(STORES_DIR, exist_ok=True)
os.makedirs(PRICES_DIR, exist_ok=True)

# ==========================================
# EMAIL CONFIGURATION
# ==========================================
EMAIL_SENDER = os.environ.get("EMAIL_SENDER") 
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD") 
EMAIL_RECEIVER = os.environ.get("EMAIL_RECEIVER")

def send_email_report(subject, body):
    if not all([EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECEIVER]):
        print("[WARNING] Email credentials not fully set. Skipping email alert.")
        return

    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_SENDER
        msg['To'] = EMAIL_RECEIVER
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain', 'utf-8'))

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()
        print(f"[SUCCESS] Email sent: {subject}")
    except Exception as e:
        print(f"[ERROR] Failed to send email: {e}")

# ==========================================
# DATA NORMALIZATION DICTIONARIES
# ==========================================
CITY_MAPPING = {
    'ת"א': 'תל אביב', 'תלאביב': 'תל אביב', 'תל אביב - יפו': 'תל אביב', 'תל אביב-יפו': 'תל אביב', 'רמת אביב א': 'תל אביב',
    'י-ם': 'ירושלים', 'ירושלם': 'ירושלים', 'ים': 'ירושלים',
    'ראשל"צ': 'ראשון לציון', 'ראשוןלציון': 'ראשון לציון', 'ראשון': 'ראשון לציון',
    'באר-שבע': 'באר שבע', 'בארשבע': 'באר שבע', 'ב"ש': 'באר שבע',
    'בית-שמש': 'בית שמש', 'ראש-פינה': 'ראש פינה', ' באר יעקב': 'באר יעקב',
    'פתח-תקוה': 'פתח תקווה', 'פתח-תקווה': 'פתח תקווה', 'פתחתקוה': 'פתח תקווה', 'פתח תקוה': 'פתח תקווה',
    'בני-ברק': 'בני ברק', 'כפר-סבא': 'כפר סבא', 'כפר סבא צפון': 'כפר סבא',
    'רמת-גן': 'רמת גן', 'רמת-השרון': 'רמת השרון', 'מצפה-רמון': 'מצפה רמון',
    'יוקנעם': 'יקנעם עילית', 'יקנעם': 'יקנעם עילית', 'טבעון': 'קריית טבעון',
    'רעות': 'מודיעין', 'חצור-הגלילית': 'חצור הגלילית', 'בת-ים': 'בת ים',
    'נס-ציונה': 'נס ציונה', 'נוף-הגליל': 'נוף הגליל',
    'NaN': 'לא ידוע', 'nan': 'לא ידוע'
}

REGION_MAPPING = {
    'אופקים': 'דרום', 'אור יהודה': 'מרכז', 'אור עקיבא': 'שרון', 'אילת': 'דרום', 
    'אלנקווה': 'יהודה ושומרון', 'אלעד': 'מרכז', 'אריאל': 'יהודה ושומרון', 'אשדוד': 'דרום', 
    'אשקלון': 'דרום', 'באר טוביה': 'דרום', 'באר יעקב': 'מרכז', 'באר שבע': 'דרום', 
    'בארות יצחק': 'מרכז', 'בית חשמונאי': 'מרכז', 'בית שאן': 'צפון', 'בית שמש': 'ירושלים והסביבה', 
    'ביתר עילית': 'ירושלים והסביבה', 'בני ברק': 'מרכז', 'בני דרור': 'שרון', 'בנימינה': 'צפון', 
    'בת חפר': 'שרון', 'בת ים': 'מרכז', 'גבעת אולגה': 'צפון', 'גבעת עדה': 'צפון', 
    'גבעת שמואל': 'מרכז', 'גבעתיים': 'מרכז', 'גדרה': 'מרכז', 'דימונה': 'דרום', 
    'דלית אל כרמל': 'צפון', 'הוד השרון': 'שרון', 'הרצליה': 'שרון', 'זכרון יעקב': 'צפון', 
    'חדרה': 'צפון', 'חולון': 'מרכז', 'חיפה': 'צפון', 'חצור הגלילית': 'צפון', 
    'חריש': 'צפון', 'טבריה': 'צפון', 'טייבה': 'מרכז', 'טירה': 'מרכז', 
    'טירת הכרמל': 'צפון', 'יבנה': 'מרכז', 'יהוד': 'מרכז', 'יקנעם עילית': 'צפון', 
    'ירוחם': 'דרום', 'ירושלים': 'ירושלים והסביבה', 'ירכא': 'צפון', 'כפר ורדים': 'צפון', 
    'כפר יונה': 'שרון', 'כפר נטר': 'שרון', 'כפר סבא': 'שרון', 'כפר קרע': 'צפון', 
    'כפר תבור': 'צפון', 'כרכור': 'צפון', 'כרמיאל': 'צפון', 'לא ידוע': 'לא מוגדר', 
    'מבשרת ציון': 'ירושלים והסביבה', 'מגדל העמק': 'צפון', 'מודיעין': 'מרכז', 
    'מודיעין עילית': 'יהודה ושומרון', 'מזכרת בתיה': 'מרכז', 'מיתר': 'דרום', 
    'מעלה אדומים': 'יהודה ושומרון', 'מעלות': 'צפון', 'מצפה רמון': 'דרום', 
    'משמר השרון': 'שרון', 'נהריה': 'צפון', 'נוף הגליל': 'צפון', 'נס ציונה': 'מרכז', 
    'נצרת': 'צפון', 'נשר': 'צפון', 'נתניה': 'שרון', 'סביון': 'מרכז', 
    'סכנין': 'צפון', 'עומר': 'דרום', 'עין שמר': 'צפון', 'עכו': 'צפון', 
    'עפולה': 'צפון', 'ערד': 'דרום', 'פרדס חנה': 'צפון', 'פרדסיה': 'שרון', 
    'פתח תקווה': 'מרכז', 'צור יגאל': 'שרון', 'צור משה': 'שרון', 'צורן': 'שרון', 
    'צפת': 'צפון', 'קדימה': 'שרון', 'קצרין': 'צפון', 'קריית אונו': 'מרכז', 
    'קריית אתא': 'צפון', 'קריית ביאליק': 'צפון', 'קריית גת': 'דרום', 
    'קריית חיים': 'צפון', 'קריית טבעון': 'צפון', 'קריית מוצקין': 'צפון', 
    'קריית ספר': 'יהודה ושומרון', 'קריית שמונה': 'צפון', 'ראש העין': 'מרכז', 
    'ראש פינה': 'צפון', 'ראשון לציון': 'מרכז', 'רהט': 'דרום', 'רחובות': 'מרכז', 
    'רכסים': 'צפון', 'רמלה': 'מרכז', 'רמת גן': 'מרכז', 'רמת השרון': 'מרכז', 
    'רעננה': 'שרון', 'שדרות': 'דרום', 'שוהם': 'מרכז', 'שילת': 'מרכז', 
    'שפרעם': 'צפון', 'תל אביב': 'מרכז', 'תל מונד': 'שרון'
}

def normalize_city_name(city_name):
    if not isinstance(city_name, str) or city_name.strip() == '': 
        return 'לא ידוע'
    city_name = city_name.strip()
    if city_name in CITY_MAPPING: return CITY_MAPPING[city_name]
    if city_name.startswith('קרית '): return city_name.replace('קרית ', 'קריית ')
    return city_name

# ==========================================
# ETL LOGIC
# ==========================================
def get_download_links():
    print("[INFO] Connecting to Shufersal website to fetch links...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive"
    }
    
    links = []
    found_targets = set()
    targets_needed = 1 + len(WATCHLIST_STORES)
    
    for page_num in range(1, 251):
        try:
            response = requests.get(f"{BASE_URL}?page={page_num}", headers=headers, timeout=45)
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
            
        except requests.exceptions.Timeout:
            print(f"[ERROR] Shufersal server timeout on page {page_num}!")
            break
        except Exception as e:
            print(f"[ERROR] Failed on page {page_num}: {e}")
            break
            
    if len(links) == 0:
        raise Exception("Critical: Found 0 files! The scraper was blocked or the site is down.")
        
    return links

def fast_parse_xml(file_path, item_tag):
    items = []
    with gzip.open(file_path, 'rb') as f:
        context = ET.iterparse(f, events=('end',))
        for event, elem in context:
            if elem.tag == item_tag or elem.tag.lower() == item_tag.lower() or elem.tag.endswith(item_tag):
                item_data = {child.tag: child.text for child in elem}
                items.append(item_data)
                elem.clear()
    return pd.DataFrame(items)

def run_full_etl():
    print("======================================")
    print("[START] Starting STREAMING ETL for Shufersal...")
    print("======================================")
    
    start_time = datetime.now()
    stats = {"stores_files": 0, "price_files": 0, "total_prices_scanned": 0, "total_prices_inserted": 0}

    with engine.begin() as conn:
        conn.execute(text(f"""
            INSERT INTO "Dim_Chains" (chain_id, chain_name) 
            VALUES ('{CHAIN_ID}', '{CHAIN_NAME}') 
            ON CONFLICT (chain_id) DO NOTHING;
        """))

    all_links = get_download_links()
    
    stores_links = [l for l in all_links if "Stores" in l[0]]
    price_links = [l for l in all_links if "PriceFull" in l[0]]
    
    print(f"[INFO] Found {len(stores_links)} store files and {len(price_links)} price files.")
    stats["stores_files"] = len(stores_links)
    stats["price_files"] = len(price_links)

    # --- שלב א: קבצי סניפים ---
    for fname, url in stores_links:
        print(f"\n[STEP] Processing Stores: {fname}")
        local_path = os.path.join(STORES_DIR, fname + ".gz")
        resp = requests.get(url)
        with open(local_path, 'wb') as f: f.write(resp.content)
        
        df = fast_parse_xml(local_path, 'STORE')
        df.columns = [c.upper() for c in df.columns]
        df = df.rename(columns={'STOREID': 'StoreId', 'STORENAME': 'StoreName', 'CITY': 'City'})
        df['City'] = df['City'].apply(normalize_city_name)
        
        with engine.begin() as conn:
            cities = df[['City']].drop_duplicates().rename(columns={'City': 'city_name'})
            cities['region'] = cities['city_name'].map(lambda x: REGION_MAPPING.get(x, 'לא מוגדר'))
            
            for idx, row in cities.iterrows():
                conn.execute(text('INSERT INTO "Dim_City" (city_name, region) VALUES (:city_name, :region) ON CONFLICT (city_name) DO UPDATE SET region = EXCLUDED.region'), row.to_dict())
            
            df['store_id'] = CHAIN_ID + "-" + df['StoreId'].astype(str).str.zfill(3)
            df['chain_id'] = CHAIN_ID
            stores_to_db = df[['store_id', 'chain_id', 'StoreName', 'City']].rename(columns={'StoreName': 'store_name', 'City': 'city'})
            
            stores_to_db.to_sql('temp_stores', conn, if_exists='replace', index=False)
            conn.execute(text("""
                INSERT INTO "Dim_Stores" (store_id, chain_id, store_name, city)
                SELECT store_id, chain_id, store_name, city FROM temp_stores
                ON CONFLICT (store_id) DO UPDATE SET store_name = EXCLUDED.store_name, city = EXCLUDED.city;
            """))
            conn.execute(text("DROP TABLE temp_stores;"))
            
        print(f"  [SUCCESS] Dim_Stores and Dim_City updated.")

    # --- שלב ב: קבצי מחירים ---
    for fname, url in price_links:
        print(f"\n[STEP] Processing Prices: {fname}")
        local_path = os.path.join(PRICES_DIR, fname + ".gz")
        resp = requests.get(url)
        with open(local_path, 'wb') as f: f.write(resp.content)
        
        df = fast_parse_xml(local_path, 'Item')
        
        # =====================================================================
        # תיקון דינאמי של שמות העמודות (Schema Drift Handler)
        # =====================================================================
        # 1. טיפול ביצרן: הופך את ManufactureName ל-ManufacturerName
        if 'ManufacturerName' not in df.columns:
            if 'ManufactureName' in df.columns:
                df = df.rename(columns={'ManufactureName': 'ManufacturerName'})
            else:
                df['ManufacturerName'] = 'לא ידוע'

        # 2. טיפול בתאריך: הופך את PriceUpdateTime ל-PriceUpdateDate
        if 'PriceUpdateDate' not in df.columns:
            if 'PriceUpdateTime' in df.columns:
                df = df.rename(columns={'PriceUpdateTime': 'PriceUpdateDate'})
            else:
                df['PriceUpdateDate'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
        # 3. גיבוי לעמודות חסרות נוספות כדי למנוע קריסה
        if 'ItemName' not in df.columns: df['ItemName'] = 'לא ידוע'
        if 'ItemPrice' not in df.columns: df['ItemPrice'] = 0.0
        # =====================================================================

        products = df[['ItemCode', 'ItemName', 'ManufacturerName']].drop_duplicates(subset=['ItemCode']).copy()
        products = products.rename(columns={'ItemCode': 'barcode', 'ItemName': 'item_name', 'ManufacturerName': 'manufacturer'})
        products['category'] = 'כללי'
        
        prices = df[['ItemCode', 'PriceUpdateDate', 'ItemPrice']].copy()
        prices = prices.rename(columns={'ItemCode': 'barcode', 'PriceUpdateDate': 'sample_date', 'ItemPrice': 'price'})
        prices['chain_id'] = CHAIN_ID
        store_num = fname.split('-')[1].split('_')[0] if '-' in fname else "001"
        prices['store_id'] = f"{CHAIN_ID}-{store_num}"
        prices['sample_date'] = pd.to_datetime(prices['sample_date'])

        stats["total_prices_scanned"] += len(prices)

        print(f"  [DB] Injecting to Supabase (filtering duplicates)...")
        with engine.begin() as conn:
            products.to_sql('temp_products', conn, if_exists='replace', index=False)
            conn.execute(text("""
                INSERT INTO "Dim_Products" (barcode, item_name, category, manufacturer)
                SELECT barcode, item_name, category, manufacturer FROM temp_products
                ON CONFLICT (barcode) DO NOTHING;
            """))
            conn.execute(text("DROP TABLE temp_products;"))
            
            prices.to_sql('temp_prices', conn, if_exists='replace', index=False)
            result = conn.execute(text("""
                INSERT INTO "Fact_Prices" (store_id, barcode, price, sample_date, chain_id)
                SELECT store_id, barcode, CAST(price AS NUMERIC), CAST(sample_date AS TIMESTAMP), chain_id FROM temp_prices
                ON CONFLICT (store_id, barcode, sample_date) DO NOTHING;
            """))
            conn.execute(text("DROP TABLE temp_prices;"))
            
            inserted_rows = result.rowcount
            stats["total_prices_inserted"] += inserted_rows
            print(f"  [SUCCESS] Store {store_num}: {inserted_rows} NEW prices inserted out of {len(prices)} scanned.")

    end_time = datetime.now()
    duration = round((end_time - start_time).total_seconds() / 60, 2)
    
    try:
        with engine.connect() as conn:
            size_bytes = conn.execute(text("SELECT pg_database_size(current_database());")).scalar()
            db_size_gb = size_bytes / (1024 ** 3)
    except Exception as e:
        print(f"[WARNING] Could not get DB size: {e}")
        db_size_gb = 0.0
    
    print("\n======================================")
    print(f"[DONE] 🎉 All data processed successfully in {duration} minutes!")
    print("======================================")
    
    report_body = f"""Shufersal Data Pipeline - SUCCESS 🟢

Run Time: {duration} minutes
Store Files Processed: {stats['stores_files']}
Price Files Processed: {stats['price_files']}

📊 Data Metrics:
- Total Prices Scanned: {stats['total_prices_scanned']}
- NEW Prices Inserted: {stats['total_prices_inserted']}

💾 Database Storage (Supabase):
- Current Size: {db_size_gb:.3f} GB
- Max Allowed (Free Tier): 0.500 GB
- Usage: {(db_size_gb / 0.5) * 100:.1f}%

Your Supermarket DSS is up to date! 🚀
"""
    send_email_report("🟢 ETL Success: Shufersal", report_body)

if __name__ == "__main__":
    try:
        run_full_etl()
    except Exception as e:
        error_tb = traceback.format_exc()
        print(f"\n[CRITICAL ERROR] Pipeline failed:\n{error_tb}")
        error_body = f"Shufersal Data Pipeline - FAILED 🔴\n\nError details:\n{error_tb}"
        send_email_report("🔴 ETL FAILED: Shufersal", error_body)
        raise e