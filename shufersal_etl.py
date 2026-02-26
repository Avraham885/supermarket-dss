import os
import gzip
import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine, text
import lxml.etree as ET
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

BASE_URL = "http://prices.shufersal.co.il/"
CHAIN_ID = "7290027600007"
CHAIN_NAME = "שופרסל"

# יצירת תיקיות זמניות אם לא קיימות
DATA_DIR = "ETL_Process_Shufersal"
STORES_DIR = os.path.join(DATA_DIR, "stores")
PRICES_DIR = os.path.join(DATA_DIR, "prices")
os.makedirs(STORES_DIR, exist_ok=True)
os.makedirs(PRICES_DIR, exist_ok=True)

# ==========================================
# EMAIL CONFIGURATION
# ==========================================
# משתני סביבה שנגדיר בהמשך ב-GitHub עבור שליחת המייל
EMAIL_SENDER = os.environ.get("EMAIL_SENDER") 
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD") 
EMAIL_RECEIVER = os.environ.get("EMAIL_RECEIVER")

def send_email_report(subject, body):
    """פונקציה לשליחת מייל התראה מפורט"""
    if not all([EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECEIVER]):
        print("[WARNING] Email credentials not fully set. Skipping email alert.")
        return

    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_SENDER
        msg['To'] = EMAIL_RECEIVER
        msg['Subject'] = subject

        msg.attach(MIMEText(body, 'plain', 'utf-8'))

        # התחברות לשרת Gmail
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()
        print("[SUCCESS] Email report sent successfully.")
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
    """פונקציה שמנקה את שם העיר בזמן אמת לפי המילונים"""
    if not isinstance(city_name, str) or city_name.strip() == '': 
        return 'לא ידוע'
    
    city_name = city_name.strip()
    
    # המרה לפי מילון שגיאות כתיב 
    if city_name in CITY_MAPPING: 
        return CITY_MAPPING[city_name]
        
    # טיפול גורף בכל עיר שמתחילה ב'קרית ' והפיכתה ל'קריית '
    if city_name.startswith('קרית '): 
        return city_name.replace('קרית ', 'קריית ')
        
    return city_name

# ==========================================
# ETL LOGIC
# ==========================================
def get_download_links():
    links = []
    print("[INFO] Connecting to Shufersal website to fetch links...")
    session = requests.Session()
    
    for page in range(1, 150):
        #print(f"[INFO] Scanning page {page}...") # הוסתר כדי למנוע ספאם בלוגים של גיטהאב
        url = f"{BASE_URL}?page={page}"
        resp = session.get(url)
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        table = soup.find('table')
        if not table: break
        
        rows = table.find_all('tr')[1:]
        if not rows: break
        
        for row in rows:
            cols = row.find_all('td')
            if len(cols) > 0:
                fname = cols[0].text.strip()
                link = cols[0].find('a')['href']
                
                # ניקח רק סניפים רגילים, או מחירים רגילים 
                if ("Stores" in fname or "PriceFull" in fname) and "Promo" not in fname and "Null" not in fname:
                    print(f"  [+] Found: {fname}")
                    links.append((fname, link))
                    
        if len(links) >= 7:  # הגבלה ל-6 סניפים + 1 קובץ Stores לבדיקות שלנו
            break
            
    return links

def fast_parse_xml(file_path, tag_name):
    records = []
    with gzip.open(file_path, 'rb') as f:
        context = ET.iterparse(f, events=('end',), tag=tag_name)
        for event, elem in context:
            record = {child.tag: child.text for child in elem}
            records.append(record)
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
    return pd.DataFrame(records)

def run_full_etl():
    print("======================================")
    print("[START] Starting STREAMING ETL for Shufersal...")
    print("======================================")
    
    start_time = datetime.now()
    stats = {"stores_files": 0, "price_files": 0, "total_prices_inserted": 0}

    # יצירת הרשת במסד הנתונים אם לא קיימת
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

    # --- שלב א: קבצי סניפים וערים ---
    for fname, url in stores_links:
        print(f"\n[STEP] Processing Stores: {fname}")
        local_path = os.path.join(STORES_DIR, fname + ".gz")
        resp = requests.get(url)
        with open(local_path, 'wb') as f: f.write(resp.content)
        
        df = fast_parse_xml(local_path, 'STORE')
        df.columns = [c.upper() for c in df.columns]
        df = df.rename(columns={'STOREID': 'StoreId', 'STORENAME': 'StoreName', 'CITY': 'City'})
        
        # נרמול שמות הערים באמצעות הפונקציה שלנו
        df['City'] = df['City'].apply(normalize_city_name)
        
        with engine.begin() as conn:
            # הזרקת ערים ומחוזות (מונע כפילויות)
            cities = df[['City']].drop_duplicates().rename(columns={'City': 'city_name'})
            cities['region'] = cities['city_name'].map(lambda x: REGION_MAPPING.get(x, 'לא מוגדר'))
            
            for idx, row in cities.iterrows():
                conn.execute(text('INSERT INTO "Dim_City" (city_name, region) VALUES (:city_name, :region) ON CONFLICT (city_name) DO UPDATE SET region = EXCLUDED.region'), row.to_dict())
            
            # הזרקת סניפים
            df['store_id'] = CHAIN_ID + "-" + df['StoreId'].astype(str).str.zfill(3)
            df['chain_id'] = CHAIN_ID
            stores_to_db = df[['store_id', 'chain_id', 'StoreName', 'City']].rename(columns={'StoreName': 'store_name', 'City': 'city'})
            
            # נשתמש בטבלה זמנית לעדכון מהיר של סניפים
            stores_to_db.to_sql('temp_stores', conn, if_exists='replace', index=False)
            conn.execute(text("""
                INSERT INTO "Dim_Stores" (store_id, chain_id, store_name, city)
                SELECT store_id, chain_id, store_name, city FROM temp_stores
                ON CONFLICT (store_id) DO UPDATE SET store_name = EXCLUDED.store_name, city = EXCLUDED.city;
            """))
            conn.execute(text("DROP TABLE temp_stores;"))
            
        print(f"  [SUCCESS] Dim_Stores and Dim_City updated.")

    # --- שלב ב: קבצי מחירים ומוצרים ---
    for fname, url in price_links:
        print(f"\n[STEP] Processing Prices: {fname}")
        local_path = os.path.join(PRICES_DIR, fname + ".gz")
        resp = requests.get(url)
        with open(local_path, 'wb') as f: f.write(resp.content)
        
        df = fast_parse_xml(local_path, 'Item')
        
        products = df[['ItemCode', 'ItemName', 'ManufacturerName']].drop_duplicates(subset=['ItemCode']).copy()
        products = products.rename(columns={'ItemCode': 'barcode', 'ItemName': 'item_name', 'ManufacturerName': 'manufacturer'})
        products['category'] = 'כללי'
        
        prices = df[['ItemCode', 'PriceUpdateDate', 'ItemPrice']].copy()
        prices = prices.rename(columns={'ItemCode': 'barcode', 'PriceUpdateDate': 'sample_date', 'ItemPrice': 'price'})
        prices['chain_id'] = CHAIN_ID
        store_num = fname.split('-')[1].split('_')[0] if '-' in fname else "001"
        prices['store_id'] = f"{CHAIN_ID}-{store_num}"
        prices['sample_date'] = pd.to_datetime(prices['sample_date'])

        print(f"  [DB] Injecting {len(prices)} rows to Supabase...")
        with engine.begin() as conn:
            # הזרקה מהירה של מוצרים (Bulk)
            products.to_sql('temp_products', conn, if_exists='replace', index=False)
            conn.execute(text("""
                INSERT INTO "Dim_Products" (barcode, item_name, category, manufacturer)
                SELECT barcode, item_name, category, manufacturer FROM temp_products
                ON CONFLICT (barcode) DO NOTHING;
            """))
            conn.execute(text("DROP TABLE temp_products;"))
            
            # הזרקת מחירים (Bulk)
            prices.to_sql('Fact_Prices', conn, if_exists='append', index=False, chunksize=1000, method='multi')
        
        stats["total_prices_inserted"] += len(prices)
        print(f"  [SUCCESS] Store {store_num} prices inserted.")

    end_time = datetime.now()
    duration = round((end_time - start_time).total_seconds() / 60, 2)
    
    print("\n======================================")
    print(f"[DONE] 🎉 All data processed successfully in {duration} minutes!")
    print("======================================")
    
    # שליחת מייל הצלחה
    report_body = f"""Shufersal Data Pipeline - SUCCESS 🟢

Run Time: {duration} minutes
Store Files Processed: {stats['stores_files']}
Price Files Processed: {stats['price_files']}
Total Price Rows Inserted: {stats['total_prices_inserted']}

Your Supermarket DSS is up to date! 🚀
"""
    send_email_report("🟢 ETL Success: Shufersal", report_body)

if __name__ == "__main__":
    try:
        run_full_etl()
    except Exception as e:
        error_tb = traceback.format_exc()
        print(f"\n[CRITICAL ERROR] Pipeline failed:\n{error_tb}")
        
        # שליחת מייל כישלון
        error_body = f"""Shufersal Data Pipeline - FAILED 🔴

An error occurred during the ETL process.
Error details:
{error_tb}

Please check GitHub Actions logs.
"""
        send_email_report("🔴 ETL FAILED: Shufersal", error_body)
        raise e  # זריקת השגיאה הלאה כדי שגיטהאב יידע שהסקריפט נכשל