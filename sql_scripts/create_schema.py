import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# טעינת מחרוזת ההתחברות המאובטחת שלנו
load_dotenv()
db_url = os.getenv("SUPABASE_DATABASE_URL")

# פקודות ה-SQL ליצירת הטבלאות (DDL)
# שים לב לסדר: קודם טבלאות הממדים (Dim), ורק בסוף טבלת העובדות (Fact) שמפנה אליהן.
create_tables_sql = """
-- 1. טבלת רשתות
CREATE TABLE IF NOT EXISTS Dim_Chains (
    chain_id VARCHAR(50) PRIMARY KEY,
    chain_name VARCHAR(255) NOT NULL
);

-- 2. טבלת סניפים
CREATE TABLE IF NOT EXISTS Dim_Stores (
    store_id VARCHAR(100) PRIMARY KEY, -- שילוב של קוד רשת וקוד סניף למניעת כפילויות
    chain_id VARCHAR(50) REFERENCES Dim_Chains(chain_id),
    store_name VARCHAR(255),
    city VARCHAR(255),
    region VARCHAR(255)
);

-- 3. טבלת מוצרים
CREATE TABLE IF NOT EXISTS Dim_Products (
    barcode VARCHAR(50) PRIMARY KEY,
    item_name VARCHAR(255),
    category VARCHAR(255),
    manufacturer VARCHAR(255)
);

-- 4. טבלת מחירים (עובדות)
CREATE TABLE IF NOT EXISTS Fact_Prices (
    price_id SERIAL PRIMARY KEY, -- מזהה שורה אוטומטי
    barcode VARCHAR(50) REFERENCES Dim_Products(barcode),
    store_id VARCHAR(100) REFERENCES Dim_Stores(store_id),
    chain_id VARCHAR(50) REFERENCES Dim_Chains(chain_id),
    sample_date TIMESTAMP,
    price DECIMAL(10, 2)
);
"""

def create_schema():
    print("מתחיל בבניית הסכימה במסד הנתונים...")
    
    try:
        engine = create_engine(db_url)
        with engine.begin() as connection: # begin() פותח טרנזקציה שנסגרת אוטומטית בסיום
            # הרצת כל פקודות ה-SQL
            connection.execute(text(create_tables_sql))
            
        print("======================================")
        print("הסכימה (Star Schema) נוצרה בהצלחה! 🏗️")
        print("כל הטבלאות (Dim_Chains, Dim_Stores, Dim_Products, Fact_Prices) מוכנות.")
        print("======================================")
        
    except Exception as e:
        print("שגיאה ביצירת הטבלאות:")
        print(e)

if __name__ == "__main__":
    create_schema()