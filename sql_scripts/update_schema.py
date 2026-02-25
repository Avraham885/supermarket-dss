import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# טעינת חיבור למסד הנתונים
load_dotenv()
db_url = os.getenv("SUPABASE_DATABASE_URL")
engine = create_engine(db_url)

def update_schema_to_snowflake():
    print("מתחיל בשדרוג הסכימה ל-Snowflake (הוספת טבלת ערים)...")
    
    sql_commands = text("""
        -- 1. יצירת טבלת הערים החדשה
        CREATE TABLE IF NOT EXISTS "Dim_City" (
            city_name VARCHAR(255) PRIMARY KEY,
            region VARCHAR(255),
            population INT,
            socio_economic_index INT
        );

        -- 2. הכנסת עיר ה"טסט" (כדי שהמפתח הזר לא יקרוס על הנתונים מאתמול)
        INSERT INTO "Dim_City" (city_name, region)
        VALUES ('לא ידוע', 'ארצי')
        ON CONFLICT (city_name) DO NOTHING;

        -- 3. יצירת קשר (Foreign Key) בין טבלת הסניפים לטבלת הערים
        -- שימוש בבלוק DO כדי לוודא שאנחנו לא יוצרים את הקשר פעמיים בטעות
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_store_city') THEN
                ALTER TABLE "Dim_Stores"
                ADD CONSTRAINT fk_store_city
                FOREIGN KEY (city) REFERENCES "Dim_City"(city_name);
            END IF;
        END;
        $$;
    """)
    
    try:
        with engine.begin() as conn:
            conn.execute(sql_commands)
            
        print("======================================")
        print("❄️ הסכימה שודרגה בהצלחה ל-Snowflake!")
        print("טבלת Dim_City נוצרה והקושרה לטבלת Dim_Stores.")
        print("======================================")
        
    except Exception as e:
        print("שגיאה במהלך שדרוג הסכימה:")
        print(e)

if __name__ == "__main__":
    update_schema_to_snowflake()