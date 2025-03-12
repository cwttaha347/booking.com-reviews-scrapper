"""
Booking.com Reviews Scraper - Database Initialization
Developed by Muhammad Taha
"""

import os
import fdb

def init_database(dsn, user, password):
    # Handle Windows paths correctly
    if ':' in dsn:
        parts = dsn.split(':')
        if len(parts) > 2:  # Handle case like localhost:C:/path/to/db.fdb
            db_path = ':'.join(parts[1:])  # Rejoin if path contains colons
        else:
            db_path = parts[1]
    else:
        db_path = dsn
        
    db_dir = os.path.dirname(db_path)
    
    # Create directory if it doesn't exist and it's not empty
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir)
    
    # Create database if it doesn't exist, with WIN1252 as default character set
    # This allows storing most Western European characters and is more compatible with Windows
    if not os.path.exists(db_path):
        connection = fdb.create_database(
            f"CREATE DATABASE '{dsn}' "
            f"USER '{user}' PASSWORD '{password}' "
            f"DEFAULT CHARACTER SET WIN1252 "
            f"COLLATION WIN1252"
        )
    else:
        connection = fdb.connect(dsn=dsn, user=user, password=password, charset='WIN1252')

    cur = connection.cursor()
    
    try:
        # Create domains with explicit character sets
        cur.execute("""CREATE DOMAIN D_CODE AS VARCHAR(5) CHARACTER SET WIN1252""")
        cur.execute("""CREATE DOMAIN D_CODE_LONG AS VARCHAR(13) CHARACTER SET WIN1252""")
        cur.execute("""CREATE DOMAIN D_CODE_SHORT AS VARCHAR(1) CHARACTER SET WIN1252""")
        cur.execute("""CREATE DOMAIN D_DATE AS DATE""")
        cur.execute("""CREATE DOMAIN D_LONG_STRING AS VARCHAR(100) CHARACTER SET WIN1252""")
        cur.execute("""CREATE DOMAIN D_VERY_LONG_STRING AS VARCHAR(1000) CHARACTER SET WIN1252""")
        cur.execute("""CREATE DOMAIN D_INT AS INTEGER""")
        cur.execute("""CREATE DOMAIN D_BLOB AS BLOB SUB_TYPE TEXT CHARACTER SET WIN1252""")
        cur.execute("""CREATE DOMAIN D_GUID AS CHAR(36) CHARACTER SET WIN1252""")
        
        # Create main reviews table
        cur.execute("""
            CREATE TABLE A_REW (
                id_rec D_GUID NOT NULL,
                biz_name D_LONG_STRING,
                biz_city D_LONG_STRING,
                biz_country D_LONG_STRING,
                username D_LONG_STRING,
                user_country D_LONG_STRING,
                room_view D_VERY_LONG_STRING,
                stay_duration D_INT,
                stay_type D_LONG_STRING,
                review_post_date D_DATE,
                review_title D_VERY_LONG_STRING,
                rating D_INT,
                original_lang D_CODE,
                review_text_liked D_VERY_LONG_STRING,
                review_text_disliked D_VERY_LONG_STRING,
                full_review D_BLOB,
                en_full_review D_BLOB,
                found_helpful D_INT,
                found_unhelpful D_INT,
                owner_resp_text D_BLOB,
                hash D_CODE_LONG
            )
        """)
        
        connection.commit()
        print("Database initialized successfully with WIN1252 character set")
        
    except fdb.fbcore.DatabaseError as e:
        if "unsuccessful metadata update" in str(e) and "table" in str(e):
            print("Database schema already exists")
        else:
            raise
    finally:
        connection.close()

if __name__ == "__main__":
    import yaml
    
    # Load config
    with open("config.yml", "r") as f:
        config = yaml.safe_load(f)
    
    init_database(
        config['DB_DSN'],
        config['DB_USER'],
        config['DB_PASSWORD']
    )