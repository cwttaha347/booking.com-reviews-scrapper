"""
Booking.com Reviews Scraper - Database Verification
Developed by Muhammad Taha
"""

import yaml
import fdb
import json

def main():
    # Load config
    with open("config.yml", "r") as f:
        config = yaml.safe_load(f)
    
    # Connect to database
    conn = fdb.connect(
        dsn=config['DB_DSN'],
        user=config['DB_USER'],
        password=config['DB_PASSWORD'],
        charset='WIN1252'
    )
    
    try:
        cur = conn.cursor()
        
        # Get total count
        cur.execute("SELECT COUNT(*) FROM A_REW")
        total = cur.fetchone()[0]
        print(f"\nTotal reviews in database: {total}\n")
        
        # Sample some reviews with different languages
        cur.execute(""" 
            SELECT review_title, review_text_liked, original_lang 
            FROM A_REW 
            ORDER BY review_post_date DESC 
            FETCH FIRST 5 ROWS ONLY 
        """)
        
        print("Sample of recent reviews:")
        print("-" * 80)
        for title, liked, lang in cur:
            print(f"Language: {lang}")
            print(f"Title: {title}")
            print(f"Liked: {liked[:100]}..." if liked and len(liked) > 100 else f"Liked: {liked}")
            print("-" * 80)
            
        # Get language distribution
        cur.execute(""" 
            SELECT original_lang, COUNT(*) as cnt 
            FROM A_REW 
            GROUP BY original_lang 
            ORDER BY cnt DESC 
        """)
        
        print("\nReviews by language:")
        for lang, count in cur:
            print(f"{lang}: {count}")
            
    finally:
        conn.close()

if __name__ == "__main__":
    main()