"""
Booking.com Reviews Scraper - Database Handler
Developed by Muhammad Taha
"""

import fdb
import hashlib
import json
import re
import logging
from datetime import datetime
from typing import Dict, List

def clean_text(text: str) -> str:
    """Clean text by removing problematic characters and converting to WIN1252 compatible format"""
    if not text:
        return None
        
    # Remove emojis and other non-WIN1252 characters
    emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        "]+", flags=re.UNICODE)
    text = emoji_pattern.sub('', text)
    
    # Replace line breaks with spaces
    text = re.sub(r'[\r\n]+', ' ', text)
    
    # Try to encode as WIN1252, replace characters that don't fit
    try:
        # First encode to WIN1252 with replacement character
        encoded = text.encode('cp1252', errors='replace')
        # Then decode back to string
        text = encoded.decode('cp1252')
    except (UnicodeEncodeError, UnicodeDecodeError):
        # If that fails, try an aggressive cleaning approach
        text = ''.join(char for char in text if ord(char) < 256)
    
    text = text.strip()
    return text if text else None

class FirebirdHandler:
    def __init__(self, dsn: str, user: str, password: str):
        self.connection = fdb.connect(
            dsn=dsn,
            user=user,
            password=password,
            charset='WIN1252'
        )
        self.logger = logging.getLogger(__name__)
        
    def generate_hash(self, review: Dict) -> str:
        """Generate a hash based on unique review attributes to detect duplicates"""
        hash_string = f"{review.get('username', '')}{review.get('review_post_date', '')}{review.get('review_title', '')}"
        return hashlib.md5(hash_string.encode('cp1252', errors='replace')).hexdigest()[:13]

    def insert_review(self, review: Dict, hotel_info: Dict) -> bool:
        """Insert a single review into the database"""
        try:
            cur = self.connection.cursor()
            
            # Generate hash for duplicate detection
            review_hash = self.generate_hash(review)
            
            # Check for duplicate
            cur.execute("SELECT COUNT(*) FROM A_REW WHERE hash = ?", (review_hash,))
            if cur.fetchone()[0] > 0:
                self.logger.info(f"Duplicate review found with hash {review_hash}, skipping...")
                return False
                
            # Convert date string to proper date object
            try:
                review_date = datetime.strptime(review.get('review_post_date', ''), '%m-%d-%Y %H:%M:%S').date()
            except (TypeError, ValueError):
                review_date = None
            
            # Clean and validate text fields
            biz_name = clean_text(hotel_info.get('name'))
            biz_city = clean_text(hotel_info.get('city'))
            biz_country = clean_text(hotel_info.get('country'))
            username = clean_text(review.get('username'))
            user_country = clean_text(review.get('user_country'))
            room_view = clean_text(review.get('room_view'))
            stay_type = clean_text(review.get('stay_type'))
            review_title = clean_text(review.get('review_title'))
            original_lang = clean_text(review.get('original_lang'))
            review_text_liked = clean_text(review.get('review_text_liked'))
            review_text_disliked = clean_text(review.get('review_text_disliked'))
            
            # Convert numeric fields
            stay_duration = None
            if review.get('stay_duration'):
                try:
                    if isinstance(review['stay_duration'], str):
                        # Try to extract just the number from strings like "1 night"
                        stay_duration = int(re.search(r'\d+', review['stay_duration']).group())
                    else:
                        stay_duration = int(review['stay_duration'])
                except (TypeError, ValueError, AttributeError):
                    stay_duration = None
                
            try:
                rating = int(float(review.get('rating'))) if review.get('rating') else None
            except (TypeError, ValueError):
                rating = None
                
            try:
                found_helpful = int(review.get('found_helpful', 0))
            except (TypeError, ValueError):
                found_helpful = 0
                
            try:
                found_unhelpful = int(review.get('found_unhelpful', 0))
            except (TypeError, ValueError):
                found_unhelpful = 0
            
            # Prepare BLOB fields with WIN1252 encoding
            full_review = None
            if any([review_title, review_text_liked, review_text_disliked]):
                full_review_dict = {
                    'title': review_title or '',
                    'liked': review_text_liked or '',
                    'disliked': review_text_disliked or ''
                }
                try:
                    full_review = json.dumps(full_review_dict, ensure_ascii=False).encode('cp1252', errors='replace')
                except UnicodeEncodeError:
                    self.logger.warning("Could not encode full_review as WIN1252")
            
            en_full_review = None
            if review.get('en_full_review'):
                cleaned = clean_text(review['en_full_review'])
                if cleaned:
                    try:
                        en_full_review = cleaned.encode('cp1252', errors='replace')
                    except UnicodeEncodeError:
                        self.logger.warning("Could not encode en_full_review as WIN1252")
            
            owner_response = None
            if review.get('owner_resp_text'):
                cleaned = clean_text(review['owner_resp_text'])
                if cleaned:
                    try:
                        owner_response = cleaned.encode('cp1252', errors='replace')
                    except UnicodeEncodeError:
                        self.logger.warning("Could not encode owner_response as WIN1252")

            # Log sample of values for debugging
            self.logger.debug(f"Inserting review with values:")
            self.logger.debug(f"biz_name: {biz_name}")
            self.logger.debug(f"username: {username}")
            self.logger.debug(f"review_title: {review_title}")

            # Build the SQL with parameters
            sql = """
                INSERT INTO A_REW (
                    id_rec, biz_name, biz_city, biz_country,
                    username, user_country, room_view, stay_duration,
                    stay_type, review_post_date, review_title, rating,
                    original_lang, review_text_liked, review_text_disliked,
                    full_review, en_full_review, found_helpful,
                    found_unhelpful, owner_resp_text, hash
                ) VALUES (
                    GEN_UUID(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
            """

            params = (
                biz_name or '', biz_city or '', biz_country or '',
                username or '', user_country or '', room_view or '',
                stay_duration, stay_type or '', review_date,
                review_title or '', rating, original_lang or '',
                review_text_liked or '', review_text_disliked or '',
                full_review, en_full_review, found_helpful,
                found_unhelpful, owner_response, review_hash
            )

            cur.execute(sql, params)
            self.connection.commit()
            return True
            
        except Exception as e:
            self.logger.error(f"Error inserting review: {str(e)}")
            if hasattr(e, '__dict__'):
                self.logger.error(f"Error details: {e.__dict__}")
            self.connection.rollback()
            return False

    def insert_reviews(self, reviews: List[Dict], hotel_info: Dict) -> int:
        """Insert multiple reviews into the database"""
        success_count = 0
        total_count = len(reviews)
        for i, review in enumerate(reviews, 1):
            if self.insert_review(review, hotel_info):
                success_count += 1
                if success_count % 10 == 0:
                    self.logger.info(f"Successfully inserted {success_count}/{total_count} reviews")
        return success_count

    def close(self):
        """Close the database connection"""
        if self.connection:
            self.connection.close()