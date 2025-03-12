"""
Booking.com Reviews Scraper
Developed by Muhammad Taha
"""

import argparse
import logging
import yaml
from core.scrape import Scrape
from core.db_handler import FirebirdHandler

def load_config():
    with open("config.yml", "r") as file:
        return yaml.safe_load(file)

def main():
    parser = argparse.ArgumentParser(description='Booking.com Reviews Scraper')
    
    # Required parameters
    parser.add_argument('hotel_name', type=str, help='Hotel name from booking.com URL')
    parser.add_argument('country', type=str, help='Country code (e.g., us, uk)')
    
    # Optional parameters
    parser.add_argument('--sort-by', type=str, default='newest_first',
                      choices=['most_relevant', 'newest_first', 'oldest_first', 'highest_scores', 'lowest_scores'],
                      help='Sort reviews by (default: newest_first)')
    parser.add_argument('--n-reviews', type=int, default=-1,
                      help='Number of reviews to scrape. Default is all (-1)')
    parser.add_argument('--stop-criteria-username', type=str,
                      help='Stop when this username is found')
    parser.add_argument('--stop-criteria-review-title', type=str,
                      help='Stop when this review title is found')
    parser.add_argument('--no-save-files', action='store_true',
                      help='Disable saving CSV/Excel files')
    
    # Optional database override parameters
    parser.add_argument('--db-dsn', type=str, help='Override Firebird database connection string')
    parser.add_argument('--db-user', type=str, help='Override database user')
    parser.add_argument('--db-password', type=str, help='Override database password')
    parser.add_argument('--hotel-city', type=str, help='Hotel city for database record')
    
    args = parser.parse_args()
    
    # Load config
    config = load_config()
    
    # Initialize database handler with config values, override with args if provided
    db = FirebirdHandler(
        args.db_dsn if args.db_dsn else config.get('DB_DSN'),
        args.db_user if args.db_user else config.get('DB_USER'),
        args.db_password if args.db_password else config.get('DB_PASSWORD')
    )
    
    # Prepare hotel info for database
    hotel_info = {
        'name': args.hotel_name,
        'city': args.hotel_city if args.hotel_city else 'Unknown',  # Default to Unknown if not provided
        'country': args.country
    }
    
    # Prepare input parameters
    input_params = {
        'country': args.country,
        'hotel_name': args.hotel_name,
        'sort_by': args.sort_by,
        'n_rows': args.n_reviews
    }
    
    if args.stop_criteria_username and args.stop_criteria_review_title:
        input_params['stop_critera'] = {
            'username': args.stop_criteria_username,
            'review_text_title': args.stop_criteria_review_title
        }
    
    try:
        # Initialize scraper with save_data_to_disk=True by default
        scraper = Scrape(input_params, save_data_to_disk=not args.no_save_files)
        reviews = scraper.run()
        
        # Save to database
        success_count = db.insert_reviews(reviews, hotel_info)
        print(f"Successfully saved {success_count} reviews to database")
        
    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    main()
