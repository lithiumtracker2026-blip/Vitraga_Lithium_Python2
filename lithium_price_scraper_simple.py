#!/usr/bin/env python3
"""
Lithium Price Scraper - Simple Version (No Selenium)
Scrapes real-time lithium carbonate prices from Trading Economics using requests
"""

import requests
from bs4 import BeautifulSoup
import logging
import re
from datetime import datetime
from database_config import get_curser
from database_operations import insert_lithium_price

def scrape_lithium_price_simple():
    """Scrape lithium price data from Trading Economics website using requests"""
    url = "https://tradingeconomics.com/commodity/lithium"
    
    try:
        logging.info(f"🌐 Fetching Trading Economics lithium page...")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        logging.info("✅ Page fetched successfully")
        
        # Parse HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        page_text = soup.get_text()
        
        lithium_data = None
        
        # Strategy 1: Parse page text for price patterns with change information (FIRST - most accurate)
        try:
            logging.info("🔍 Analyzing page text for lithium price patterns with change data...")
            
            # Look for the text pattern "rose to X CNY/T on DATE, up Y% from the previous day"
            change_pattern = r'rose to\s+([\d,]+(?:\.\d+)?)\s+CNY/T.*?up\s+([\d.]+)%\s+from the previous day'
            change_match = re.search(change_pattern, page_text, re.IGNORECASE)
            
            if change_match:
                try:
                    price = float(change_match.group(1).replace(',', ''))
                    change_percent = float(change_match.group(2))
                    
                    # Calculate previous price from percentage
                    previous_price = price / (1 + change_percent / 100)
                    price_change = price - previous_price
                    
                    lithium_data = {
                        'price': price,
                        'previous_price': previous_price,
                        'price_change': price_change,
                        'price_change_percent': change_percent,
                        'currency': 'CNY',
                        'unit': 'CNY/T',
                        'scraped_at': datetime.now(),
                        'source': 'Trading Economics'
                    }
                    logging.info(f"✅ Found price via text parsing: {price} CNY/T (+{change_percent}%, Change: +{price_change:.2f})")
                except (ValueError, ZeroDivisionError) as e:
                    logging.error(f"Error calculating price change: {e}")
                    
        except Exception as e:
            logging.error(f"❌ Error in text parsing strategy: {e}")
        
        # Strategy 2: Look for table with Actual/Previous values
        if not lithium_data:
            try:
                # Find all table rows
                tables = soup.find_all('table')
                for table in tables:
                    rows = table.find_all('tr')
                    for row in rows:
                        cells = row.find_all('td')
                        if len(cells) >= 2:
                            # Look for Actual and Previous values
                            cell_texts = [cell.get_text(strip=True) for cell in cells]
                            
                            # Check if this row contains price data
                            for i, text in enumerate(cell_texts):
                                # Look for numbers that could be lithium prices (50,000 - 6,000,000 CNY/T)
                                numbers = re.findall(r'(\d+(?:,\d+)*(?:\.\d+)?)', text)
                                if numbers:
                                    try:
                                        price = float(numbers[0].replace(',', ''))
                                        if 50000 <= price <= 6000000:
                                            # Try to get previous price from next cell
                                            previous_price = price
                                            if i + 1 < len(cell_texts):
                                                prev_numbers = re.findall(r'(\d+(?:,\d+)*(?:\.\d+)?)', cell_texts[i + 1])
                                                if prev_numbers:
                                                    try:
                                                        prev_val = float(prev_numbers[0].replace(',', ''))
                                                        if 50000 <= prev_val <= 6000000:
                                                            previous_price = prev_val
                                                    except ValueError:
                                                        pass
                                            
                                            price_change = price - previous_price
                                            price_change_percent = (price_change / previous_price * 100) if previous_price != 0 else 0
                                            
                                            lithium_data = {
                                                'price': price,
                                                'previous_price': previous_price,
                                                'price_change': price_change,
                                                'price_change_percent': price_change_percent,
                                                'currency': 'CNY',
                                                'unit': 'CNY/T',
                                                'scraped_at': datetime.now(),
                                                'source': 'Trading Economics'
                                            }
                                            logging.info(f"✅ Found lithium price in table: {price} CNY/T (Previous: {previous_price}, Change: {price_change:+.2f})")
                                            break
                                    except ValueError:
                                        continue
                            if lithium_data:
                                break
                    if lithium_data:
                        break
            except Exception as e:
                logging.error(f"❌ Error in table parsing: {e}")
        
        # Strategy 3: Parse page text for basic price patterns (fallback)
        if not lithium_data:
            try:
                logging.info("🔍 Analyzing page text for lithium price patterns...")
                
                # Look for the text pattern "rose to X CNY/T on DATE, up Y% from the previous day"
                change_pattern = r'rose to\s+([\d,]+(?:\.\d+)?)\s+CNY/T.*?up\s+([\d.]+)%\s+from the previous day'
                change_match = re.search(change_pattern, page_text, re.IGNORECASE)
                
                if change_match:
                    try:
                        price = float(change_match.group(1).replace(',', ''))
                        change_percent = float(change_match.group(2))
                        
                        # Calculate previous price from percentage
                        previous_price = price / (1 + change_percent / 100)
                        price_change = price - previous_price
                        
                        lithium_data = {
                            'price': price,
                            'previous_price': previous_price,
                            'price_change': price_change,
                            'price_change_percent': change_percent,
                            'currency': 'CNY',
                            'unit': 'CNY/T',
                            'scraped_at': datetime.now(),
                            'source': 'Trading Economics'
                        }
                        logging.info(f"✅ Found price via text parsing: {price} CNY/T (+{change_percent}%)")
                    except (ValueError, ZeroDivisionError) as e:
                        logging.error(f"Error calculating price change: {e}")
                
                # If still no data, try other patterns
                if not lithium_data:
                    price_patterns = [
                        r'(\d{5,7}(?:,\d+)*(?:\.\d+)?)\s+CNY/T',
                        r'Actual["\s:]+(\d+(?:,\d+)*(?:\.\d+)?)',
                        r'trade at\s+(\d+(?:,\d+)*(?:\.\d+)?)\s+CNY',
                    ]
                    
                    for pattern in price_patterns:
                        matches = re.findall(pattern, page_text, re.IGNORECASE)
                        if matches:
                            for price_str in matches:
                                try:
                                    price = float(price_str.replace(',', ''))
                                    # Reasonable lithium price range (CNY/T)
                                    if 50000 <= price <= 6000000:
                                        lithium_data = {
                                            'price': price,
                                            'previous_price': price,
                                            'price_change': 0.0,
                                            'price_change_percent': 0.0,
                                            'currency': 'CNY',
                                            'unit': 'CNY/T',
                                            'scraped_at': datetime.now(),
                                            'source': 'Trading Economics (Text Parse)'
                                        }
                                        logging.info(f"✅ Found price via text parsing: {price} CNY/T")
                                        break
                                except ValueError:
                                    continue
                            if lithium_data:
                                break
                            
            except Exception as e:
                logging.error(f"❌ Error in text parsing: {e}")
        
        # If no real data found, return None
        if not lithium_data:
            logging.error("❌ Could not scrape real data from Trading Economics website")
            return None
        
        return lithium_data
        
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error fetching page: {e}")
        return None
    except Exception as e:
        logging.error(f"❌ Error in scraper: {e}")
        return None

def main():
    """Main function to scrape and store lithium price data"""
    logging.info("🚀 Starting Lithium Price Scraper (Simple Version)")
    
    try:
        # Get database connection
        connection, cursor = get_curser()
        logging.info("✅ Connected to database")
        
        # Scrape lithium data
        lithium_data = scrape_lithium_price_simple()
        
        if not lithium_data:
            logging.error("❌ No real lithium data could be scraped from Trading Economics website")
            logging.error("❌ Skipping database insertion - only real data allowed")
            return
        
        # Insert data into database
        insert_lithium_price(cursor, connection, lithium_data)
        logging.info("✅ Lithium price data inserted into database")
        
        logging.info(f"🎉 Lithium price scraping completed: {lithium_data['price']} {lithium_data['unit']} ({lithium_data['price_change']:+.2f}, {lithium_data['price_change_percent']:+.2f}%)")
        
    except Exception as e:
        logging.error(f"❌ Error in Lithium price scraper: {e}")
        raise
    
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()
        logging.info("✅ Database connection closed")

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )
    main()
