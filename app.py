#!/usr/bin/env python3
"""
Main Application - Lithium Stock Data Collection Pipeline
Process-based execution: 
- process1 = most followed stocks fetcher (lithium stocks)
- process2 = metal price fetcher (lithium ETF, aluminum, nickel, zinc, platinum)
- process3 = lithium price scraper (Trading Economics)
- process4 = insider transactions fetcher (lithium stocks)

IMPORTANT: Process is updated BEFORE execution to prevent getting stuck on failures
"""

import logging
import sys
import time
from datetime import datetime

# Import our modules
from most_followed import get_most_followed_data
from copper_price_fetcher import main as run_metal_price_fetcher
from lithium_price_scraper_simple import main as run_lithium_scraper
from insider_transactions_fetcher import main as run_insider_transactions_fetcher
from database_config import get_curser
from database_operations import update_process_status

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)


def get_current_process():
    """Get current process from database"""
    try:
        connection, cursor = get_curser()
        cursor.execute("SELECT current_process FROM process_python2 LIMIT 1")
        result = cursor.fetchone()
        cursor.close()
        connection.close()
        return result[0] if result else "process1"
    except:
        return "process1"

def main():
    """Main pipeline function - runs based on current process"""
    current_process = get_current_process()
    logging.info(f"🔍 DEBUG: Current process from DB: '{current_process}'")
    logging.info(f"🔍 DEBUG: Type: {type(current_process)}")
    logging.info(f"🔍 DEBUG: Length: {len(current_process)}")
    logging.info(f"🔍 DEBUG: Repr: {repr(current_process)}")
    
    # Strip any whitespace just in case
    current_process = current_process.strip()
    
    logging.info(f"🔍 DEBUG: After strip: '{current_process}'")
    logging.info(f"🔍 DEBUG: Equals 'process1': {current_process == 'process1'}")
    logging.info(f"🔍 DEBUG: Equals 'process2': {current_process == 'process2'}")
    logging.info(f"🔍 DEBUG: Equals 'process3': {current_process == 'process3'}")
    
    if current_process == "process1":
        # Most followed stocks fetcher
        logging.info("🚀 STARTING PROCESS 1: MOST FOLLOWED STOCKS FETCHER")
        
        # Update to next process FIRST (before execution)
        connection, cursor = get_curser()
        logging.info("✅ Updating to process2 BEFORE execution")
        update_process_status(cursor, connection, "process2")
        cursor.close()
        connection.close()
        
        try:
            get_most_followed_data()
            logging.info("✅ Process 1 completed successfully")
        except Exception as e:
            logging.error(f"❌ Error in process1: {e}")
            
    elif current_process == "process2":
        # Metal price fetcher
        logging.info("🚀 STARTING PROCESS 2: METAL PRICE FETCHER")
        
        # Update to next process FIRST (before execution)
        connection, cursor = get_curser()
        logging.info("✅ Updating to process3 BEFORE execution")
        update_process_status(cursor, connection, "process3")
        cursor.close()
        connection.close()
        
        try:
            run_metal_price_fetcher()
            logging.info("✅ Process 2 completed successfully")
        except Exception as e:
            logging.error(f"❌ Error in process2: {e}")
            
    elif current_process == "process3":
        # Lithium price scraper
        logging.info("🚀 STARTING PROCESS 3: LITHIUM PRICE SCRAPER")
        
        # Update to next process FIRST (before execution) - cycling to process4
        connection, cursor = get_curser()
        logging.info("✅ Updating to process4 BEFORE execution")
        update_process_status(cursor, connection, "process4")
        cursor.close()
        connection.close()
        
        try:
            logging.info("🌐 About to run lithium price scraper...")
            run_lithium_scraper()
            logging.info("✅ Process 3 completed successfully")
        except Exception as e:
            logging.error(f"❌ Error in process3: {e}")
            raise
    
    elif current_process == "process4":
        # Insider transactions fetcher
        logging.info("🚀 STARTING PROCESS 4: INSIDER TRANSACTIONS FETCHER")
        
        # Update to next process FIRST (before execution) - cycling back to process1
        connection, cursor = get_curser()
        logging.info("✅ Updating to process1 BEFORE execution (cycling back)")
        update_process_status(cursor, connection, "process1")
        cursor.close()
        connection.close()
        
        try:
            logging.info("📊 About to run insider transactions fetcher...")
            run_insider_transactions_fetcher()
            logging.info("✅ Process 4 completed successfully")
        except Exception as e:
            logging.error(f"❌ Error in process4: {e}")
            raise
    else:
        # Default to process1 if unknown process
        logging.warning(f"❌ Unknown process: '{current_process}', defaulting to process1")
        connection, cursor = get_curser()
        update_process_status(cursor, connection, "process1")
        cursor.close()
        connection.close()
        
        # Run process1 as fallback
        try:
            get_most_followed_data()
            logging.info("✅ Fallback process 1 completed")
        except Exception as e:
            logging.error(f"❌ Error in fallback process1: {e}")

if __name__ == "__main__":
    main()