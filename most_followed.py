import threading
import time
import yfinance as yf
from datetime import datetime, timedelta
import os
from insert_function import insert_most_followed_stock
from database_config import get_curser

server_config = os.getenv("server_config")

# Custom ticker mappings for Yahoo Finance
custom_mappings = {
    'ALB': 'ALB',
    'SQM': 'SQM',
    'LTHM': 'LTHM',
    'LAC': 'LAC',
    'GNENF': 'GNENF',
    'RIO': 'RIO',
    'LITM': 'LITM',
    'QDST': 'QDST',
    'SGML': 'SGML',
    'LIT': 'LIT',
    'LIXT': 'LIXT',
    'LI': 'LI.TO',
    'LIACF': 'LIACF',
    'LIILIF': 'LIILIF',
    'LIACF2': 'LIACF',
    'E3M': 'E3M.V',
    'PMET': 'PMET.TO',
    'FL': 'FL.V',
    'LITM2': 'LITM',
    'PLS': 'PLS.AX',
    'LTR': 'LTR.AX',
    'MIN': 'MIN.AX',
    'IGO': 'IGO.AX',
    'CXO': 'CXO.AX',
    'GL1': 'GL1.AX',
    'VUL': 'VUL.AX',
    'LKE': 'LKE.AX',
    'AGY': 'AGY.AX',
    'AKE': 'AKE.AX',
}

# Exchange suffix mappings for Yahoo Finance
exchange_mappings = {
    'TSXV': '.V',
    'TSX.V': '.V',
    'TSX': '.TO',
    'NYSE': '',
    'NYSE Arca': '',
    'LSE': '.L',
    'LONDON': '.L',
    'ASX': '.AX',
    'CNE': '.CN',
    'BRUSSELS': '.BR',
    'TOKYO': '.T',
    'HKEX': '.HK',
    'OTC': ''
}

# Column 1: Most Watched Lithium
most_watched = [
    {"Name": "Albemarle Corporation", "Country": "United States", "Ticker": "ALB", "tv_ticker": "ALB", "Stock exchange": "NYSE", "stock_exchange_tv": "NYSE"},
    {"Name": "Sociedad Quimica y Minera", "Country": "Chile", "Ticker": "SQM", "tv_ticker": "SQM", "Stock exchange": "NYSE", "stock_exchange_tv": "NYSE"},
    {"Name": "Livent Corporation", "Country": "United States", "Ticker": "LTHM", "tv_ticker": "LTHM", "Stock exchange": "NYSE", "stock_exchange_tv": "NYSE"},
    {"Name": "Lithium Americas", "Country": "Canada", "Ticker": "LAC", "tv_ticker": "LAC", "Stock exchange": "NYSE", "stock_exchange_tv": "NYSE"},
    {"Name": "Ganfeng Lithium (ADR)", "Country": "China", "Ticker": "GNENF", "tv_ticker": "GNENF", "Stock exchange": "OTC", "stock_exchange_tv": "OTC"},
    {"Name": "Rio Tinto (Arcadium)", "Country": "United Kingdom", "Ticker": "RIO", "tv_ticker": "RIO", "Stock exchange": "NYSE", "stock_exchange_tv": "NYSE"},
    {"Name": "Standard Lithium", "Country": "Canada", "Ticker": "LITM", "tv_ticker": "LITM", "Stock exchange": "NYSE", "stock_exchange_tv": "NYSE"},
    {"Name": "Lithium Argentina", "Country": "Canada", "Ticker": "LITM", "tv_ticker": "LITM", "Stock exchange": "NYSE", "stock_exchange_tv": "NYSE"},
    {"Name": "Sigma Lithium", "Country": "Canada", "Ticker": "SGML", "tv_ticker": "SGML", "Stock exchange": "NASDAQ", "stock_exchange_tv": "NASDAQ"},
    {"Name": "Global X Lithium ETF", "Country": "United States", "Ticker": "LIT", "tv_ticker": "LIT", "Stock exchange": "NYSE Arca", "stock_exchange_tv": "AMEX"},
]

# Column 2: North American Leaders
north_american_leaders = [
    {"Name": "Lithium Ionic", "Country": "Canada", "Ticker": "LIILIF", "tv_ticker": "LIILIF", "Stock exchange": "OTC", "stock_exchange_tv": "OTC"},
    {"Name": "Lithium Americas Corp", "Country": "Canada", "Ticker": "LIACF", "tv_ticker": "LIACF", "Stock exchange": "OTC", "stock_exchange_tv": "OTC"},
    {"Name": "E3 Lithium", "Country": "Canada", "Ticker": "E3M", "tv_ticker": "E3M", "Stock exchange": "TSX.V", "stock_exchange_tv": "TSXV"},
    {"Name": "Patriot Battery Metals", "Country": "Canada", "Ticker": "PMET", "tv_ticker": "PMET", "Stock exchange": "TSX.V", "stock_exchange_tv": "TSXV"},
    {"Name": "Frontier Lithium", "Country": "Canada", "Ticker": "FL", "tv_ticker": "FL", "Stock exchange": "TSX.V", "stock_exchange_tv": "TSXV"},
    {"Name": "Standard Lithium", "Country": "Canada", "Ticker": "LITM", "tv_ticker": "LITM", "Stock exchange": "NYSE", "stock_exchange_tv": "NYSE"},
    {"Name": "QuantumScape (batteries)", "Country": "United States", "Ticker": "QDST", "tv_ticker": "QDST", "Stock exchange": "NYSE", "stock_exchange_tv": "NYSE"},
    {"Name": "Euro Manganese", "Country": "Canada", "Ticker": "EEMMF", "tv_ticker": "EEMMF", "Stock exchange": "OTC", "stock_exchange_tv": "OTC"},
    {"Name": "Li Auto (EV)", "Country": "China", "Ticker": "LI", "tv_ticker": "LI", "Stock exchange": "NASDAQ", "stock_exchange_tv": "NASDAQ"},
    {"Name": "Lithium Americas", "Country": "Canada", "Ticker": "LIACF", "tv_ticker": "LIACF", "Stock exchange": "OTC", "stock_exchange_tv": "OTC"},
]

# Column 3: Australian Lithium Leaders
global_market_leaders = [
    {"Name": "Pilbara Minerals", "Country": "Australia", "Ticker": "PLS", "tv_ticker": "PLS", "Stock exchange": "ASX", "stock_exchange_tv": "ASX"},
    {"Name": "Liontown Resources", "Country": "Australia", "Ticker": "LTR", "tv_ticker": "LTR", "Stock exchange": "ASX", "stock_exchange_tv": "ASX"},
    {"Name": "Mineral Resources", "Country": "Australia", "Ticker": "MIN", "tv_ticker": "MIN", "Stock exchange": "ASX", "stock_exchange_tv": "ASX"},
    {"Name": "IGO Limited", "Country": "Australia", "Ticker": "IGO", "tv_ticker": "IGO", "Stock exchange": "ASX", "stock_exchange_tv": "ASX"},
    {"Name": "Core Lithium", "Country": "Australia", "Ticker": "CXO", "tv_ticker": "CXO", "Stock exchange": "ASX", "stock_exchange_tv": "ASX"},
    {"Name": "Global Lithium Resources", "Country": "Australia", "Ticker": "GL1", "tv_ticker": "GL1", "Stock exchange": "ASX", "stock_exchange_tv": "ASX"},
    {"Name": "Vulcan Energy Resources", "Country": "Australia", "Ticker": "VUL", "tv_ticker": "VUL", "Stock exchange": "ASX", "stock_exchange_tv": "ASX"},
    {"Name": "Lake Resources", "Country": "Australia", "Ticker": "LKE", "tv_ticker": "LKE", "Stock exchange": "ASX", "stock_exchange_tv": "ASX"},
    {"Name": "Argosy Minerals", "Country": "Australia", "Ticker": "AGY", "tv_ticker": "AGY", "Stock exchange": "ASX", "stock_exchange_tv": "ASX"},
    {"Name": "Allkem Limited", "Country": "Australia", "Ticker": "AKE", "tv_ticker": "AKE", "Stock exchange": "ASX", "stock_exchange_tv": "ASX"},
]

# Combine all stocks for processing
most_followed_stocks = most_watched + north_american_leaders + global_market_leaders


def get_yahoo_ticker(ticker, exchange):
    """
    Get the correct Yahoo Finance ticker using custom mappings and exchange suffixes.
    """
    # First check if there's a custom mapping
    if ticker in custom_mappings:
        return custom_mappings[ticker]
    
    # If no custom mapping, use exchange mapping
    base_ticker = ticker.split('.')[0]  # Remove any existing suffix
    
    # Map exchange to suffix
    suffix = exchange_mappings.get(exchange, '')
    
    return f"{base_ticker}{suffix}"


def process_stock_data(cursor, connection, stockdata):
    """
    Inserts or updates a single stock data into the database using the upsert function.
    """
    print("DOne 3")
    try:
        insert_most_followed_stock(
            cursor=cursor,
            connection=connection,
            name=stockdata.get("name"),
            ticker=stockdata.get("ticker"),
            open_price=stockdata.get("open_price"),
            close_price=stockdata.get("close_price"),
            intraday_percentage=stockdata.get("intraday_percentage"),
            current_price=stockdata.get("current_price"),
            intraday_change=stockdata.get("intraday_change"),
            seven_day_change=stockdata.get("seven_day_change"),
            seven_day_percentage=stockdata.get("seven_day_percentage"),
            volume=stockdata.get("volume"),
            country=stockdata.get("country"),
            stock_exchange=stockdata.get("stock_exchange"),
            stock_type=stockdata.get("stocks_type"),
        )
    except Exception as e:
        print(f"Error processing stock data for {stockdata.get('ticker')}: {e}")


def calculate_percentage_change(start, end):
    """Calculate percentage change between two values."""
    if start and end and start != 0:
        return ((end - start) / start) * 100
    return None

stock_data = []

def get_stock_data_from_yfinance(ticker):
    """Get stock data using yfinance library."""
    try:
        # Let yfinance handle the session - don't pass custom session
        data = yf.Ticker(ticker)
        
        # Get current market data
        current_info = data.info
        
        # Get historical data
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)  # 7-day range
        hist = data.history(start=start_date, end=end_date)
        
        if hist.empty:
            print(f"No historical data available for {ticker}")
            return None
        
        # Extract data
        open_price = hist['Open'].iloc[-1] if 'Open' in hist.columns and len(hist['Open']) > 0 else None
        close_price = hist['Close'].iloc[-1] if 'Close' in hist.columns and len(hist['Close']) > 0 else None
        current_price = close_price  # Use close price as current price
        
        first_close = hist['Close'].iloc[0] if 'Close' in hist.columns and len(hist['Close']) > 0 else None
        volume = hist['Volume'].iloc[-1] if 'Volume' in hist.columns and len(hist['Volume']) > 0 else None
        
        # Calculate changes
        intraday_change = current_price - open_price if open_price and current_price else None
        intraday_percentage = calculate_percentage_change(open_price, current_price)
        seven_day_change = current_price - first_close if first_close and current_price else None
        seven_day_percentage = calculate_percentage_change(first_close, current_price)
        
        return {
            "price": current_price,
            "open_price": open_price,
            "close_price": close_price,
            "intraday_change": intraday_change,
            "intraday_percentage": intraday_percentage,
            "seven_day_change": seven_day_change,
            "seven_day_percentage": seven_day_percentage,
            "volume": volume
        }
        
    except Exception as e:
        print(f"Error getting data for {ticker}: {e}")
        return None


def process_stock_category(cursor, connection, stocks, category_name):
    global stock_data
    for stock in stocks:
        ticker = stock["Ticker"]
        exchange = stock["Stock exchange"]
        
        # Get the correct Yahoo Finance ticker
        yahoo_ticker = get_yahoo_ticker(ticker, exchange)
        
        try:
            # Get stock data from yfinance using the mapped ticker
            stock_info_data = get_stock_data_from_yfinance(yahoo_ticker)
            
            if stock_info_data:
                stock_info = {
                    "name": stock["Name"],
                    "ticker": stock["Ticker"],
                    "open_price": round(stock_info_data["open_price"], 2) if stock_info_data["open_price"] else None,
                    "close_price": stock_info_data["close_price"] if stock_info_data["close_price"] else None,
                    "current_price": round(stock_info_data["price"], 2) if stock_info_data["price"] else None,
                    "intraday_change": round(stock_info_data["intraday_change"], 2) if stock_info_data["intraday_change"] else None,
                    "intraday_percentage": round(stock_info_data["intraday_percentage"], 2) if stock_info_data["intraday_percentage"] else None,
                    "seven_day_change": round(stock_info_data["seven_day_change"], 2) if stock_info_data["seven_day_change"] else None,
                    "seven_day_percentage": round(stock_info_data["seven_day_percentage"], 2) if stock_info_data["seven_day_percentage"] else None,
                    "volume": stock_info_data["volume"],
                    "country": stock["Country"],
                    "stock_exchange": stock["Stock exchange"],
                    "stocks_type": category_name
                }

                process_stock_data(cursor, connection, stock_info)
                stock_data.append(stock_info)
            else:
                print(f"No data available for {yahoo_ticker} (original: {ticker})")
        except Exception as e:
            print(f"Error processing stock {stock['Name']} ({yahoo_ticker}): {str(e)}")

def get_most_followed_data():
    # Get database connection
    connection, cursor = get_curser()
    
    try:
        # Process all categories
        process_stock_category(cursor, connection, most_watched, "most_watched")
        process_stock_category(cursor, connection, north_american_leaders, "north_american_leaders")
        process_stock_category(cursor, connection, global_market_leaders, "global_market_leaders")

        print("Scraped Data:")
        print(stock_data)

        return stock_data
    
    finally:
        # Close database connection
        cursor.close()
        connection.close()
