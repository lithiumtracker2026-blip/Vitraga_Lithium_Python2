# lithium_python2

Lithium price and most followed stocks data collection pipeline - scrapes lithium prices from Trading Economics, tracks most followed lithium stocks, and fetches insider transactions.

## Features
- Process 1: Most followed stocks tracker (lithium stocks)
- Process 2: Metal price fetcher from multiple sources (Lithium ETF, Aluminum, Nickel, Zinc, Platinum)
- Process 3: Lithium price scraper from Trading Economics (using requests/BeautifulSoup)
- Process 4: Insider transactions fetcher (US & Canadian lithium stocks)

## Setup
1. Install dependencies: `pip install -r requirements.txt`
2. Configure `.env` with database credentials
3. Database tables are already set up (reusing existing tables)
4. Run `python set_process.py` to select process
5. Run `python app.py` to start scraping

## Database
Uses PostgreSQL on Railway.app to store lithium prices (in api_app_cmecopperspot table), metal prices, stock tracking data, and insider transactions for lithium stocks.

## Docker
Includes Dockerfile.selenium for running scrapers in containerized environment (optional).

## Process Cycle
The application runs in a continuous cycle:
1. Process 1 → Process 2 → Process 3 → Process 4 → (back to Process 1)
2. Each process updates to the next before execution to prevent getting stuck on failures
