import pandas as pd
pd.set_option('display.max_columns', None)
import requests
from datetime import datetime, timedelta
import json
import time
import random
from urllib.parse import urlencode
import os
import logging
from pathlib import Path

class NSEInsiderTradingScraper:
    def __init__(self, data_file='nse_insider_trading_data.csv'):
        self.session = requests.Session()
        self.base_url = "https://www.nseindia.com"
        self.data_file = data_file
        self.setup_session()
        self.setup_logging()
    
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('nse_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def setup_session(self):
        """Setup session with realistic headers and cookies"""
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
        
        headers = {
            'User-Agent': random.choice(user_agents),
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://www.nseindia.com/companies-listing/corporate-filings-insider-trading'
        }
        
        self.session.headers.update(headers)
    
    def get_cookies(self):
        """Get necessary cookies by visiting NSE pages"""
        try:
            self.logger.info("Getting session cookies...")
            
            response = self.session.get(self.base_url, timeout=30)
            self.logger.info(f"Homepage status: {response.status_code}")
            
            time.sleep(random.uniform(1, 3))
            
            insider_url = f"{self.base_url}/companies-listing/corporate-filings-insider-trading"
            response = self.session.get(insider_url, timeout=30)
            self.logger.info(f"Insider trading page status: {response.status_code}")
            
            time.sleep(random.uniform(1, 2))
            
            return response.status_code == 200
            
        except Exception as e:
            self.logger.error(f"Error getting cookies: {e}")
            return False
    
    def get_last_update_time(self):
        """Get the last update time from existing data file"""
        if not os.path.exists(self.data_file):
            # If file doesn't exist, start from 7 days ago
            return datetime.now() - timedelta(days=7)
        
        try:
            df = pd.read_csv(self.data_file)
            if df.empty:
                return datetime.now() - timedelta(days=7)
            
            # Convert date column to datetime if it exists
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
                last_date = df['date'].max()
                if pd.notna(last_date):
                    return last_date
            
            # Fallback to file modification time
            file_stat = os.stat(self.data_file)
            return datetime.fromtimestamp(file_stat.st_mtime)
            
        except Exception as e:
            self.logger.error(f"Error getting last update time: {e}")
            return datetime.now() - timedelta(days=7)
    
    def fetch_insider_data(self, from_date=None, to_date=None):
        """Fetch insider trading data with automatic date range"""
        try:
            if to_date is None:
                to_date = datetime.now()
            
            if from_date is None:
                from_date = self.get_last_update_time()
            
            # Ensure we don't go back more than 30 days (NSE limit)
            max_back_date = datetime.now() - timedelta(days=30)
            if from_date < max_back_date:
                from_date = max_back_date
            
            # Format dates as required by NSE API
            to_date_str = to_date.strftime('%d-%m-%Y')
            from_date_str = from_date.strftime('%d-%m-%Y')
            
            self.logger.info(f"Fetching data from {from_date_str} to {to_date_str}")
            
            # Construct API URL
            api_url = f"{self.base_url}/api/corporates-pit"
            params = {
                'index': 'equities',
                'from_date': from_date_str,
                'to_date': to_date_str
            }
            
            # Make request
            response = self.session.get(api_url, params=params, timeout=30)
            self.logger.info(f"API response status: {response.status_code}")
            
            if response.status_code != 200:
                self.logger.error(f"HTTP Error: {response.status_code}")
                return None
            
            # Handle response content
            content_encoding = response.headers.get('Content-Encoding', '').lower()
            response_content = None
            
            if 'br' in content_encoding:
                self.logger.info("Brotli encoding detected. Attempting to decompress...")
                try:
                    import brotli
                    decompressed = brotli.decompress(response.content)
                    response_content = decompressed.decode('utf-8')
                except ImportError:
                    self.logger.error("Brotli library not installed. Please install with: pip install brotli")
                    return None
                except Exception as e:
                    self.logger.error(f"Brotli decompression error: {e}")
                    return None
            else:
                response_content = response.text
            
            # Check if response is JSON
            content_type = response.headers.get('content-type', '')
            if 'application/json' not in content_type:
                self.logger.warning(f"Response is not JSON. Content-Type: {content_type}")
                return None
            
            # Parse JSON
            try:
                data = json.loads(response_content)
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON decode error: {e}")
                return None
            
            # Check data structure
            if not isinstance(data, dict) or 'data' not in data:
                self.logger.error("Invalid data structure received")
                return None
            
            if not data['data']:
                self.logger.info("No new insider trading data found")
                return pd.DataFrame(), from_date_str, to_date_str
            
            # Convert to DataFrame
            df = pd.DataFrame(data['data'])
            self.logger.info(f"Successfully fetched {len(df)} records")
            
            return df, from_date_str, to_date_str
            
        except Exception as e:
            self.logger.error(f"Error fetching data: {e}")
            return None
    
    def clean_data(self, df):
        """Clean and process the DataFrame"""
        if df is None or df.empty:
            return df
        
        self.logger.info("Cleaning data...")
        df_clean = df.copy()
        
        # Convert date columns
        date_columns = ['date', 'acqfromDt', 'acqtoDt', 'intimDt']
        for col in date_columns:
            if col in df_clean.columns:
                df_clean[col] = pd.to_datetime(df_clean[col], format='%d-%b-%Y %H:%M', errors='coerce')
        
        # Convert numeric columns
        numeric_columns = ['buyValue', 'sellValue', 'buyQuantity', 'sellquantity', 'secAcq', 
                          'befAcqSharesNo', 'befAcqSharesPer', 'secVal', 'afterAcqSharesNo', 'afterAcqSharesPer']
        for col in numeric_columns:
            if col in df_clean.columns:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
        
        # Replace None and '-' with NaN
        df_clean = df_clean.replace([None, '-', ''], pd.NA)
        
        return df_clean
    
    def create_unique_key(self, row):
        """Create a unique key for each record to identify duplicates"""
        # Use combination of fields that should uniquely identify a record
        key_fields = ['symbol', 'company', 'name', 'date', 'secVal', 'tdpTransactionType']
        key_parts = []
        
        for field in key_fields:
            if field in row.index:
                value = str(row[field]) if pd.notna(row[field]) else 'NA'
                key_parts.append(value)
        
        return '|'.join(key_parts)
    
    def update_data_file(self, new_df):
        """Update the main data file with new records"""
        if new_df is None or new_df.empty:
            self.logger.info("No new data to update")
            return 0
        
        # Check if main file exists
        if os.path.exists(self.data_file):
            try:
                existing_df = pd.read_csv(self.data_file)
                self.logger.info(f"Loaded existing data: {len(existing_df)} records")
                
                # Convert date columns in existing data
                if 'date' in existing_df.columns:
                    existing_df['date'] = pd.to_datetime(existing_df['date'], errors='coerce')
                
                # Create unique keys for both dataframes
                existing_df['unique_key'] = existing_df.apply(self.create_unique_key, axis=1)
                new_df['unique_key'] = new_df.apply(self.create_unique_key, axis=1)
                
                # Find truly new records
                new_records = new_df[~new_df['unique_key'].isin(existing_df['unique_key'])]
                
                if new_records.empty:
                    self.logger.info("No new unique records found")
                    return 0
                
                # Remove the helper column
                new_records = new_records.drop('unique_key', axis=1)
                existing_df = existing_df.drop('unique_key', axis=1)
                
                # Combine data
                combined_df = pd.concat([existing_df, new_records], ignore_index=True)
                
                # Sort by date if available
                if 'date' in combined_df.columns:
                    combined_df = combined_df.sort_values('date', ascending=False)
                
            except Exception as e:
                self.logger.error(f"Error reading existing file: {e}")
                combined_df = new_df
                new_records = new_df
        else:
            self.logger.info("Creating new data file")
            combined_df = new_df
            new_records = new_df
        
        # Save updated data
        try:
            # Create backup of existing file
            if os.path.exists(self.data_file):
                backup_file = f"{self.data_file}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                os.rename(self.data_file, backup_file)
                self.logger.info(f"Created backup: {backup_file}")
            
            # Save new data
            combined_df.to_csv(self.data_file, index=False)
            self.logger.info(f"Updated {self.data_file} with {len(new_records)} new records")
            self.logger.info(f"Total records in file: {len(combined_df)}")
            
            return len(new_records)
            
        except Exception as e:
            self.logger.error(f"Error saving data: {e}")
            return 0
    
    def display_summary(self, df, new_records_count=0):
        """Display data summary"""
        summary = f"""
{'='*50}
DATA UPDATE SUMMARY
{'='*50}
Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
New records added: {new_records_count:,}
Total records in file: {len(df) if df is not None and not df.empty else 0:,}
"""
        
        if df is not None and not df.empty:
            if 'company' in df.columns:
                summary += f"Unique companies: {df['company'].nunique():,}\n"
            if 'symbol' in df.columns:
                summary += f"Unique symbols: {df['symbol'].nunique():,}\n"
        
        print(summary)
        self.logger.info(summary.replace('\n', ' | '))
    
    def run_update_cycle(self):
        """Run a single update cycle"""
        try:
            self.logger.info(f"Starting update cycle at {datetime.now()}")
            
            # Get cookies
            if not self.get_cookies():
                self.logger.warning("Failed to get session cookies. Continuing anyway...")
            
            # Add delay before API call
            time.sleep(random.uniform(2, 4))
            
            # Fetch new data
            result = self.fetch_insider_data()
            
            if result is None:
                self.logger.error("Failed to fetch data")
                return False
            
            df, from_date, to_date = result
            
            # Clean data
            df_clean = self.clean_data(df)
            
            # Update data file
            new_records_count = self.update_data_file(df_clean)
            
            # Display summary
            if os.path.exists(self.data_file):
                current_df = pd.read_csv(self.data_file)
                self.display_summary(current_df, new_records_count)
            
            self.logger.info(f"Update cycle completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error in update cycle: {e}")
            return False
    
    def run_continuous(self, interval_minutes=15):
        """Run continuous updates every specified interval"""
        self.logger.info(f"Starting continuous monitoring (every {interval_minutes} minutes)")
        self.logger.info(f"Data will be saved to: {self.data_file}")
        self.logger.info("Press Ctrl+C to stop")
        
        try:
            while True:
                success = self.run_update_cycle()
                
                if success:
                    self.logger.info(f"Sleeping for {interval_minutes} minutes...")
                else:
                    self.logger.warning(f"Update failed. Retrying in {interval_minutes} minutes...")
                
                # Sleep for the specified interval
                time.sleep(interval_minutes * 60)
                
        except KeyboardInterrupt:
            self.logger.info("Continuous monitoring stopped by user")
        except Exception as e:
            self.logger.error(f"Unexpected error in continuous mode: {e}")

def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='NSE Insider Trading Data Scraper')
    parser.add_argument('--mode', choices=['once', 'continuous'], default='continuous',
                       help='Run mode: once for single update, continuous for auto-updates')
    parser.add_argument('--interval', type=int, default=15,
                       help='Update interval in minutes (default: 15)')
    parser.add_argument('--file', type=str, default='nse_insider_trading_data.csv',
                       help='Data file name (default: nse_insider_trading_data.csv)')
    
    args = parser.parse_args()
    
    scraper = NSEInsiderTradingScraper(data_file=args.file)
    
    if args.mode == 'once':
        print("Running single update...")
        scraper.run_update_cycle()
    else:
        print(f"Running continuous updates every {args.interval} minutes...")
        scraper.run_continuous(interval_minutes=args.interval)

if __name__ == "__main__":
    main()