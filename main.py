import pandas as pd
pd.set_option('display.max_columns', None)
import requests
from datetime import datetime, timedelta
import json
import time
import random
from urllib.parse import urlencode

class NSEInsiderTradingScraper:
    def __init__(self):
        self.session = requests.Session()
        self.base_url = "https://www.nseindia.com"
        self.setup_session()
    
    def setup_session(self):
        """Setup session with realistic headers and cookies"""
        # Rotate between different user agents
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
        
        headers = {
            'User-Agent': random.choice(user_agents),
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',  # Removed 'br' to avoid Brotli compression
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
            print("Getting session cookies...")
            
            # Visit homepage first
            response = self.session.get(self.base_url, timeout=30)
            print(f"Homepage status: {response.status_code}")
            
            # Add random delay
            time.sleep(random.uniform(1, 3))
            
            # Visit insider trading page
            insider_url = f"{self.base_url}/companies-listing/corporate-filings-insider-trading"
            response = self.session.get(insider_url, timeout=30)
            print(f"Insider trading page status: {response.status_code}")
            
            # Add another delay
            time.sleep(random.uniform(1, 2))
            
            return response.status_code == 200
            
        except Exception as e:
            print(f"Error getting cookies: {e}")
            return False
    
    def fetch_insider_data(self, days_back=90):
        """Fetch insider trading data with Brotli compression handling"""
        try:
            # Calculate dates
            todate = datetime.today()
            fromdate = todate - timedelta(days=days_back)
            
            # Format dates as required by NSE API
            to_date_str = todate.strftime('%d-%m-%Y')
            from_date_str = fromdate.strftime('%d-%m-%Y')
            
            print(f"Fetching data from {from_date_str} to {to_date_str}")
            
            # Construct API URL
            api_url = f"{self.base_url}/api/corporates-pit"
            params = {
                'index': 'equities',
                'from_date': from_date_str,
                'to_date': to_date_str
            }
            
            full_url = f"{api_url}?{urlencode(params)}"
            print(f"API URL: {full_url}")
            
            # Make request
            response = self.session.get(api_url, params=params, timeout=30)
            print(f"API response status: {response.status_code}")
            content_type = response.headers.get('content-type', 'Unknown')
            print(f"Response content type: {content_type}")
            content_encoding = response.headers.get('Content-Encoding', '').lower()
            print(f"Content-Encoding: {content_encoding}")
            
            if response.status_code != 200:
                print(f"HTTP Error: {response.status_code}")
                print(f"Response headers: {dict(response.headers)}")
                return None
            
            # Handle response content
            response_content = None
            if 'br' in content_encoding:
                print("Brotli encoding detected. Attempting to decompress...")
                try:
                    import brotli
                    decompressed = brotli.decompress(response.content)
                    response_content = decompressed.decode('utf-8')
                    print(f"Decompressed response preview: {response_content[:200]}")
                except ImportError:
                    print("Brotli library not installed. Please install with: pip install brotli")
                    return None
                except Exception as e:
                    print(f"Brotli decompression error: {e}")
                    return None
            else:
                response_content = response.text
                print(f"Response preview: {response_content[:200]}")
            
            # Check if response is JSON
            if 'application/json' not in content_type:
                print(f"Warning: Response is not JSON. Content-Type: {content_type}")
                print("This might be an error page or CAPTCHA challenge")
                
                # Save the response for debugging
                with open('nse_response_debug.html', 'w', encoding='utf-8') as f:
                    f.write(response_content)
                print("Response saved to 'nse_response_debug.html' for inspection")
                return None
            
            # Parse JSON
            try:
                data = json.loads(response_content)
            except json.JSONDecodeError as e:
                print(f"JSON decode error: {e}")
                print("Response might be empty or malformed")
                return None
            
            # Check data structure
            if not isinstance(data, dict):
                print(f"Unexpected data type: {type(data)}")
                return None
            
            if 'data' not in data:
                print(f"No 'data' key found. Available keys: {list(data.keys())}")
                return None
            
            if not data['data']:
                print("No insider trading data found for the specified date range")
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame(data['data'])
            print(f"Successfully fetched {len(df)} records")
            
            return df, from_date_str, to_date_str
            
        except requests.exceptions.Timeout:
            print("Request timed out. NSE server might be slow.")
            return None
        except requests.exceptions.ConnectionError:
            print("Connection error. Check your internet connection.")
            return None
        except Exception as e:
            print(f"Unexpected error: {e}")
            return None
    
    def clean_data(self, df):
        """Clean and process the DataFrame"""
        if df is None or df.empty:
            return None
        
        print("Cleaning data...")
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
    
    def save_data(self, df, from_date_str, to_date_str):
        """Save data to CSV with summary"""
        if df is None or df.empty:
            print("No data to save")
            return
        
        # Create filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'nse_insider_trading_{from_date_str.replace("-", "")}_{to_date_str.replace("-", "")}_{timestamp}.csv'
        
        # Save to CSV
        df.to_csv(filename, index=False)
        print(f"\nData saved to: {filename}")
        
        # Display summary
        self.display_summary(df, from_date_str, to_date_str)
        
        return filename
    
    def display_summary(self, df, from_date_str, to_date_str):
        """Display data summary"""
        print(f"\n{'='*50}")
        print("DATA SUMMARY")
        print(f"{'='*50}")
        print(f"Date range: {from_date_str} to {to_date_str}")
        print(f"Total records: {len(df):,}")
        
        if 'company' in df.columns:
            print(f"Unique companies: {df['company'].nunique():,}")
        if 'symbol' in df.columns:
            print(f"Unique symbols: {df['symbol'].nunique():,}")
        
        # Transaction types
        if 'tdpTransactionType' in df.columns:
            print(f"\nTransaction Types:")
            trans_counts = df['tdpTransactionType'].value_counts()
            for trans_type, count in trans_counts.items():
                print(f"  {trans_type}: {count:,}")
        
        # Value statistics
        if 'secVal' in df.columns:
            total_value = df['secVal'].sum()
            if pd.notna(total_value):
                print(f"\nTotal transaction value: ₹{total_value:,.2f}")
        
        print(f"\nColumns available: {', '.join(df.columns)}")
        
        # Display sample records
        print(f"\nSample records:")
        print(df.head(3).to_string())

def main():
    """Main execution function"""
    start_time = time.time()
    
    scraper = NSEInsiderTradingScraper()
    
    try:
        # Get cookies first
        if not scraper.get_cookies():
            print("Failed to get session cookies. Continuing anyway...")
        
        # Add delay before API call
        time.sleep(random.uniform(2, 4))
        
        # Fetch data (default 90 days)
        result = scraper.fetch_insider_data(days_back=90)
        
        if result is None:
            print("\nFailed to fetch data. This could be due to:")
            print("1. NSE's anti-scraping measures (most likely)")
            print("2. Network connectivity issues")
            print("3. API endpoint changes")
            print("4. CAPTCHA challenges")
            print("\nAlternative solutions:")
            print("- Use Selenium with headless Chrome")
            print("- Try different time intervals")
            print("- Use proxy servers")
            print("- Consider NSE's official data feeds")
            return
        
        df, from_date, to_date = result
        
        # Clean data
        df_clean = scraper.clean_data(df)
        
        if df_clean is not None:
            # Save data
            filename = scraper.save_data(df_clean, from_date, to_date)
            print(f"\nSuccess! Data exported to {filename}")
        else:
            print("Data cleaning failed")
    
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        end_time = time.time()
        print(f"\nTotal execution time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()