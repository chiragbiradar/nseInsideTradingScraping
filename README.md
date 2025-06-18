# NSE Insider Trading Data Scraper

A Python script that automatically scrapes insider trading data from the National Stock Exchange (NSE) of India and maintains an up-to-date CSV database with continuous monitoring capabilities.

## 🚀 Features

- **Automated Data Collection**: Scrapes insider trading data from NSE's official API
- **Continuous Monitoring**: Runs every 15 minutes (configurable) to fetch new records
- **Smart Duplicate Detection**: Prevents duplicate entries using multi-field unique keys
- **Incremental Updates**: Only fetches new data since the last update
- **Data Safety**: Automatic backups and error recovery
- **Comprehensive Logging**: Detailed logs for monitoring and debugging
- **Flexible Configuration**: Customizable intervals and file names

## 📋 Requirements

### Python Version
- Python 3.7 or higher

### Required Libraries
```bash
pip install pandas requests brotli
```

### Individual Package Installation
```bash
pip install pandas
pip install requests
pip install brotli  # For handling compressed responses
```

## 🛠️ Installation

1. **Clone or Download** the script files to your local machine

2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
   
   Or install manually:
   ```bash
   pip install pandas requests brotli
   ```

3. **Make the script executable** (Linux/Mac):
   ```bash
   chmod +x nse_insider_scraper.py
   ```

## 🎯 Usage

### Quick Start (Continuous Mode)
```bash
python nse_insider_scraper.py
```
This will start continuous monitoring with updates every 15 minutes.

### Command Line Options

#### Run Modes
```bash
# Continuous monitoring (default)
python nse_insider_scraper.py --mode continuous

# Single update only
python nse_insider_scraper.py --mode once
```

#### Custom Intervals
```bash
# Update every 30 minutes
python nse_insider_scraper.py --interval 30

# Update every 5 minutes
python nse_insider_scraper.py --interval 5
```

#### Custom File Names
```bash
# Use custom CSV file name
python nse_insider_scraper.py --file my_insider_data.csv

# Combine options
python nse_insider_scraper.py --mode continuous --interval 20 --file insider_trades.csv
```

### Complete Command Reference
```bash
python nse_insider_scraper.py [OPTIONS]

Options:
  --mode {once,continuous}    Run mode (default: continuous)
  --interval INTEGER          Update interval in minutes (default: 15)
  --file TEXT                 CSV file name (default: nse_insider_trading_data.csv)
  --help                      Show help message
```

## 📁 Output Files

### Primary Data File
- **`nse_insider_trading_data.csv`**: Main database containing all insider trading records
- Automatically created on first run
- Continuously updated with new records
- Contains cleaned and processed data

### Log Files
- **`nse_scraper.log`**: Detailed operational logs
- Includes timestamps, status updates, and error messages
- Rotates automatically to prevent excessive file size

### Backup Files
- **`*.backup_YYYYMMDD_HHMMSS`**: Automatic backups created before updates
- Ensures data safety in case of corruption
- Timestamped for easy identification

## 📊 Data Structure

The CSV file contains the following key columns:

| Column | Description |
|--------|-------------|
| `symbol` | Stock symbol (e.g., RELIANCE, TCS) |
| `company` | Company name |
| `name` | Name of the insider |
| `date` | Transaction date |
| `tdpTransactionType` | Type of transaction (Buy/Sell) |
| `secVal` | Security value in INR |
| `buyValue` | Buy transaction value |
| `sellValue` | Sell transaction value |
| `buyQuantity` | Number of shares bought |
| `sellquantity` | Number of shares sold |
| `befAcqSharesNo` | Shares held before acquisition |
| `afterAcqSharesNo` | Shares held after acquisition |
| `befAcqSharesPer` | Percentage held before |
| `afterAcqSharesPer` | Percentage held after |

## ⚙️ Configuration

### Customizing Update Intervals
```python
# In the script, modify the default interval
scraper.run_continuous(interval_minutes=30)  # 30 minutes instead of 15
```

### Modifying Date Range
```python
# Fetch data for last 30 days instead of default 7
result = scraper.fetch_insider_data(days_back=30)
```

### Logging Configuration
The script uses Python's logging module. You can modify logging levels in the `setup_logging()` method:

```python
logging.basicConfig(
    level=logging.DEBUG,  # Change to DEBUG for more detailed logs
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('nse_scraper.log'),
        logging.StreamHandler()
    ]
)
```

## 🔧 Troubleshooting

### Common Issues

#### 1. **No Data Retrieved**
```
Possible causes:
- NSE's anti-scraping measures activated
- Network connectivity issues
- API endpoint changes
- CAPTCHA challenges

Solutions:
- Wait and retry after some time
- Check internet connection
- Verify NSE website accessibility
```

#### 2. **Import Errors**
```bash
# Missing pandas
pip install pandas

# Missing requests
pip install requests

# Missing brotli (for compressed responses)
pip install brotli
```

#### 3. **Permission Errors**
```bash
# Linux/Mac: Ensure write permissions
chmod 755 .
chmod 644 *.csv

# Windows: Run as administrator if needed
```

#### 4. **Memory Issues**
For large datasets, you might encounter memory issues:
```python
# Reduce data retention period
# Modify in fetch_insider_data method:
max_back_date = datetime.now() - timedelta(days=7)  # Reduce from 30 to 7
```

### Debug Mode
To run in debug mode with detailed logging:
```bash
python nse_insider_scraper.py --mode once
```
Then check `nse_scraper.log` for detailed information.

## 📈 Performance Optimization

### Reducing API Calls
- The script automatically tracks the last update time
- Only fetches data since the last successful update
- Uses efficient date range queries

### Memory Management
- Processes data in chunks
- Cleans up temporary objects
- Uses efficient pandas operations

### Network Optimization
- Implements random delays to avoid rate limiting
- Uses session management for connection reuse
- Handles compressed responses efficiently

## 🛡️ Data Safety Features

### Backup System
- Automatic backups before each update
- Timestamped backup files
- Quick recovery in case of data corruption

### Duplicate Prevention
- Multi-field unique key generation
- Prevents duplicate entries
- Maintains data integrity

### Error Recovery
- Continues operation despite individual failures
- Comprehensive error logging
- Graceful handling of network issues

## 📋 Monitoring and Maintenance

### Checking Status
```bash
# View recent logs
tail -f nse_scraper.log

# Check data file size
ls -lh nse_insider_trading_data.csv

# Count records
wc -l nse_insider_trading_data.csv
```

### Regular Maintenance
- **Weekly**: Review log files for errors
- **Monthly**: Clean up old backup files
- **Quarterly**: Verify data integrity

## 🔄 Automation Setup

### Linux/Mac (Cron Alternative)
```bash
# Create a systemd service for better process management
sudo nano /etc/systemd/system/nse-scraper.service
```

### Windows (Task Scheduler Alternative)
The script's continuous mode is more reliable than Windows Task Scheduler for frequent updates.

### Docker Deployment
```dockerfile
FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY nse_insider_scraper.py .
CMD ["python", "nse_insider_scraper.py"]
```

## 📊 Data Analysis Examples

### Basic Analysis with Pandas
```python
import pandas as pd

# Load data
df = pd.read_csv('nse_insider_trading_data.csv')

# Top companies by transaction volume
top_companies = df.groupby('company')['secVal'].sum().sort_values(ascending=False).head(10)

# Recent transactions
recent = df.sort_values('date', ascending=False).head(20)

# Buy vs Sell analysis
transaction_summary = df.groupby('tdpTransactionType')['secVal'].agg(['count', 'sum'])
```

## 🚨 Legal and Ethical Considerations

- **Data Usage**: This script accesses publicly available data from NSE
- **Rate Limiting**: Implements delays to respect server resources
- **Terms of Service**: Users should review NSE's terms of service
- **Personal Use**: Intended for personal analysis and research

### Reporting Issues
- Check the log file (`nse_scraper.log`) for error details
- Include relevant error messages when reporting issues
- Specify your Python version and operating system

---

**Note**: This script is for educational purposes. Always ensure compliance with the website's terms of service and applicable laws when scraping data.
