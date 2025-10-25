# USPTO Controller-Based Data Processing System

A modular, scalable system for downloading, processing, and storing USPTO trademark data using separate controllers for different responsibilities. Optimized for low-spec systems with batch processing and checkpoint/resume functionality.

## Features

- **Modular Architecture**: Separate controllers for API, download, processing, and database operations
- **Product-Specific Tables**: Automatically creates separate database tables for each USPTO product identifier
- **Batch Processing**: Processes large files in manageable batches to handle memory constraints
- **Checkpoint/Resume**: Can resume processing from where it left off if interrupted
- **Latest File Detection**: Only downloads and processes new or updated files
- **Memory Optimized**: Designed to work efficiently on low-spec systems
- **Configurable**: JSON-based configuration system for easy customization
- **Progress Tracking**: Comprehensive logging and progress monitoring

## Quick Start

### 1. Setup System

```bash
# Run setup script
python setup.py

# Install dependencies
pip install -r requirements.txt
```

### 2. Setup Database

First, create the database schema:

```bash
# Using psql
psql -U postgres -d trademarks -f create_multi_product_schema.sql
```

### 3. Configure Database

Edit `uspto_config.json` (created by setup script):

```json
{
  "database": {
    "dbname": "trademarks",
    "user": "postgres",
    "password": "your_password",
    "host": "localhost",
    "port": "5432"
  }
}
```

### 4. Run the Processor

```bash
# Test all controllers
python run_uspto.py --test

# Basic run with default settings
python run_uspto.py

# Process only 1 file per product (faster)
python run_uspto.py --max-files 1

# Force redownload all files
python run_uspto.py --force-redownload
```

## Project Structure

```
📁 Project Root
├── 🚀 run_uspto.py                    # Main entry point
├── ⚙️ uspto_config.json               # Configuration file
├── 🛠️ setup.py                       # Setup script
├── 📋 requirements.txt                # Dependencies
├── 🗄️ create_multi_product_schema.sql # Database schema
├── 📚 documentation.txt               # Complete documentation
├── 📖 README.md                       # This file
├── 📁 controllers/                    # Controller package
│   ├── __init__.py                    # Package initialization
│   ├── 📁 core/                       # Core controllers
│   │   ├── __init__.py
│   │   ├── uspto_controllers.py       # Main controller classes
│   │   └── uspto_controller_runner.py # Runner and configuration
│   ├── 📁 examples/                   # Usage examples
│   │   ├── __init__.py
│   │   └── controller_examples.py     # Example scripts
│   └── 📁 utils/                      # Utility scripts
│       ├── __init__.py
│       └── check_tables.py            # Database verification
└── 📊 uspto_data/                     # Data directory
    ├── zips/                          # Downloaded files
    ├── extracted/                     # Extracted files
    └── processed/                     # Processed data
```

## Configuration

The system uses a JSON configuration file (`uspto_config.json`) with the following structure:

```json
{
  "database": {
    "dbname": "trademarks",
    "user": "postgres",
    "password": "1234",
    "host": "localhost",
    "port": "5432"
  },
  "processing": {
    "max_files_per_product": 2,
    "batch_size": 10000,
    "memory_limit_mb": 512,
    "max_workers": 2,
    "chunk_size": 50000
  },
  "download": {
    "download_dir": "./uspto_data",
    "keep_latest_zips": 10,
    "force_redownload": false
  },
  "api": {
    "base_url": "https://data.uspto.gov/ui/datasets/products/search",
    "timeout": 30,
    "retry_attempts": 3
  }
}
```

## Command Line Options

```bash
python uspto_runner.py [options]

Options:
  --config FILE           Configuration file (default: uspto_config.json)
  --max-files N           Maximum files per product
  --force-redownload      Force redownload of all files
  --resume                Resume processing from checkpoint
  --product-id ID         Process specific product only
  --batch-size N          Batch size for processing
  --memory-limit N        Memory limit in MB
  --setup-db              Setup database schema only
```

## Database Schema

The system creates the following main tables:

- `uspto_products`: Registry of all USPTO products
- `file_processing_history`: Tracks download and processing status
- `batch_processing`: Tracks individual batch processing
- `product_[product_id]`: Product-specific data tables

### Product-Specific Tables

Each USPTO product gets its own table with appropriate schema:

- **TRCFECO2** (Case File Data): `product_trcfeco2`
- **TRASECO** (Assignment Data): `product_traseco`  
- **TTABTDXF** (TTAB Proceedings): `product_ttabtdxf`

## Monitoring Progress

### Check Processing Status

```sql
-- View overall processing status
SELECT * FROM processing_status;

-- Check specific product
SELECT * FROM processing_status WHERE product_id = 'TRCFECO2';

-- View processing statistics
SELECT * FROM get_processing_stats();
```

### Monitor Batch Processing

```sql
-- Check batch status
SELECT product_id, file_name, COUNT(*) as total_batches,
       COUNT(CASE WHEN processed = TRUE THEN 1 END) as processed_batches,
       COUNT(CASE WHEN saved_to_db = TRUE THEN 1 END) as saved_batches
FROM batch_processing 
GROUP BY product_id, file_name;
```

## Performance Optimization

### For Low-Spec Systems

```bash
# Reduce batch size and memory usage
python uspto_runner.py --batch-size 5000 --memory-limit 256

# Process fewer files at once
python uspto_runner.py --max-files 1
```

### For High-Performance Systems

```bash
# Increase batch size for faster processing
python uspto_runner.py --batch-size 50000 --memory-limit 2048

# Process more files per product
python uspto_runner.py --max-files 5
```

## File Structure

```
uspto_data/
├── zips/                    # Downloaded ZIP files
│   ├── TRCFECO2/
│   ├── TRASECO/
│   └── TTABTDXF/
├── extracted/               # Extracted files
│   ├── TRCFECO2/
│   ├── TRASECO/
│   └── TTABTDXF/
├── processed/               # Processed data files
├── checkpoints/             # Processing checkpoints
└── batches/                 # Batch files
    ├── TRCFECO2/
    ├── TRASECO/
    └── TTABTDXF/
```

## Error Handling

The system includes comprehensive error handling:

- **Download Errors**: Retries failed downloads
- **Processing Errors**: Saves progress and allows resume
- **Database Errors**: Logs errors and continues processing
- **Memory Errors**: Automatically reduces batch sizes

## Troubleshooting

### Common Issues

1. **Database Connection Error**
   - Check database credentials in `uspto_config.json`
   - Ensure PostgreSQL is running
   - Verify database exists

2. **Memory Issues**
   - Reduce `batch_size` in configuration
   - Lower `memory_limit_mb`
   - Process fewer files at once

3. **Download Failures**
   - Check internet connection
   - Verify USPTO API is accessible
   - Use `--force-redownload` to retry

### Logs

Check log files for detailed information:
- `uspto_multi_processor.log`: Main processing log
- Database logs: Check PostgreSQL logs

## API Reference

### USPTOMultiProcessor

Main processor class with methods:

- `get_trademark_datasets()`: Fetch available datasets
- `register_product(product_info)`: Register a product
- `download_file(file_info, product_id)`: Download a file
- `process_file_in_batches(file_path, product_id)`: Process file in batches
- `resume_processing(product_id)`: Resume from checkpoint

### Configuration Management

- `USPTOConfig`: Configuration manager
- `get(key_path)`: Get config value with dot notation
- `set(key_path, value)`: Set config value
- `save()`: Save configuration to file

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License.
