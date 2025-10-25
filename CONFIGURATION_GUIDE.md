# USPTO Configuration Guide

This document explains all configuration options available in the USPTO Controller-Based Data Processing System.

## Configuration Files

- `uspto_config.json` - Complete configuration with all options
- `uspto_config_template.json` - Simplified template for basic setup

## Configuration Sections

### 1. Project Settings

```json
{
  "project": {
    "name": "USPTO Controller-Based Data Processing System",
    "version": "1.0.0",
    "description": "Modular system for downloading, processing, and storing USPTO trademark data",
    "author": "USPTO Data Processing Team",
    "license": "MIT"
  }
}
```

**Purpose**: Basic project metadata and identification.

### 2. API Configuration

```json
{
  "api": {
    "base_url": "https://data.uspto.gov",
    "search_endpoint": "/ui/datasets/products/search",
    "full_url": "https://data.uspto.gov/ui/datasets/products/search?facets=true&latest=true&labels=Trademark",
    "timeout": 30,
    "retry_attempts": 3,
    "retry_delay": 5,
    "rate_limit_delay": 1,
    "headers": {
      "User-Agent": "USPTO-Data-Processor/1.0.0",
      "Accept": "application/json",
      "Accept-Encoding": "gzip, deflate"
    }
  }
}
```

**Key Settings**:
- `base_url`: USPTO API base URL
- `full_url`: Complete API endpoint with parameters
- `timeout`: Request timeout in seconds
- `retry_attempts`: Number of retry attempts for failed requests
- `retry_delay`: Delay between retries in seconds
- `rate_limit_delay`: Delay to respect rate limits
- `headers`: HTTP headers for API requests

### 3. Download Configuration

```json
{
  "download": {
    "base_dir": "./uspto_data",
    "zips_dir": "./uspto_data/zips",
    "extracted_dir": "./uspto_data/extracted",
    "processed_dir": "./uspto_data/processed",
    "checkpoints_dir": "./uspto_data/checkpoints",
    "batches_dir": "./uspto_data/batches",
    "keep_latest_zips": 10,
    "force_redownload": false,
    "verify_downloads": true,
    "chunk_size": 8192,
    "max_concurrent_downloads": 3,
    "download_timeout": 300,
    "retry_failed_downloads": true,
    "cleanup_temp_files": true
  }
}
```

**Key Settings**:
- `base_dir`: Root directory for all downloaded data
- `zips_dir`: Directory for downloaded ZIP files
- `extracted_dir`: Directory for extracted files
- `processed_dir`: Directory for processed data
- `checkpoints_dir`: Directory for processing checkpoints
- `batches_dir`: Directory for batch files
- `keep_latest_zips`: Number of latest ZIP files to keep
- `force_redownload`: Force redownload of existing files
- `verify_downloads`: Verify file integrity after download
- `chunk_size`: Download chunk size in bytes
- `max_concurrent_downloads`: Maximum concurrent downloads
- `download_timeout`: Download timeout in seconds

### 4. Processing Configuration

```json
{
  "processing": {
    "batch_size": 10000,
    "chunk_size": 50000,
    "memory_limit_mb": 512,
    "max_workers": 2,
    "enable_parallel_processing": false,
    "data_cleaning": {
      "normalize_column_names": true,
      "remove_empty_rows": true,
      "handle_null_values": true,
      "validate_data_types": true,
      "sanitize_text": true
    },
    "file_types": {
      "csv": {
        "encoding": "utf-8",
        "delimiter": ",",
        "quote_char": "\"",
        "skip_blank_lines": true,
        "infer_schema": true
      },
      "xml": {
        "encoding": "utf-8",
        "parse_mode": "iterparse",
        "validate_xml": false,
        "strip_namespaces": true
      },
      "dta": {
        "encoding": "utf-8",
        "convert_to_csv": true
      }
    },
    "metadata": {
      "add_data_source": true,
      "add_file_hash": true,
      "add_batch_number": true,
      "add_processing_timestamp": true,
      "add_record_count": true
    }
  }
}
```

**Key Settings**:
- `batch_size`: Number of records per processing batch
- `chunk_size`: Number of rows to read at once
- `memory_limit_mb`: Memory limit in megabytes
- `max_workers`: Maximum number of worker processes
- `enable_parallel_processing`: Enable parallel processing
- `data_cleaning`: Data cleaning and normalization options
- `file_types`: File type-specific processing options
- `metadata`: Metadata to add to processed records

### 5. Database Configuration

```json
{
  "database": {
    "dbname": "trademarks",
    "user": "postgres",
    "password": "1234",
    "host": "localhost",
    "port": "5432",
    "schema": "public",
    "use_copy": true,
    "batch_insert_size": 1000,
    "connection_pool_size": 5,
    "connection_timeout": 30,
    "query_timeout": 300,
    "enable_logging": true,
    "log_level": "INFO",
    "backup_before_processing": false,
    "vacuum_after_processing": true,
    "analyze_after_processing": true,
    "indexes": {
      "create_automatic_indexes": true,
      "index_on_serial_number": true,
      "index_on_registration_number": true,
      "index_on_filing_date": true,
      "index_on_data_source": true,
      "index_on_batch_number": true
    }
  }
}
```

**Key Settings**:
- `dbname`: Database name
- `user`: Database username
- `password`: Database password
- `host`: Database host
- `port`: Database port
- `schema`: Database schema
- `use_copy`: Use COPY operations for faster inserts
- `batch_insert_size`: Batch size for database inserts
- `connection_pool_size`: Database connection pool size
- `connection_timeout`: Database connection timeout
- `query_timeout`: Database query timeout
- `enable_logging`: Enable database logging
- `log_level`: Database log level
- `backup_before_processing`: Backup database before processing
- `vacuum_after_processing`: Run VACUUM after processing
- `analyze_after_processing`: Run ANALYZE after processing
- `indexes`: Automatic index creation options

### 6. Orchestrator Configuration

```json
{
  "orchestrator": {
    "max_files_per_product": 2,
    "enable_parallel_processing": false,
    "log_level": "INFO",
    "progress_reporting_interval": 1000,
    "checkpoint_interval": 5000,
    "error_threshold": 10,
    "max_retries": 3,
    "retry_delay": 30,
    "cleanup_on_completion": true,
    "generate_summary_report": true,
    "notifications": {
      "enable_email_notifications": false,
      "email_on_completion": false,
      "email_on_error": false,
      "email_recipients": []
    }
  }
}
```

**Key Settings**:
- `max_files_per_product`: Maximum files to process per product
- `enable_parallel_processing`: Enable parallel processing
- `log_level`: Logging level
- `progress_reporting_interval`: Progress reporting interval
- `checkpoint_interval`: Checkpoint saving interval
- `error_threshold`: Maximum errors before stopping
- `max_retries`: Maximum retry attempts
- `retry_delay`: Delay between retries
- `cleanup_on_completion`: Clean up temporary files on completion
- `generate_summary_report`: Generate processing summary report
- `notifications`: Email notification settings

### 7. Logging Configuration

```json
{
  "logging": {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file_logging": true,
    "log_file": "./logs/uspto_processor.log",
    "max_log_size_mb": 10,
    "backup_count": 5,
    "console_logging": true,
    "log_rotation": "daily",
    "log_retention_days": 30
  }
}
```

**Key Settings**:
- `level`: Logging level (DEBUG, INFO, WARNING, ERROR)
- `format`: Log message format
- `file_logging`: Enable file logging
- `log_file`: Log file path
- `max_log_size_mb`: Maximum log file size
- `backup_count`: Number of log file backups
- `console_logging`: Enable console logging
- `log_rotation`: Log rotation frequency
- `log_retention_days`: Log retention period

### 8. Monitoring Configuration

```json
{
  "monitoring": {
    "enable_performance_monitoring": true,
    "memory_monitoring": true,
    "disk_space_monitoring": true,
    "database_monitoring": true,
    "alert_thresholds": {
      "memory_usage_percent": 80,
      "disk_space_percent": 85,
      "processing_time_hours": 24,
      "error_rate_percent": 5
    },
    "metrics_collection": {
      "collect_processing_metrics": true,
      "collect_performance_metrics": true,
      "collect_error_metrics": true,
      "metrics_retention_days": 7
    }
  }
}
```

**Key Settings**:
- `enable_performance_monitoring`: Enable performance monitoring
- `memory_monitoring`: Monitor memory usage
- `disk_space_monitoring`: Monitor disk space
- `database_monitoring`: Monitor database performance
- `alert_thresholds`: Alert thresholds for various metrics
- `metrics_collection`: Metrics collection settings

### 9. Security Configuration

```json
{
  "security": {
    "encrypt_sensitive_data": false,
    "secure_database_connection": false,
    "api_key_encryption": false,
    "log_sanitization": true,
    "data_anonymization": false,
    "access_control": {
      "enable_user_authentication": false,
      "require_api_key": false,
      "rate_limiting": false
    }
  }
}
```

**Key Settings**:
- `encrypt_sensitive_data`: Encrypt sensitive data
- `secure_database_connection`: Use secure database connection
- `api_key_encryption`: Encrypt API keys
- `log_sanitization`: Sanitize log output
- `data_anonymization`: Anonymize sensitive data
- `access_control`: Access control settings

### 10. Backup Configuration

```json
{
  "backup": {
    "enable_automatic_backup": false,
    "backup_frequency": "daily",
    "backup_retention_days": 7,
    "backup_location": "./backups",
    "compress_backups": true,
    "backup_before_processing": false,
    "backup_after_processing": false
  }
}
```

**Key Settings**:
- `enable_automatic_backup`: Enable automatic backups
- `backup_frequency`: Backup frequency
- `backup_retention_days`: Backup retention period
- `backup_location`: Backup directory
- `compress_backups`: Compress backup files
- `backup_before_processing`: Backup before processing
- `backup_after_processing`: Backup after processing

### 11. Cache Configuration

```json
{
  "cache": {
    "enable_caching": true,
    "cache_directory": "./cache",
    "cache_expiry_hours": 24,
    "cache_api_responses": true,
    "cache_file_metadata": true,
    "cache_database_queries": false,
    "max_cache_size_mb": 100
  }
}
```

**Key Settings**:
- `enable_caching`: Enable caching
- `cache_directory`: Cache directory
- `cache_expiry_hours`: Cache expiry time
- `cache_api_responses`: Cache API responses
- `cache_file_metadata`: Cache file metadata
- `cache_database_queries`: Cache database queries
- `max_cache_size_mb`: Maximum cache size

### 12. Validation Configuration

```json
{
  "validation": {
    "validate_input_data": true,
    "validate_database_schema": true,
    "validate_file_integrity": true,
    "data_quality_checks": {
      "check_required_fields": true,
      "check_data_types": true,
      "check_value_ranges": true,
      "check_duplicates": true,
      "check_referential_integrity": true
    },
    "validation_rules": {
      "max_file_size_mb": 1000,
      "min_batch_size": 100,
      "max_batch_size": 50000,
      "required_fields": ["data_source", "file_hash"],
      "date_format": "YYYY-MM-DD",
      "timestamp_format": "YYYY-MM-DD HH:MM:SS"
    }
  }
}
```

**Key Settings**:
- `validate_input_data`: Validate input data
- `validate_database_schema`: Validate database schema
- `validate_file_integrity`: Validate file integrity
- `data_quality_checks`: Data quality check options
- `validation_rules`: Validation rules and constraints

### 13. Performance Configuration

```json
{
  "performance": {
    "optimization_level": "balanced",
    "memory_optimization": true,
    "disk_optimization": true,
    "network_optimization": true,
    "database_optimization": true,
    "compression": {
      "compress_downloads": false,
      "compress_processed_data": false,
      "compression_level": 6
    },
    "parallel_processing": {
      "max_parallel_downloads": 3,
      "max_parallel_processors": 2,
      "max_parallel_database_operations": 1
    }
  }
}
```

**Key Settings**:
- `optimization_level`: Optimization level (low, balanced, high)
- `memory_optimization`: Enable memory optimization
- `disk_optimization`: Enable disk optimization
- `network_optimization`: Enable network optimization
- `database_optimization`: Enable database optimization
- `compression`: Compression settings
- `parallel_processing`: Parallel processing settings

### 14. USPTO Products Configuration

```json
{
  "uspto_products": {
    "supported_products": [
      "TRCFECO2",
      "TRASECO", 
      "TTABTDXF",
      "TRTDXFAG",
      "TRTDXFAP",
      "TRTYRAG",
      "TRTYRAP",
      "TTABYR"
    ],
    "product_configurations": {
      "TRCFECO2": {
        "table_name": "product_trcfeco2",
        "schema_type": "case_file",
        "primary_key": "serial_number",
        "batch_size": 10000,
        "priority": 1
      },
      "TRASECO": {
        "table_name": "product_traseco", 
        "schema_type": "assignment",
        "primary_key": "assignment_id",
        "batch_size": 15000,
        "priority": 2
      },
      "TTABTDXF": {
        "table_name": "product_ttabtdxf",
        "schema_type": "ttab",
        "primary_key": "proceeding_number",
        "batch_size": 5000,
        "priority": 3
      }
    }
  }
}
```

**Key Settings**:
- `supported_products`: List of supported USPTO products
- `product_configurations`: Product-specific configurations
- `table_name`: Database table name for each product
- `schema_type`: Schema type (case_file, assignment, ttab, generic)
- `primary_key`: Primary key field
- `batch_size`: Product-specific batch size
- `priority`: Processing priority

### 15. File Patterns Configuration

```json
{
  "file_patterns": {
    "csv_files": ["*.csv", "*.CSV"],
    "xml_files": ["*.xml", "*.XML"],
    "zip_files": ["*.zip", "*.ZIP"],
    "dta_files": ["*.dta", "*.DTA"],
    "excluded_files": ["*.tmp", "*.temp", "*.log", "*.bak"],
    "file_extensions": {
      "case_files": [".csv"],
      "assignment_files": [".csv", ".dta"],
      "ttab_files": [".xml"],
      "archive_files": [".zip"]
    }
  }
}
```

**Key Settings**:
- `csv_files`: CSV file patterns
- `xml_files`: XML file patterns
- `zip_files`: ZIP file patterns
- `dta_files`: DTA file patterns
- `excluded_files`: Files to exclude
- `file_extensions`: File extensions by type

### 16. Error Handling Configuration

```json
{
  "error_handling": {
    "max_errors_per_file": 100,
    "max_errors_per_batch": 50,
    "error_recovery": {
      "skip_corrupted_records": true,
      "retry_failed_operations": true,
      "continue_on_error": true,
      "log_all_errors": true
    },
    "error_categories": {
      "critical": ["database_connection", "file_not_found", "permission_denied"],
      "warning": ["data_validation", "format_warning", "performance_warning"],
      "info": ["processing_info", "progress_update", "completion_info"]
    }
  }
}
```

**Key Settings**:
- `max_errors_per_file`: Maximum errors per file
- `max_errors_per_batch`: Maximum errors per batch
- `error_recovery`: Error recovery options
- `error_categories`: Error categorization

### 17. Development Configuration

```json
{
  "development": {
    "debug_mode": false,
    "verbose_logging": false,
    "profile_performance": false,
    "test_mode": false,
    "mock_api_calls": false,
    "dry_run": false,
    "development_features": {
      "enable_hot_reload": false,
      "enable_debug_endpoints": false,
      "enable_test_data": false
    }
  }
}
```

**Key Settings**:
- `debug_mode`: Enable debug mode
- `verbose_logging`: Enable verbose logging
- `profile_performance`: Enable performance profiling
- `test_mode`: Enable test mode
- `mock_api_calls`: Mock API calls for testing
- `dry_run`: Enable dry run mode
- `development_features`: Development-specific features

## Configuration Best Practices

### 1. Start with Template
Use `uspto_config_template.json` for basic setup, then customize as needed.

### 2. Environment-Specific Configs
Create different configs for development, staging, and production.

### 3. Security Considerations
- Never commit passwords to version control
- Use environment variables for sensitive data
- Enable security features in production

### 4. Performance Tuning
- Adjust batch sizes based on system capabilities
- Enable parallel processing for multi-core systems
- Monitor memory usage and adjust limits

### 5. Monitoring Setup
- Enable monitoring for production environments
- Set appropriate alert thresholds
- Configure log rotation and retention

### 6. Backup Strategy
- Enable automatic backups for production
- Test backup and restore procedures
- Set appropriate retention periods

## Configuration Validation

The system validates configuration files on startup. Common validation errors:

1. **Missing Required Fields**: Ensure all required fields are present
2. **Invalid Values**: Check that values are within valid ranges
3. **File Paths**: Ensure all file paths are accessible
4. **Database Connection**: Test database connection settings
5. **API Endpoints**: Verify API endpoint URLs are correct

## Environment Variables

You can override configuration values using environment variables:

```bash
export USPTO_DB_PASSWORD="your_password"
export USPTO_LOG_LEVEL="DEBUG"
export USPTO_BATCH_SIZE="5000"
```

## Configuration Examples

### Development Configuration
```json
{
  "database": {
    "dbname": "trademarks_dev",
    "user": "postgres",
    "password": "dev_password",
    "host": "localhost",
    "port": "5432"
  },
  "processing": {
    "batch_size": 1000,
    "memory_limit_mb": 256
  },
  "development": {
    "debug_mode": true,
    "verbose_logging": true
  }
}
```

### Production Configuration
```json
{
  "database": {
    "dbname": "trademarks_prod",
    "user": "uspto_user",
    "password": "secure_password",
    "host": "prod-db-server",
    "port": "5432",
    "use_copy": true
  },
  "processing": {
    "batch_size": 20000,
    "memory_limit_mb": 1024,
    "max_workers": 4
  },
  "security": {
    "encrypt_sensitive_data": true,
    "secure_database_connection": true
  },
  "backup": {
    "enable_automatic_backup": true,
    "backup_frequency": "daily"
  }
}
```

### Low-Spec System Configuration
```json
{
  "processing": {
    "batch_size": 5000,
    "chunk_size": 25000,
    "memory_limit_mb": 256,
    "max_workers": 1
  },
  "orchestrator": {
    "max_files_per_product": 1,
    "enable_parallel_processing": false
  },
  "performance": {
    "optimization_level": "high",
    "memory_optimization": true,
    "disk_optimization": true
  }
}
```

This configuration system provides comprehensive control over all aspects of the USPTO data processing system, allowing for fine-tuned optimization and customization based on specific requirements and system capabilities.
