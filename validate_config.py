#!/usr/bin/env python3
"""
USPTO Configuration Validator
Validates the configuration file and checks for common issues.
"""

import json
import os
import sys
from pathlib import Path
from typing import Dict, Any, List, Tuple

class ConfigurationValidator:
    """Validates USPTO configuration files"""
    
    def __init__(self, config_file: str = "uspto_config.json"):
        self.config_file = config_file
        self.config = None
        self.errors = []
        self.warnings = []
    
    def load_config(self) -> bool:
        """Load configuration file"""
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
            return True
        except FileNotFoundError:
            self.errors.append(f"Configuration file not found: {self.config_file}")
            return False
        except json.JSONDecodeError as e:
            self.errors.append(f"Invalid JSON in configuration file: {e}")
            return False
        except Exception as e:
            self.errors.append(f"Error loading configuration file: {e}")
            return False
    
    def validate_required_sections(self) -> None:
        """Validate required configuration sections"""
        required_sections = [
            "api", "download", "processing", "database", "orchestrator"
        ]
        
        for section in required_sections:
            if section not in self.config:
                self.errors.append(f"Missing required section: {section}")
    
    def validate_api_config(self) -> None:
        """Validate API configuration"""
        if "api" not in self.config:
            return
        
        api_config = self.config["api"]
        
        # Required fields
        required_fields = ["base_url", "search_endpoint", "timeout"]
        for field in required_fields:
            if field not in api_config:
                self.errors.append(f"Missing required API field: {field}")
        
        # Validate URLs
        if "base_url" in api_config:
            url = api_config["base_url"]
            if not url.startswith(("http://", "https://")):
                self.errors.append(f"Invalid base_url format: {url}")
        
        # Validate numeric fields
        numeric_fields = ["timeout", "retry_attempts", "retry_delay"]
        for field in numeric_fields:
            if field in api_config:
                try:
                    value = int(api_config[field])
                    if value <= 0:
                        self.errors.append(f"Invalid {field} value: {value} (must be positive)")
                except (ValueError, TypeError):
                    self.errors.append(f"Invalid {field} value: {api_config[field]} (must be integer)")
    
    def validate_download_config(self) -> None:
        """Validate download configuration"""
        if "download" not in self.config:
            return
        
        download_config = self.config["download"]
        
        # Validate directory paths
        dir_fields = ["base_dir", "zips_dir", "extracted_dir", "processed_dir"]
        for field in dir_fields:
            if field in download_config:
                path = download_config[field]
                if not isinstance(path, str):
                    self.errors.append(f"Invalid {field} value: {path} (must be string)")
                elif not path.startswith(("./", "/", "C:\\", "D:\\")):
                    self.warnings.append(f"Suspicious {field} path: {path}")
        
        # Validate numeric fields
        numeric_fields = ["keep_latest_zips", "chunk_size", "max_concurrent_downloads"]
        for field in numeric_fields:
            if field in download_config:
                try:
                    value = int(download_config[field])
                    if value <= 0:
                        self.errors.append(f"Invalid {field} value: {value} (must be positive)")
                except (ValueError, TypeError):
                    self.errors.append(f"Invalid {field} value: {download_config[field]} (must be integer)")
    
    def validate_processing_config(self) -> None:
        """Validate processing configuration"""
        if "processing" not in self.config:
            return
        
        processing_config = self.config["processing"]
        
        # Validate batch size
        if "batch_size" in processing_config:
            try:
                batch_size = int(processing_config["batch_size"])
                if batch_size < 100:
                    self.warnings.append(f"Small batch_size: {batch_size} (may impact performance)")
                elif batch_size > 100000:
                    self.warnings.append(f"Large batch_size: {batch_size} (may cause memory issues)")
            except (ValueError, TypeError):
                self.errors.append(f"Invalid batch_size value: {processing_config['batch_size']}")
        
        # Validate memory limit
        if "memory_limit_mb" in processing_config:
            try:
                memory_limit = int(processing_config["memory_limit_mb"])
                if memory_limit < 256:
                    self.warnings.append(f"Low memory_limit_mb: {memory_limit} (may cause issues)")
                elif memory_limit > 8192:
                    self.warnings.append(f"High memory_limit_mb: {memory_limit} (may exceed system limits)")
            except (ValueError, TypeError):
                self.errors.append(f"Invalid memory_limit_mb value: {processing_config['memory_limit_mb']}")
    
    def validate_database_config(self) -> None:
        """Validate database configuration"""
        if "database" not in self.config:
            return
        
        db_config = self.config["database"]
        
        # Required fields
        required_fields = ["dbname", "user", "host", "port"]
        for field in required_fields:
            if field not in db_config:
                self.errors.append(f"Missing required database field: {field}")
        
        # Validate port
        if "port" in db_config:
            try:
                port = int(db_config["port"])
                if not (1 <= port <= 65535):
                    self.errors.append(f"Invalid port value: {port} (must be 1-65535)")
            except (ValueError, TypeError):
                self.errors.append(f"Invalid port value: {db_config['port']}")
        
        # Check for default password
        if "password" in db_config:
            password = db_config["password"]
            if password in ["1234", "password", "admin", "root"]:
                self.warnings.append("Using default/weak database password")
        
        # Validate boolean fields
        boolean_fields = ["use_copy", "enable_logging", "vacuum_after_processing"]
        for field in boolean_fields:
            if field in db_config:
                value = db_config[field]
                if not isinstance(value, bool):
                    self.errors.append(f"Invalid {field} value: {value} (must be boolean)")
    
    def validate_orchestrator_config(self) -> None:
        """Validate orchestrator configuration"""
        if "orchestrator" not in self.config:
            return
        
        orchestrator_config = self.config["orchestrator"]
        
        # Validate max_files_per_product
        if "max_files_per_product" in orchestrator_config:
            try:
                max_files = int(orchestrator_config["max_files_per_product"])
                if max_files <= 0:
                    self.errors.append(f"Invalid max_files_per_product value: {max_files}")
            except (ValueError, TypeError):
                self.errors.append(f"Invalid max_files_per_product value: {orchestrator_config['max_files_per_product']}")
        
        # Validate log level
        if "log_level" in orchestrator_config:
            log_level = orchestrator_config["log_level"]
            valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
            if log_level.upper() not in valid_levels:
                self.errors.append(f"Invalid log_level: {log_level} (must be one of {valid_levels})")
    
    def validate_file_paths(self) -> None:
        """Validate file paths and directories"""
        if not self.config:
            return
        
        # Check if directories exist or can be created
        dir_fields = ["base_dir", "zips_dir", "extracted_dir", "processed_dir"]
        for section in ["download", "logging"]:
            if section in self.config:
                config_section = self.config[section]
                for field in dir_fields:
                    if field in config_section:
                        path = Path(config_section[field])
                        if not path.exists():
                            try:
                                path.mkdir(parents=True, exist_ok=True)
                                print(f"✅ Created directory: {path}")
                            except Exception as e:
                                self.errors.append(f"Cannot create directory {path}: {e}")
    
    def validate_database_connection(self) -> None:
        """Test database connection"""
        if "database" not in self.config:
            return
        
        db_config = self.config["database"]
        
        try:
            import psycopg2
            
            # Test connection
            conn = psycopg2.connect(
                dbname=db_config.get("dbname", "trademarks"),
                user=db_config.get("user", "postgres"),
                password=db_config.get("password", ""),
                host=db_config.get("host", "localhost"),
                port=db_config.get("port", "5432")
            )
            conn.close()
            print("✅ Database connection successful")
            
        except ImportError:
            self.warnings.append("psycopg2 not installed - cannot test database connection")
        except Exception as e:
            self.errors.append(f"Database connection failed: {e}")
    
    def validate_api_endpoint(self) -> None:
        """Test API endpoint accessibility"""
        if "api" not in self.config:
            return
        
        api_config = self.config["api"]
        
        try:
            import requests
            
            # Test API endpoint
            url = api_config.get("full_url", api_config.get("base_url", ""))
            if url:
                response = requests.get(url, timeout=10)
                if response.status_code == 200:
                    print("✅ API endpoint accessible")
                else:
                    self.warnings.append(f"API endpoint returned status {response.status_code}")
            
        except ImportError:
            self.warnings.append("requests not installed - cannot test API endpoint")
        except Exception as e:
            self.warnings.append(f"API endpoint test failed: {e}")
    
    def validate_all(self) -> Tuple[bool, List[str], List[str]]:
        """Run all validation checks"""
        print(f"🔍 Validating configuration file: {self.config_file}")
        print("=" * 50)
        
        # Load configuration
        if not self.load_config():
            return False, self.errors, self.warnings
        
        # Run validation checks
        self.validate_required_sections()
        self.validate_api_config()
        self.validate_download_config()
        self.validate_processing_config()
        self.validate_database_config()
        self.validate_orchestrator_config()
        self.validate_file_paths()
        
        # Test connections
        self.validate_database_connection()
        self.validate_api_endpoint()
        
        # Summary
        print("\n" + "=" * 50)
        print("📊 Validation Summary")
        print("=" * 50)
        
        if self.errors:
            print(f"❌ Errors found: {len(self.errors)}")
            for error in self.errors:
                print(f"  • {error}")
        
        if self.warnings:
            print(f"⚠️ Warnings found: {len(self.warnings)}")
            for warning in self.warnings:
                print(f"  • {warning}")
        
        if not self.errors and not self.warnings:
            print("✅ Configuration is valid!")
        
        return len(self.errors) == 0, self.errors, self.warnings

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Validate USPTO configuration file")
    parser.add_argument("--config", "-c", default="uspto_config.json", 
                       help="Configuration file to validate")
    parser.add_argument("--fix", "-f", action="store_true",
                       help="Attempt to fix common issues")
    
    args = parser.parse_args()
    
    validator = ConfigurationValidator(args.config)
    is_valid, errors, warnings = validator.validate_all()
    
    if args.fix and errors:
        print("\n🔧 Attempting to fix common issues...")
        # Add fix logic here if needed
    
    if not is_valid:
        print(f"\n❌ Configuration validation failed with {len(errors)} errors")
        sys.exit(1)
    elif warnings:
        print(f"\n⚠️ Configuration validation passed with {len(warnings)} warnings")
        sys.exit(0)
    else:
        print(f"\n✅ Configuration validation passed successfully!")
        sys.exit(0)

if __name__ == "__main__":
    main()
