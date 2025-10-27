#!/usr/bin/env python3
"""
USPTO Controller-Based Configuration and Runner
Configuration management and runner for the controller-based USPTO processor
"""

import os
import json
import argparse
from pathlib import Path
from .uspto_controllers import USPTOOrchestrator

# Default configuration for controller-based system
DEFAULT_CONTROLLER_CONFIG = {
    "api": {
        "api_url": "https://data.uspto.gov/ui/datasets/products/search",
        "timeout": 30,
        "retry_attempts": 3
    },
    "download": {
        "download_dir": "./uspto_data",
        "keep_latest_zips": 10,
        "force_redownload": False
    },
    "processing": {
        "batch_size": 10000,
        "chunk_size": 50000,
        "memory_limit_mb": 512,
        "max_workers": 2
    },
    "database": {
        "dbname": "trademarks",
        "user": "postgres",
        "password": "1234",
        "host": "localhost",
        "port": "5432",
        "use_copy": True
    },
    "orchestrator": {
        "max_files_per_product": 2,
        "enable_parallel_processing": False,
        "log_level": "INFO"
    }
}

class ControllerConfig:
    """Configuration manager for controller-based system"""
    
    def __init__(self, config_file: str = "uspto_controller_config.json"):
        self.config_file = Path(config_file)
        self.config = self._load_config()
    
    def _load_config(self) -> dict:
        """Load configuration from file or create default"""
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r') as f:
                    config = json.load(f)
                # Merge with defaults to ensure all keys exist
                return self._merge_configs(DEFAULT_CONTROLLER_CONFIG, config)
            except Exception as e:
                print(f"Error loading config file: {e}")
                print("Using default configuration")
                return DEFAULT_CONTROLLER_CONFIG.copy()
        else:
            # Create default config file
            self._save_config(DEFAULT_CONTROLLER_CONFIG)
            return DEFAULT_CONTROLLER_CONFIG.copy()
    
    def _merge_configs(self, default: dict, user: dict) -> dict:
        """Recursively merge user config with defaults"""
        result = default.copy()
        for key, value in user.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_configs(result[key], value)
            else:
                result[key] = value
        return result
    
    def _save_config(self, config: dict):
        """Save configuration to file"""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(config, f, indent=2)
            print(f"Configuration saved to {self.config_file}")
        except Exception as e:
            print(f"Error saving config: {e}")
    
    def get(self, key_path: str, default=None):
        """Get configuration value using dot notation (e.g., 'database.host')"""
        keys = key_path.split('.')
        value = self.config
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default
    
    def set(self, key_path: str, value):
        """Set configuration value using dot notation"""
        keys = key_path.split('.')
        config = self.config
        for key in keys[:-1]:
            if key not in config:
                config[key] = {}
            config = config[key]
        config[keys[-1]] = value
    
    def save(self):
        """Save current configuration"""
        self._save_config(self.config)
    
    def get_flat_config(self) -> dict:
        """Get flattened configuration for orchestrator"""
        flat_config = {}
        
        # Flatten nested config
        for section, values in self.config.items():
            if isinstance(values, dict):
                for key, value in values.items():
                    flat_config[key] = value
            else:
                flat_config[section] = values
        
        return flat_config

def setup_environment():
    """Setup environment variables for optimal performance"""
    # Set environment variables for memory management
    os.environ['USPTO_CSV_CHUNKSIZE'] = '50000'
    os.environ['USPTO_DB_BATCH_SIZE'] = '5000'
    os.environ['USPTO_USE_COPY'] = 'true'  # Use COPY for faster inserts
    
    # Set pandas options for better memory usage
    import pandas as pd
    pd.set_option('mode.chained_assignment', None)
    pd.set_option('display.max_columns', None)

def run_controller_processor(config_file: str = "uspto_controller_config.json", 
                           max_files: int = None,
                           force_redownload: bool = False,
                           batch_size: int = None,
                           memory_limit: int = None,
                           product_id: str = None,
                           skip_products: str = None,
                           only_products: str = None):
    """Run the controller-based USPTO processor"""
    
    # Load configuration
    config = ControllerConfig(config_file)
    
    # Override config with command line arguments
    if max_files is not None:
        config.set('orchestrator.max_files_per_product', max_files)
    if force_redownload:
        config.set('download.force_redownload', True)
    if batch_size is not None:
        config.set('processing.batch_size', batch_size)
    if memory_limit is not None:
        config.set('processing.memory_limit_mb', memory_limit)
    
    # Apply product filters from CLI (comma-separated)
    if skip_products:
        skips = [p.strip().upper() for p in skip_products.split(',') if p.strip()]
        config.set('skip_products', skips)
    if only_products:
        onlys = [p.strip().upper() for p in only_products.split(',') if p.strip()]
        config.set('only_products', onlys)
    
    # Setup environment
    setup_environment()
    
    # Get flattened configuration for orchestrator
    orchestrator_config = config.get_flat_config()
    
    # Create orchestrator
    orchestrator = USPTOOrchestrator(orchestrator_config)
    
    try:
        # Initialize
        print("Initializing USPTO Controller System...")
        if not orchestrator.initialize():
            print("❌ Failed to initialize orchestrator")
            return False
        
        print("✅ All controllers initialized successfully")
        
        # Run full process
        print("\n🚀 Starting USPTO data processing pipeline...")
        results = orchestrator.run_full_process(
            max_files_per_product=config.get('orchestrator.max_files_per_product'),
            force_redownload=config.get('download.force_redownload')
        )
        
        # Print detailed results
        print("\n" + "="*60)
        print("📊 PROCESSING RESULTS")
        print("="*60)
        print(f"✅ Success: {results['success']}")
        print(f"🏭 Products Processed: {results['products_processed']}")
        print(f"📁 Files Processed: {results['files_processed']}")
        print(f"📝 Total Rows Processed: {results['total_rows_processed']:,}")
        print(f"💾 Total Rows Saved: {results['total_rows_saved']:,}")
        
        if results['total_rows_processed'] > 0:
            success_rate = (results['total_rows_saved'] / results['total_rows_processed']) * 100
            print(f"📈 Success Rate: {success_rate:.1f}%")
        
        if results['errors']:
            print(f"\n❌ Errors ({len(results['errors'])}):")
            for i, error in enumerate(results['errors'], 1):
                print(f"  {i}. {error}")
        else:
            print("\n🎉 No errors encountered!")
        
        return results['success']
    
    except KeyboardInterrupt:
        print("\n⚠️ Processing interrupted by user")
        return False
    except Exception as e:
        print(f"❌ Error during processing: {e}")
        return False
    finally:
        # Cleanup
        print("\n🧹 Cleaning up resources...")
        orchestrator.cleanup()
        print("✅ Cleanup completed")

def test_controllers(config_file: str = "uspto_controller_config.json"):
    """Test individual controllers"""
    print("Testing USPTO Controllers...")
    
    config = ControllerConfig(config_file)
    orchestrator_config = config.get_flat_config()
    
    # Test each controller individually
    from .uspto_controllers import (
        USPTOAPIController, 
        DownloadController, 
        ProcessingController, 
        DatabaseController
    )
    
    controllers = [
        ("API Controller", USPTOAPIController(orchestrator_config)),
        ("Download Controller", DownloadController(orchestrator_config)),
        ("Processing Controller", ProcessingController(orchestrator_config)),
        ("Database Controller", DatabaseController(orchestrator_config))
    ]
    
    results = {}
    
    for name, controller in controllers:
        print(f"\nTesting {name}...")
        try:
            success = controller.initialize()
            if success:
                print(f"PASS {name} initialized successfully")
                results[name] = True
            else:
                print(f"FAIL {name} failed to initialize")
                results[name] = False
        except Exception as e:
            print(f"ERROR {name} error: {e}")
            results[name] = False
        finally:
            try:
                controller.cleanup()
            except:
                pass
    
    # Summary
    print("\n" + "="*40)
    print("CONTROLLER TEST RESULTS")
    print("="*40)
    for name, success in results.items():
        status = "PASS" if success else "FAIL"
        print(f"{status} {name}")
    
    all_passed = all(results.values())
    print(f"\nOverall: {'ALL TESTS PASSED' if all_passed else 'SOME TESTS FAILED'}")
    
    return all_passed

def main():
    """Main function with command line interface"""
    parser = argparse.ArgumentParser(
        description='USPTO Controller-Based Data Processor',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default settings
  python uspto_controller_runner.py
  
  # Process only 1 file per product
  python uspto_controller_runner.py --max-files 1
  
  # Force redownload of all files
  python uspto_controller_runner.py --force-redownload
  
  # Test all controllers
  python uspto_controller_runner.py --test
  
  # Optimize for low-memory system
  python uspto_controller_runner.py --batch-size 5000 --memory-limit 256
  
  # Use custom configuration file
  python uspto_controller_runner.py --config my_config.json
        """
    )
    
    parser.add_argument('--config', type=str, default='uspto_controller_config.json',
                       help='Configuration file path (default: uspto_controller_config.json)')
    parser.add_argument('--max-files', type=int, 
                       help='Maximum files to process per product')
    parser.add_argument('--force-redownload', action='store_true',
                       help='Force redownload of all files')
    parser.add_argument('--batch-size', type=int,
                       help='Batch size for processing (default: 10000)')
    parser.add_argument('--memory-limit', type=int,
                       help='Memory limit in MB (default: 512)')
    parser.add_argument('--product-id', type=str,
                       help='Process specific product only')
    parser.add_argument('--test', action='store_true',
                       help='Test all controllers')
    parser.add_argument('--skip-products', type=str,
                       help='Comma-separated product IDs to skip (e.g., TRCFECO2,TRASECO)')
    parser.add_argument('--only-products', type=str,
                       help='Comma-separated product IDs to exclusively process')
    
    args = parser.parse_args()
    
    if args.test:
        # Test controllers
        success = test_controllers(args.config)
        exit(0 if success else 1)
    
    # Run processor
    success = run_controller_processor(
        config_file=args.config,
        max_files=args.max_files,
        force_redownload=args.force_redownload,
        batch_size=args.batch_size,
        memory_limit=args.memory_limit,
        product_id=args.product_id,
        skip_products=args.skip_products,
        only_products=args.only_products
    )
    
    exit(0 if success else 1)

if __name__ == "__main__":
    main()
