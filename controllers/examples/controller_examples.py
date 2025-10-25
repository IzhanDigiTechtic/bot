#!/usr/bin/env python3
"""
USPTO Controller Usage Examples
Examples showing how to use individual controllers or combine them
"""

from uspto_controllers import (
    USPTOAPIController,
    DownloadController, 
    ProcessingController,
    DatabaseController,
    USPTOOrchestrator,
    ProductInfo,
    FileInfo
)
import json

# Example 1: Using individual controllers
def example_individual_controllers():
    """Example of using controllers individually"""
    print("🔧 Example 1: Using Individual Controllers")
    print("=" * 50)
    
    # Configuration
    config = {
        'api_url': 'https://data.uspto.gov/ui/datasets/products/search',
        'download_dir': './uspto_data',
        'database': {
            'dbname': 'trademarks',
            'user': 'postgres',
            'password': '1234',
            'host': 'localhost',
            'port': '5432'
        },
        'batch_size': 5000,
        'chunk_size': 25000,
        'memory_limit_mb': 256
    }
    
    # Initialize controllers
    api_controller = USPTOAPIController(config)
    download_controller = DownloadController(config)
    processing_controller = ProcessingController(config)
    database_controller = DatabaseController(config)
    
    try:
        # Initialize all controllers
        controllers = [api_controller, download_controller, processing_controller, database_controller]
        for controller in controllers:
            if not controller.initialize():
                print(f"❌ Failed to initialize {controller.__class__.__name__}")
                return
        
        print("✅ All controllers initialized")
        
        # Step 1: Get products from API
        print("\n📡 Fetching products from API...")
        products = api_controller.get_trademark_datasets()
        print(f"Found {len(products)} products")
        
        # Step 2: Register first product
        if products:
            product = products[0]
            print(f"\n🏭 Registering product: {product.product_id}")
            database_controller.register_product(product)
            
            # Step 3: Download first file
            if product.files:
                file_info = product.files[0]
                print(f"\n📥 Downloading file: {file_info.filename}")
                file_path = download_controller.download_file(file_info)
                
                if file_path:
                    print(f"✅ Downloaded to: {file_path}")
                    
                    # Step 4: Process file
                    print(f"\n⚙️ Processing file...")
                    batch_count = 0
                    total_rows = 0
                    
                    for batch in processing_controller.process_csv_file(file_path, product.product_id):
                        batch_count += 1
                        result = database_controller.save_batch(batch, product.product_id, batch_count)
                        total_rows += result.rows_saved
                        
                        if batch_count % 10 == 0:
                            print(f"  Processed {batch_count} batches, {total_rows} rows saved")
                    
                    print(f"✅ Processing completed: {total_rows} rows saved")
        
    finally:
        # Cleanup
        for controller in controllers:
            controller.cleanup()
        print("🧹 Cleanup completed")

# Example 2: Using the orchestrator
def example_orchestrator():
    """Example of using the orchestrator for full pipeline"""
    print("\n🎯 Example 2: Using Orchestrator")
    print("=" * 50)
    
    # Configuration
    config = {
        'api_url': 'https://data.uspto.gov/ui/datasets/products/search',
        'download_dir': './uspto_data',
        'database': {
            'dbname': 'trademarks',
            'user': 'postgres',
            'password': '1234',
            'host': 'localhost',
            'port': '5432'
        },
        'batch_size': 10000,
        'chunk_size': 50000,
        'memory_limit_mb': 512,
        'use_copy': True
    }
    
    # Create orchestrator
    orchestrator = USPTOOrchestrator(config)
    
    try:
        # Initialize
        if not orchestrator.initialize():
            print("❌ Failed to initialize orchestrator")
            return
        
        print("✅ Orchestrator initialized")
        
        # Run full process (limited to 1 file per product for demo)
        print("\n🚀 Running full processing pipeline...")
        results = orchestrator.run_full_process(max_files_per_product=1)
        
        # Print results
        print("\n📊 Results:")
        print(f"  Success: {results['success']}")
        print(f"  Products: {results['products_processed']}")
        print(f"  Files: {results['files_processed']}")
        print(f"  Rows Processed: {results['total_rows_processed']:,}")
        print(f"  Rows Saved: {results['total_rows_saved']:,}")
        
        if results['errors']:
            print(f"  Errors: {len(results['errors'])}")
    
    finally:
        orchestrator.cleanup()
        print("🧹 Cleanup completed")

# Example 3: Custom processing workflow
def example_custom_workflow():
    """Example of custom processing workflow"""
    print("\n🛠️ Example 3: Custom Workflow")
    print("=" * 50)
    
    config = {
        'api_url': 'https://data.uspto.gov/ui/datasets/products/search',
        'download_dir': './uspto_data',
        'database': {
            'dbname': 'trademarks',
            'user': 'postgres',
            'password': '1234',
            'host': 'localhost',
            'port': '5432'
        },
        'batch_size': 5000,
        'chunk_size': 25000,
        'memory_limit_mb': 256
    }
    
    # Initialize only the controllers we need
    api_controller = USPTOAPIController(config)
    database_controller = DatabaseController(config)
    
    try:
        api_controller.initialize()
        database_controller.initialize()
        
        print("✅ Controllers initialized")
        
        # Get products
        products = api_controller.get_trademark_datasets()
        
        # Process only TRCFECO2 products
        trcfeco_products = [p for p in products if 'TRCFECO' in p.product_id]
        
        print(f"\n🎯 Found {len(trcfeco_products)} TRCFECO products")
        
        for product in trcfeco_products:
            print(f"\n📋 Processing: {product.title}")
            
            # Register product
            database_controller.register_product(product)
            
            # Get processing stats
            stats = database_controller.get_processing_stats()
            print(f"📊 Database stats: {stats}")
        
    finally:
        api_controller.cleanup()
        database_controller.cleanup()
        print("🧹 Cleanup completed")

# Example 4: Error handling and recovery
def example_error_handling():
    """Example of error handling and recovery"""
    print("\n🛡️ Example 4: Error Handling")
    print("=" * 50)
    
    config = {
        'api_url': 'https://data.uspto.gov/ui/datasets/products/search',
        'download_dir': './uspto_data',
        'database': {
            'dbname': 'trademarks',
            'user': 'postgres',
            'password': '1234',
            'host': 'localhost',
            'port': '5432'
        },
        'batch_size': 1000,  # Small batch for demo
        'chunk_size': 5000,
        'memory_limit_mb': 128
    }
    
    orchestrator = USPTOOrchestrator(config)
    
    try:
        if not orchestrator.initialize():
            print("❌ Initialization failed")
            return
        
        print("✅ Orchestrator initialized")
        
        # Run with error handling
        try:
            results = orchestrator.run_full_process(max_files_per_product=1)
            
            if results['success']:
                print("✅ Processing completed successfully")
            else:
                print("⚠️ Processing completed with errors")
                
            # Handle errors
            if results['errors']:
                print(f"\n❌ Errors encountered ({len(results['errors'])}):")
                for i, error in enumerate(results['errors'][:5], 1):  # Show first 5 errors
                    print(f"  {i}. {error}")
                
                if len(results['errors']) > 5:
                    print(f"  ... and {len(results['errors']) - 5} more errors")
        
        except Exception as e:
            print(f"❌ Processing failed with exception: {e}")
    
    finally:
        orchestrator.cleanup()
        print("🧹 Cleanup completed")

def main():
    """Run all examples"""
    print("🎓 USPTO Controller Usage Examples")
    print("=" * 60)
    
    try:
        # Run examples
        example_individual_controllers()
        example_orchestrator()
        example_custom_workflow()
        example_error_handling()
        
        print("\n🎉 All examples completed!")
        
    except KeyboardInterrupt:
        print("\n⚠️ Examples interrupted by user")
    except Exception as e:
        print(f"\n❌ Error running examples: {e}")

if __name__ == "__main__":
    main()
