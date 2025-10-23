import os
import zipfile
import xml.etree.ElementTree as ET
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def debug_zip_contents(download_dir="./uspto_data"):
    """Debug what's inside the downloaded ZIP files"""
    zip_dir = f"{download_dir}/zips"
    
    if not os.path.exists(zip_dir):
        logger.error("Download directory doesn't exist")
        return
    
    for product_id in os.listdir(zip_dir):
        product_dir = os.path.join(zip_dir, product_id)
        if os.path.isdir(product_dir):
            logger.info(f"\n=== Checking product: {product_id} ===")
            
            for file_name in os.listdir(product_dir):
                if file_name.endswith('.zip'):
                    file_path = os.path.join(product_dir, file_name)
                    logger.info(f"\n--- File: {file_name} ---")
                    
                    try:
                        with zipfile.ZipFile(file_path, 'r') as zip_ref:
                            file_list = zip_ref.namelist()
                            logger.info(f"Contains {len(file_list)} files:")
                            
                            for i, inner_file in enumerate(file_list[:10]):  # Show first 10 files
                                logger.info(f"  {i+1}. {inner_file}")
                                
                                # If it's an XML file, try to peek at the structure
                                if inner_file.endswith('.xml'):
                                    try:
                                        with zip_ref.open(inner_file) as xml_file:
                                            content = xml_file.read(1000)  # Read first 1000 bytes
                                            logger.info(f"     XML preview: {content[:200]}...")
                                            
                                            # Try to parse the XML structure
                                            xml_file.seek(0)
                                            tree = ET.parse(xml_file)
                                            root = tree.getroot()
                                            logger.info(f"     Root tag: {root.tag}")
                                            
                                            # Look for common elements
                                            case_files = root.findall('.//case-file')
                                            transactions = root.findall('.//transaction')
                                            logger.info(f"     Found {len(case_files)} case-file elements")
                                            logger.info(f"     Found {len(transactions)} transaction elements")
                                            
                                    except Exception as e:
                                        logger.error(f"     Error reading XML: {e}")
                                
                            if len(file_list) > 10:
                                logger.info(f"  ... and {len(file_list) - 10} more files")
                                
                    except Exception as e:
                        logger.error(f"Error processing {file_name}: {e}")

if __name__ == "__main__":
    debug_zip_contents()