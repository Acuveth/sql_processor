#!/usr/bin/env python3
"""
Large JSON File Processor
Process thousands of products from JSON files with AI enhancement
"""

import os
import json
import time
import argparse
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging
from dotenv import load_dotenv
from product_ai_enhancer import ProductProcessor
from database_integration import ProductPipeline

# Configure detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('product_processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class LargeJSONProcessor:
    """Process large JSON files with product data"""
    
    def __init__(self, gemini_api_key: str, db_config: Optional[Dict] = None):
        """Initialize processor"""
        self.processor = ProductProcessor(gemini_api_key)
        self.db_pipeline = ProductPipeline(gemini_api_key, db_config) if db_config else None
        self.stats = {
            'total': 0,
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'start_time': None,
            'end_time': None
        }
    
    def load_json_file(self, file_path: str) -> List[Dict]:
        """Load and parse JSON file"""
        logger.info(f"Loading JSON file: {file_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Handle different JSON structures
            if isinstance(data, list):
                products = data
                logger.info(f"Found direct array with {len(products)} products")
            elif isinstance(data, dict):
                if 'products' in data:
                    products = data['products']
                    logger.info(f"Found products array with {len(products)} products")
                elif 'data' in data and isinstance(data['data'], list):
                    products = data['data']
                    logger.info(f"Found data array with {len(products)} products")
                elif 'items' in data:
                    products = data['items']
                    logger.info(f"Found items array with {len(products)} products")
                else:
                    # Try to find any array field
                    for key, value in data.items():
                        if isinstance(value, list) and len(value) > 0:
                            products = value
                            logger.info(f"Found {key} array with {len(products)} products")
                            break
                    else:
                        raise ValueError("No product array found in JSON")
            else:
                raise ValueError("JSON must be array or object with product array")
            
            return products
            
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format: {e}")
            raise
        except Exception as e:
            logger.error(f"Error loading JSON: {e}")
            raise
    
    def save_progress(self, results: List[Dict], batch_num: int, store_type: str):
        """Save intermediate results"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"enhanced_products_{store_type}_batch_{batch_num}_{timestamp}.json"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved batch {batch_num} to {filename}")
            return filename
        except Exception as e:
            logger.error(f"Error saving batch {batch_num}: {e}")
            return None
    
    def save_final_results(self, all_results: List[Dict], store_type: str, original_filename: str):
        """Save final complete results"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = os.path.splitext(os.path.basename(original_filename))[0]
        filename = f"enhanced_{base_name}_{store_type}_{timestamp}.json"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump({
                    'metadata': {
                        'original_file': original_filename,
                        'store_type': store_type,
                        'total_products': len(all_results),
                        'processing_stats': self.stats,
                        'enhanced_at': timestamp
                    },
                    'products': all_results
                }, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Final results saved to {filename}")
            return filename
        except Exception as e:
            logger.error(f"Error saving final results: {e}")
            return None
    
    def process_batch(self, products: List[Dict], store_type: str, batch_num: int, 
                     batch_size: int, delay: float = 2.0) -> List[Dict]:
        """Process a batch of products"""
        results = []
        
        logger.info(f"Processing batch {batch_num}: {len(products)} products")
        
        for i, product in enumerate(products):
            try:
                # Get product name for logging
                product_name = self.get_product_name(product, store_type)
                logger.info(f"  [{batch_num}.{i+1}] Processing: {product_name}")
                
                # Process with AI
                result = self.processor.process_product(product, store_type)
                
                if result:
                    results.append(result)
                    self.stats['successful'] += 1
                    logger.info(f"    ✅ Success: {result.get('ai_main_category')} | Score: {result.get('ai_health_score')}")
                else:
                    self.stats['failed'] += 1
                    logger.warning(f"    ❌ Failed to process: {product_name}")
                
                # NEW: Log the problematic product data for debugging
                logger.debug(f"    🔍 Product data: {json.dumps(product, indent=2)}")

                self.stats['processed'] += 1
                
                # Progress indicator
                if (i + 1) % 5 == 0:
                    logger.info(f"    Progress: {i+1}/{len(products)} in batch {batch_num}")
                
                # Rate limiting delay
                if delay > 0:
                    time.sleep(delay)
                    
            except Exception as e:
                self.stats['failed'] += 1
                logger.error(f"    💥 Exception processing product {i+1}: {e}")
                logger.error(f"    🔍 Product data: {json.dumps(product, indent=2)}")
                continue
        
        return results
    
    def get_product_name(self, product: Dict, store_type: str) -> str:
        """Extract product name based on store type"""
        try:
            if store_type == "dm":
                return product.get("name", "Unknown DM Product")
            elif store_type == "mercator":
                return product.get("data", {}).get("name", "Unknown Mercator Product")
            elif store_type == "spar":
                return product.get("masterValues", {}).get("name", "Unknown Spar Product")
            elif store_type == "tus":
                return product.get("name", "Unknown TUS Product")
            elif store_type == "lidl":
                return product.get("name", "Unknown Lidl Product")
            else:
                return f"Unknown {store_type} Product"
        except:
            return f"Unknown {store_type} Product"
    
    def print_progress_stats(self, current_batch: int, total_batches: int):
        """Print current progress statistics"""
        if self.stats['start_time']:
            elapsed = time.time() - self.stats['start_time']
            rate = self.stats['processed'] / elapsed if elapsed > 0 else 0
            
            print(f"\n📊 PROGRESS UPDATE - Batch {current_batch}/{total_batches}")
            print(f"   ⏱️  Elapsed: {elapsed/60:.1f} minutes")
            print(f"   📈 Processed: {self.stats['processed']}/{self.stats['total']}")
            print(f"   ✅ Successful: {self.stats['successful']}")
            print(f"   ❌ Failed: {self.stats['failed']}")
            print(f"   🚀 Rate: {rate:.1f} products/second")
            print(f"   💡 Success Rate: {(self.stats['successful']/max(self.stats['processed'], 1)*100):.1f}%")
    
    def process_file(self, json_file_path: str, store_type: str, 
                    batch_size: int = 25, delay: float = 2.0, 
                    save_to_db: bool = False, save_batches: bool = True) -> Dict[str, Any]:
        """Process entire JSON file"""
        
        logger.info(f"🚀 Starting large file processing")
        logger.info(f"   File: {json_file_path}")
        logger.info(f"   Store: {store_type}")
        logger.info(f"   Batch size: {batch_size}")
        logger.info(f"   Delay: {delay}s between products")
        logger.info(f"   Save to DB: {save_to_db}")
        
        # Load products
        products = self.load_json_file(json_file_path)
        self.stats['total'] = len(products)
        self.stats['start_time'] = time.time()
        
        # Calculate batches
        total_batches = (len(products) + batch_size - 1) // batch_size
        logger.info(f"   Total batches: {total_batches}")
        
        all_results = []
        
        # Process in batches
        for i in range(0, len(products), batch_size):
            batch_num = (i // batch_size) + 1
            batch = products[i:i + batch_size]
            
            try:
                # Process batch
                batch_results = self.process_batch(batch, store_type, batch_num, batch_size, delay)
                all_results.extend(batch_results)
                
                # Save intermediate results
                if save_batches and batch_results:
                    self.save_progress(batch_results, batch_num, store_type)
                
                # Save to database if requested
                if save_to_db and self.db_pipeline and batch_results:
                    try:
                        db_stats = self.db_pipeline.process_and_store_products(
                            [r for r in batch if r], store_type
                        )
                        logger.info(f"    💾 DB: {db_stats['stored']}/{len(batch_results)} stored")
                    except Exception as e:
                        logger.error(f"    💥 DB Error: {e}")
                
                # Progress update
                self.print_progress_stats(batch_num, total_batches)
                
                # Longer delay between batches
                if batch_num < total_batches:
                    time.sleep(5)  # 5 second pause between batches
                    
            except KeyboardInterrupt:
                logger.warning("⚠️  Processing interrupted by user")
                break
            except Exception as e:
                logger.error(f"💥 Error processing batch {batch_num}: {e}")
                continue
        
        # Final statistics
        self.stats['end_time'] = time.time()
        total_time = self.stats['end_time'] - self.stats['start_time']
        
        # Save final results
        final_file = self.save_final_results(all_results, store_type, json_file_path)
        
        # Print final report
        self.print_final_report(total_time, final_file)
        
        return {
            'stats': self.stats,
            'results': all_results,
            'output_file': final_file
        }
    
    def print_final_report(self, total_time: float, output_file: str):
        """Print comprehensive final report"""
        print(f"\n🎉 PROCESSING COMPLETE!")
        print(f"=" * 60)
        print(f"📊 FINAL STATISTICS:")
        print(f"   📦 Total Products: {self.stats['total']}")
        print(f"   ✅ Successfully Enhanced: {self.stats['successful']}")
        print(f"   ❌ Failed: {self.stats['failed']}")
        print(f"   📈 Success Rate: {(self.stats['successful']/max(self.stats['total'], 1)*100):.1f}%")
        print(f"   ⏱️  Total Time: {total_time/60:.1f} minutes")
        print(f"   🚀 Average Rate: {self.stats['successful']/total_time:.1f} products/second")
        print(f"   💾 Output File: {output_file}")
        
        if self.stats['successful'] > 0:
            cost_estimate = self.stats['successful'] * 0.005  # Rough estimate
            print(f"   💰 Estimated Cost: ~${cost_estimate:.2f}")

def main():
    """Main function with command line arguments"""
    parser = argparse.ArgumentParser(description='Process large JSON files with AI enhancement')
    parser.add_argument('file_path', help='Path to JSON file')
    parser.add_argument('store_type', choices=['dm', 'mercator', 'spar', 'tus', 'lidl'], 
                       help='Store type')
    parser.add_argument('--batch-size', type=int, default=25, 
                       help='Products per batch (default: 25)')
    parser.add_argument('--delay', type=float, default=2.0, 
                       help='Delay between products in seconds (default: 2.0)')
    parser.add_argument('--no-save-batches', action='store_true', 
                       help='Don\'t save intermediate batch files')
    parser.add_argument('--save-to-db', action='store_true', 
                       help='Save results to database')
    parser.add_argument('--max-products', type=int, 
                       help='Limit processing to first N products (for testing)')
    
    args = parser.parse_args()
    
    # Load environment
    load_dotenv()
    
    # Database config (if saving to DB)
    db_config = None
    if args.save_to_db:
        db_config = {
            'host': os.getenv("DB_HOST", "localhost"),
            'database': os.getenv("DB_NAME", "ai_food"),
            'user': os.getenv("DB_USER", "root"),
            'password': os.getenv("DB_PASSWORD", "pass"),
            'port': int(os.getenv("DB_PORT", 3306))
        }
    
    # Initialize processor
    processor = LargeJSONProcessor(os.getenv("GEMINI_API_KEY"), db_config)
    
    # Process file
    try:
        results = processor.process_file(
            args.file_path,
            args.store_type,
            batch_size=args.batch_size,
            delay=args.delay,
            save_to_db=args.save_to_db,
            save_batches=not args.no_save_batches
        )
        
        print(f"\n✅ Processing completed successfully!")
        print(f"📄 Enhanced data saved to: {results['output_file']}")
        
    except Exception as e:
        logger.error(f"💥 Processing failed: {e}")
        raise

def quick_test():
    """Quick test function for small samples"""
    print("🧪 QUICK TEST MODE")
    print("=" * 30)
    
    # You can modify this to test with your actual file
    file_path = input("Enter JSON file path: ")
    store_type = input("Enter store type (dm/mercator/spar/tus/lidl): ")
    
    load_dotenv()
    
    # Test with small batch
    processor = LargeJSONProcessor(os.getenv("GEMINI_API_KEY"))
    
    # Load just first 5 products for testing
    products = processor.load_json_file(file_path)[:5]
    print(f"Testing with first {len(products)} products...")
    
    results = []
    for i, product in enumerate(products):
        print(f"\nTesting product {i+1}...")
        result = processor.processor.process_product(product, store_type)
        if result:
            results.append(result)
            print(f"✅ {result.get('ai_main_category')} - {result.get('ai_health_score')}")
        else:
            print("❌ Failed")
    
    print(f"\n🎯 Test Results: {len(results)}/{len(products)} successful")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) == 1:
        print("🚀 Large JSON Processor")
        print("=" * 30)
        print("Usage options:")
        print("1. python process_large_json.py <file.json> <store_type>")
        print("2. Run quick_test() for small samples")
        print("\nStore types: dm, mercator, spar, tus, lidl")
        print("\nExample:")
        print("python process_large_json.py mercator_products.json mercator")
        
        if input("\nRun quick test? (y/n): ").lower() == 'y':
            quick_test()
    else:
        main()