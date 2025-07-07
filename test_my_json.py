#!/usr/bin/env python3
"""
Simple JSON File Tester
Quick way to test your JSON files with AI enhancement
"""

import os
import json
import time
from datetime import datetime
from dotenv import load_dotenv
from product_ai_enhancer import ProductProcessor

def load_and_preview_json(file_path):
    """Load JSON and show preview"""
    print(f"📁 Loading: {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Handle different JSON structures
    if isinstance(data, list):
        products = data
        print(f"✅ Direct array: {len(products)} products")
    elif isinstance(data, dict):
        if 'products' in data:
            products = data['products']
            print(f"✅ Products array: {len(products)} products")
        elif 'data' in data:
            products = data['data']
            print(f"✅ Data array: {len(products)} products")
        else:
            # Find any array
            for key, value in data.items():
                if isinstance(value, list) and len(value) > 0:
                    products = value
                    print(f"✅ Found {key} array: {len(products)} products")
                    break
            else:
                raise ValueError("No product array found")
    
    # Show first product structure
    if products:
        print(f"\n📋 First product preview:")
        first_product = products[0]
        if isinstance(first_product, dict):
            keys = list(first_product.keys())[:10]  # Show first 10 keys
            print(f"   Keys: {keys}")
            if len(first_product.keys()) > 10:
                print(f"   ... and {len(first_product.keys()) - 10} more")
        print(f"   Sample: {str(first_product)[:200]}...")
    
    return products

def test_json_file(file_path, store_type, max_products=10, delay=3):
    """Test JSON file with AI enhancement"""
    
    print(f"🚀 TESTING JSON FILE")
    print(f"=" * 50)
    print(f"File: {file_path}")
    print(f"Store: {store_type}")
    print(f"Max products: {max_products}")
    print(f"Delay: {delay}s between requests")
    
    # Load environment
    load_dotenv()
    
    # Load products
    products = load_and_preview_json(file_path)
    
    # Initialize processor
    processor = ProductProcessor(os.getenv("GEMINI_API_KEY"))
    
    # Test with limited number
    test_products = products[:max_products]
    print(f"\n🧪 Testing first {len(test_products)} products...")
    
    results = []
    failed = []
    
    for i, product in enumerate(test_products):
        print(f"\n📦 Product {i+1}/{len(test_products)}")
        
        try:
            # Get product name
            if store_type == "dm":
                name = product.get("name", "Unknown")
            elif store_type == "mercator":
                name = product.get("data", {}).get("name", "Unknown")
            elif store_type == "spar":
                name = product.get("masterValues", {}).get("name", "Unknown")
            elif store_type == "tus":
                name = product.get("name", "Unknown")
            elif store_type == "lidl":
                name = product.get("name", "Unknown")
            else:
                name = "Unknown"
            
            print(f"   Name: {name}")
            
            # Process with AI
            result = processor.process_product(product, store_type)
            
            if result:
                results.append(result)
                print(f"   ✅ Category: {result.get('ai_main_category')} → {result.get('ai_subcategory')}")
                print(f"   📊 Health: {result.get('ai_health_score')}")
                print(f"   💰 Value: {result.get('ai_value_rating')}")
                print(f"   📝 Summary: {result.get('ai_product_summary', '')[:60]}...")
            else:
                failed.append(name)
                print(f"   ❌ Failed to process")
            
            # Rate limiting
            if i < len(test_products) - 1:
                print(f"   ⏳ Waiting {delay}s...")
                time.sleep(delay)
                
        except Exception as e:
            failed.append(name)
            print(f"   💥 Error: {e}")
    
    # Results summary
    print(f"\n📊 TEST RESULTS")
    print(f"=" * 30)
    print(f"✅ Successful: {len(results)}")
    print(f"❌ Failed: {len(failed)}")
    print(f"📈 Success Rate: {(len(results)/len(test_products)*100):.1f}%")
    
    if failed:
        print(f"\n❌ Failed products:")
        for name in failed:
            print(f"   - {name}")
    
    # Save test results
    if results:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"test_results_{store_type}_{timestamp}.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                'test_info': {
                    'file': file_path,
                    'store_type': store_type,
                    'total_tested': len(test_products),
                    'successful': len(results),
                    'failed': len(failed),
                    'timestamp': timestamp
                },
                'results': results
            }, f, indent=2, ensure_ascii=False)
        
        print(f"\n💾 Test results saved to: {output_file}")
    
    return results

def process_full_file(file_path, store_type, batch_size=20, delay=2):
    """Process entire file in batches"""
    
    print(f"🔥 PROCESSING FULL FILE")
    print(f"=" * 50)
    
    # Load environment
    load_dotenv()
    
    # Load all products
    products = load_and_preview_json(file_path)
    total_products = len(products)
    
    print(f"\n⚡ Full processing mode:")
    print(f"   Total products: {total_products}")
    print(f"   Batch size: {batch_size}")
    print(f"   Estimated time: {(total_products * delay / 60):.1f} minutes")
    
    if input("\nContinue? (y/n): ").lower() != 'y':
        print("Cancelled.")
        return
    
    # Initialize processor
    processor = ProductProcessor(os.getenv("GEMINI_API_KEY"))
    
    all_results = []
    total_batches = (total_products + batch_size - 1) // batch_size
    
    start_time = time.time()
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, total_products)
        batch = products[start_idx:end_idx]
        
        print(f"\n🔄 Batch {batch_num + 1}/{total_batches} ({len(batch)} products)")
        
        batch_results = []
        for i, product in enumerate(batch):
            try:
                result = processor.process_product(product, store_type)
                if result:
                    batch_results.append(result)
                    if i % 5 == 0:  # Progress every 5 products
                        print(f"   ✅ {i+1}/{len(batch)} done")
                
                time.sleep(delay)
                
            except Exception as e:
                print(f"   ❌ Error on product {i+1}: {e}")
        
        all_results.extend(batch_results)
        
        # Progress update
        elapsed = time.time() - start_time
        processed = len(all_results)
        rate = processed / elapsed if elapsed > 0 else 0
        
        print(f"   📊 Batch {batch_num + 1} complete: {len(batch_results)}/{len(batch)} successful")
        print(f"   📈 Overall: {processed}/{total_products} ({rate:.1f}/sec)")
        
        # Save progress
        if batch_results:
            progress_file = f"progress_{store_type}_batch_{batch_num + 1}.json"
            with open(progress_file, 'w', encoding='utf-8') as f:
                json.dump(batch_results, f, indent=2, ensure_ascii=False)
    
    # Save final results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    final_file = f"enhanced_{store_type}_{timestamp}.json"
    
    with open(final_file, 'w', encoding='utf-8') as f:
        json.dump({
            'metadata': {
                'source_file': file_path,
                'store_type': store_type,
                'total_processed': len(all_results),
                'processing_time_minutes': (time.time() - start_time) / 60,
                'timestamp': timestamp
            },
            'products': all_results
        }, f, indent=2, ensure_ascii=False)
    
    print(f"\n🎉 PROCESSING COMPLETE!")
    print(f"   ✅ Total enhanced: {len(all_results)}/{total_products}")
    print(f"   ⏱️  Time: {(time.time() - start_time)/60:.1f} minutes")
    print(f"   💾 Saved to: {final_file}")

def main():
    """Interactive main function"""
    print("🚀 JSON FILE TESTER")
    print("=" * 40)
    
    # Get file path
    file_path = input("Enter JSON file path: ").strip().strip('"')
    
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return
    
    # Get store type
    print("\nStore types:")
    print("1. dm")
    print("2. mercator") 
    print("3. spar")
    print("4. tus")
    print("5. lidl")
    
    store_type = input("\nEnter store type: ").strip().lower()
    
    if store_type not in ['dm', 'mercator', 'spar', 'tus', 'lidl']:
        print(f"❌ Invalid store type: {store_type}")
        return
    
    # Choose mode
    print("\nProcessing mode:")
    print("1. Test (first 10 products)")
    print("2. Full processing")
    
    mode = input("Choose mode (1/2): ").strip()
    
    if mode == "1":
        max_products = int(input("How many products to test? (default 10): ") or "10")
        test_json_file(file_path, store_type, max_products)
    elif mode == "2":
        batch_size = int(input("Batch size? (default 20): ") or "20")
        process_full_file(file_path, store_type, batch_size)
    else:
        print("❌ Invalid mode")

if __name__ == "__main__":
    main()