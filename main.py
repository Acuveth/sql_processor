#!/usr/bin/env python3
"""
Simple Starter Script - main.py
Shows exactly how to use the product AI enhancer
"""

import os
import json
from dotenv import load_dotenv

# Import from our product_ai_enhancer file
from product_ai_enhancer import ProductProcessor

def setup_environment():
    """Load environment variables and check setup"""
    load_dotenv()
    
    # Check required environment variables
    api_key = os.getenv('GEMINI_API_KEY')
    if not api_key:
        print("❌ GEMINI_API_KEY not found in environment variables")
        print("Please create a .env file with your API key")
        return False
    
    print("✅ Environment variables loaded successfully")
    return True

def test_ai_enhancement():
    """Test AI enhancement only (without database)"""
    print("\n--- Testing AI Enhancement ---")
    
    try:
        # Initialize AI processor
        processor = ProductProcessor(os.getenv("GEMINI_API_KEY"))
        
        # Test product
        test_product = {
            "gtin": 4058172925122,
            "dan": 1461618,
            "name": "Bio mleta bourbonska vanilja",
            "brandName": "dmBio",
            "title": "Bio mleta bourbonska vanilja, 5 g",
            "price": {"value": 4.95, "currencyIso": "EUR"},
            "categoryNames": ["Sestavine za peko"]
        }
        
        # Process with AI
        enhanced = processor.process_product(test_product, "dm")
        
        if enhanced:
            print("✅ AI Enhancement successful!")
            print(f"   Category: {enhanced.get('ai_main_category')}")
            print(f"   Health Score: {enhanced.get('ai_health_score')}")
            print(f"   Value Rating: {enhanced.get('ai_value_rating')}")
            print(f"   Summary: {enhanced.get('ai_product_summary')}")
            return enhanced
        else:
            print("❌ AI Enhancement failed")
            return None
            
    except Exception as e:
        print(f"❌ AI Enhancement error: {e}")
        return None

def process_your_product():
    """Example of how to process your own product"""
    print("\n--- Process Your Own Product ---")
    
    # Example: Replace this with your actual product JSON
    your_product = {
        "gtin": 1234567890,
        "dan": 98765,
        "name": "Your Product Name Here",
        "brandName": "Your Brand",
        "title": "Your Product Title Here",
        "price": {"value": 2.99, "currencyIso": "EUR"},
        "categoryNames": ["Your Category"]
    }
    
    try:
        processor = ProductProcessor(os.getenv("GEMINI_API_KEY"))
        result = processor.process_product(your_product, "dm")
        
        if result:
            print("✅ Your product processed successfully!")
            print(f"   Product: {result.get('product_name')}")
            print(f"   Category: {result.get('ai_main_category')}")
            print(f"   Health Score: {result.get('ai_health_score')}")
            print(f"   Usage: {result.get('ai_usage_suggestions')}")
            
            # Save result to file
            with open('enhanced_product.json', 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            print("   ✅ Result saved to 'enhanced_product.json'")
            
        else:
            print("❌ Failed to process your product")
            
    except Exception as e:
        print(f"❌ Error processing your product: {e}")

def main():
    """Main function - runs all tests and examples"""
    print("🚀 Product AI Enhancement System - Starter Script")
    print("=" * 60)
    
    # 1. Setup environment
    if not setup_environment():
        return
    
    # 2. Test AI enhancement
    enhanced_product = test_ai_enhancement()
    
    if enhanced_product:
        print("\n🎉 AI Enhancement is working!")
        
        # 3. Show how to process your own products
        process_your_product()
        
        print("\n✅ System is ready to use!")
        print("\nNext steps:")
        print("1. Replace the sample product in process_your_product() with your actual data")
        print("2. Run this script again to process your products")
        print("3. Check 'enhanced_product.json' for the results")
        
    else:
        print("\n❌ AI Enhancement failed. Please check your setup.")

if __name__ == "__main__":
    main()