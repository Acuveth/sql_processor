#!/usr/bin/env python3
"""
Real Product Data Tester
Tests AI enhancement with actual product data from all stores
"""

import os
import json
import time
from dotenv import load_dotenv
from product_ai_enhancer import ProductProcessor

def test_real_dm_products():
    """Test with real DM products"""
    print("🏪 Testing DM Products")
    print("=" * 50)
    
    dm_products = [
        {
            "gtin": 4058172925122,
            "dan": 1461618,
            "name": "Bio mleta bourbonska vanilja",
            "brandName": "dmBio",
            "title": "Bio mleta bourbonska vanilja, 5 g",
            "isoCountry": "SI",
            "isoLanguage": "sl",
            "purchasable": True,
            "notAvailable": False,
            "relativeProductUrl": "/dmbio-bio-mleta-bourbonska-vanilja-p4058172925122.html",
            "imageUrlTemplates": [
                "https://media.dm-static.com/images/{transformations}/v1744766367/products/pim/4058172925122-2907452/dmbio-bio-mleta-bourbonska-vanilja"
            ],
            "price": {
                "formattedValue": "4,95 €",
                "value": 4.95,
                "currencySymbol": "€",
                "currencyIso": "EUR"
            },
            "basePrice": {
                "formattedValue": "99,00 €"
            },
            "basePriceUnit": "g",
            "basePriceQuantity": 100.0,
            "basePriceRelNetQuantity": 5.0,
            "contentUnit": "g",
            "netQuantityContent": 5.0,
            "ratingValue": 4.7737,
            "ratingCount": 190,
            "categoryNames": [
                "Sestavine za peko"
            ]
        },
        {
            "gtin": 4337185584008,
            "dan": 1234567,
            "name": "Ekološki med",
            "brandName": "dmBio",
            "title": "Ekološki med cvetličen, 500 g",
            "price": {
                "value": 7.99,
                "currencyIso": "EUR"
            },
            "ratingValue": 4.5,
            "ratingCount": 85,
            "categoryNames": [
                "Sladila in džemi"
            ]
        }
    ]
    
    return dm_products, "dm"

def test_real_mercator_products():
    """Test with real Mercator products"""
    print("🏪 Testing Mercator Products")
    print("=" * 50)
    
    mercator_products = [
        {
            "data": {
                "cinv": "17931243",
                "code": "00366029",
                "codewz": "366029",
                "name": "Kisla smetana, Mercator, 20 % m.m., 400 g",
                "unit_quantity": "400",
                "invoice_unit": "Kos",
                "invoice_unit_type": "1",
                "average_weight": 0,
                "normal_price": 0,
                "current_price": "1.19",
                "pc30_price": "0.00",
                "price_per_unit": 2.9749999999999996,
                "price_per_unit_base": "1kg",
                "eko": "0",
                "has_recipes": "1",
                "brand_name": "MERCATOR",
                "gtins": [
                    {
                        "gtin": "3838900940273"
                    }
                ],
                "allergens": [
                    {
                        "value": "70_false",
                        "hover_text": "Ne vsebuje jajc"
                    },
                    {
                        "value": "73_true",
                        "hover_text": "Vsebuje mleko"
                    }
                ],
                "discounts": [],
                "ratings_sum": "9",
                "ratings_num": "3",
                "rating": 3,
                "package": 0,
                "offer_expires_on": "1970-01-01",
                "category1": "MLEKO, JAJCA IN MLEČNI IZDELKI",
                "category2": "SMETANE",
                "category3": "KISLA SMETANA"
            },
            "itemId": "17931243",
            "mainImageSrc": "https://mercatoronline.si/img/cache/products/4169/product_small_image/00366029.jpg",
            "url": "/izdelek/17931243/kisla-smetana-mercator-20-m-m-400-g",
            "short_name": "Kisla smetana, Mercator, 20 % m.m., 400 g"
        },
        {
            "data": {
                "cinv": "17845632",
                "code": "00123456",
                "name": "Kruh črni, Mercator, 500 g",
                "current_price": "1.89",
                "brand_name": "MERCATOR",
                "category1": "KRUH IN PEKOVSKI IZDELKI",
                "category2": "KRUH",
                "category3": "ČRNI KRUH",
                "gtins": [{"gtin": "3838900123456"}],
                "rating": 4,
                "ratings_num": "12"
            }
        }
    ]
    
    return mercator_products, "mercator"

def test_real_spar_products():
    """Test with real Spar products"""
    print("🏪 Testing Spar Products")
    print("=" * 50)
    
    spar_products = [
        {
            "masterValues": {
                "is-on-promotion": "false",
                "category-names": "Pudingi|VSI IZDELKI|Jogurti, deserti in pudingi|HLAJENI IN MLEČNI IZDELKI",
                "badge-icon": "/online/medias/sys_master/images/images/h68/h25/10524621799454/badge-cold-FIN.jpg",
                "description": "SPAR PUDING VANILI 200",
                "sales-unit": "kos",
                "title": "PUDING VANILIJA S SMETANO SPAR, 200G",
                "badge-names": "Hlajeno",
                "item-type": "Product",
                "code-internal": "8801145454593",
                "category-name": "Pudingi",
                "price": 0.37,
                "badge-short-name": "Hlajeno",
                "created-at": "1499473944603",
                "categories": [
                    "root_VSI IZDELKI:food_null",
                    "S2-4_Jogurti, deserti in pudingi:S2-4-4_Pudingi",
                    "S2_HLAJENI IN MLEČNI IZDELKI:S2-4_Jogurti, deserti in pudingi",
                    "food_null:S2_HLAJENI IN MLEČNI IZDELKI"
                ],
                "best-price": 0.37,
                "short-description-2": "/",
                "ecr-category-number": "110401010301",
                "stock-status": "inStock",
                "is-new": "false",
                "image-url": "https://cdn1.interspar.at/cachableservlets/articleImage.dam/si/292934/dt_main.jpg",
                "short-description": "/",
                "approx-weight-product": "false",
                "url": "/puding-vanilija-s-smetano-spar-200g/p/292934",
                "ecr-brand": "Spar",
                "name": "SPAR PUDING VANILIJA 200G",
                "product-number": "292934",
                "price-per-unit": "1,85 €/kg",
                "regular-price": 0.37,
                "price-per-unit-number": 1.85
            },
            "variantValues": [],
            "id": "292934",
            "score": 107,
            "position": 1,
            "foundWords": []
        },
        {
            "masterValues": {
                "is-on-promotion": "true",
                "category-names": "Zdravje|OSEBNA NEGA|Zaščita pred insekti|VSI IZDELKI",
                "title": "REPELENT PROTI KOMARJEM, SPAR, 100ML",
                "price": 4.49,
                "best-price": 3.49,
                "promotion-text": "SPAR PLUS POPUST",
                "ecr-brand": "Spar",
                "name": "SPAR REPELENT 100ML PROTI",
                "regular-price": 4.49
            },
            "id": "389994"
        }
    ]
    
    return spar_products, "spar"

def test_real_tus_products():
    """Test with real TUS products"""
    print("🏪 Testing TUS Products")
    print("=" * 50)
    
    tus_products = [
        {
            "index": 0,
            "ean": "4005401164135",
            "name": "Barvice Faber Castell, Black Edition, kovina, 12/1",
            "url": "https://www.tus.si/izdelki/barvice-faber-castell-black-edition-kovina-12-1/",
            "image_url": "https://www.tus.si/app/images/4005401164135.jpg",
            "current_price": "8,99 €",
            "current_price_numeric": 8.99,
            "regular_price": "9,99 €",
            "regular_price_numeric": 9.99,
            "discount_percentage": 10.01,
            "id": "360200",
            "sku": "4005401164135",
            "has_discount": True,
            "page_number": 1
        },
        {
            "index": 1,
            "ean": "4005401164142",
            "name": "Barvice Faber Castell, Black Edition, kožni toni, 12/1",
            "url": "https://www.tus.si/izdelki/barvice-faber-castell-black-edition-kozni-toni-12-1/",
            "image_url": "https://www.tus.si/app/images/4005401164142.jpg",
            "current_price": "6,29 €",
            "current_price_numeric": 6.29,
            "regular_price": "6,99 €",
            "regular_price_numeric": 6.99,
            "discount_percentage": 10.01,
            "id": "360269",
            "sku": "4005401164142",
            "has_discount": True,
            "page_number": 1
        }
    ]
    
    return tus_products, "tus"

def test_real_lidl_products():
    """Test with real Lidl products - MISSING FUNCTION"""
    print("🏪 Testing Lidl Products")
    print("=" * 50)
    
    lidl_products = [
        {
            "code": "10082461",
            "name": "Bučke",
            "url": "",
            "result_class": "product",
            "type": "product",
            "product_id": 10082461,
            "erp_number": "10082461",
            "item_id": 10082461,
            "full_title": "Bučke",
            "title": "Bučke",
            "category": "Food",
            "product_type": "RETAIL",
            "age_restriction": False,
            "price": 0.69,
            "old_price": 1.49,
            "currency_code": "EUR",
            "currency_symbol": "€",
            "discount_percentage": 53,
            "packaging_text": "za kg",
            "main_image": "https://imgproxy-retcat.assets.schwarz/QZAX6-P12liEncYyVjT7lWddFbirVb8slWyxlKE5nPo/sm:1/w:427/h:320/cz/M6Ly9wcm9kLWNhd/GFsb2ctbWVkaWEvc2kvMS9ENzU3QzgwM0QyQjY5QzU3QTYzOTY2Q0V/FMkVBRDQ4OTE1RDlDRDRFREE2RDA4RUI0RTM4MjZCM0JBMERBNDg0LmpwZw.jpg",
            "image_accessibility": "Košara s svežimi zelenimi bučkami.",
            "analytics_category": "Food",
            "more_details": "Dodatne podrobnosti o izdelku",
            "won_category_primary": "Svetovi potreb/Hrana in bližnja hrana/Sadje in zelenjava",
            "canonical_path": "/p/bucke/p10082461",
            "canonical_url": "/p/bucke/p10082461",
            "ians": ["82345"],
            "category_breadcrumbs": [
                {
                    "id": "10068374",
                    "name": "Hrana in pijača",
                    "url": "/c/hrana-in-pijaca/s10068374",
                    "hidden": False
                },
                {
                    "id": "10071012",
                    "name": "Sadje in zelenjava",
                    "url": "/h/sadje-in-zelenjava/h10071012",
                    "hidden": False
                }
            ],
            "world_of_needs_code": "1710",
            "world_of_needs_name": "Sadje in zelenjava",
            "badge_texts": ["V trgovini od 10.07. do 12.07."],
            "badge_types": ["IN_STORE_FROM_FUTURE_DATE_RANGE"]
        },
        {
            "code": "10055678",
            "name": "Kruh polnozrnati",
            "product_id": 10055678,
            "erp_number": "10055678",
            "item_id": 10055678,
            "full_title": "Kruh polnozrnati, 500g",
            "title": "Kruh polnozrnati",
            "price": 1.29,
            "old_price": 0,
            "currency_code": "EUR",
            "currency_symbol": "€",
            "discount_percentage": 0,
            "packaging_text": "za kos",
            "main_image": "https://example-lidl-image.jpg",
            "world_of_needs_name": "Kruh in pekovski izdelki",
            "category_breadcrumbs": [
                {
                    "name": "Hrana in pijača"
                },
                {
                    "name": "Kruh in pekovski izdelki"
                }
            ],
            "canonical_url": "/p/kruh-polnozrnati/p10055678"
        }
    ]
    
    return lidl_products, "lidl"

def process_store_products(products, store_type, processor):
    """Process products for a specific store"""
    results = []
    
    print(f"\n🔄 Processing {len(products)} {store_type.upper()} products...")
    
    for i, product in enumerate(products):
        print(f"\n📦 Product {i+1}/{len(products)}: ", end="")
        
        if store_type == "dm":
            product_name = product.get("name", "Unknown")
        elif store_type == "mercator":
            product_name = product.get("data", {}).get("name", "Unknown")
        elif store_type == "spar":
            product_name = product.get("masterValues", {}).get("name", "Unknown")
        elif store_type == "tus":
            product_name = product.get("name", "Unknown")
        elif store_type == "lidl": 
            product_name = product.get("name", "Unknown")
        
        print(f"{product_name}")
        
        # Process with AI
        result = processor.process_product(product, store_type)
        
        if result:
            print(f"   ✅ Category: {result.get('ai_main_category')}")
            print(f"   📊 Health Score: {result.get('ai_health_score')}")
            print(f"   💰 Value Rating: {result.get('ai_value_rating')}")
            print(f"   📝 Summary: {result.get('ai_product_summary', '')[:100]}...")
            
            results.append(result)
        else:
            print("   ❌ Processing failed")
        
        # Small delay to be respectful to API
        time.sleep(1)
    
    return results

def save_results(all_results, filename="real_products_enhanced.json"):
    """Save all results to JSON file"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 Results saved to {filename}")

def analyze_results(all_results):
    """Analyze the enhancement results"""
    print("\n📊 ENHANCEMENT ANALYSIS")
    print("=" * 50)
    
    total_products = len(all_results)
    
    # Category distribution
    categories = {}
    health_scores = []
    value_ratings = {}
    
    for result in all_results:
        # Count categories
        category = result.get('ai_main_category', 'Unknown')
        categories[category] = categories.get(category, 0) + 1
        
        # Collect health scores
        health_score = result.get('ai_health_score')
        if health_score:
            health_scores.append(health_score)
        
        # Count value ratings
        value_rating = result.get('ai_value_rating', 'Unknown')
        value_ratings[value_rating] = value_ratings.get(value_rating, 0) + 1
    
    print(f"📈 Total Products Processed: {total_products}")
    
    print(f"\n🏷️  Category Distribution:")
    for category, count in sorted(categories.items()):
        percentage = (count / total_products) * 100
        print(f"   {category}: {count} ({percentage:.1f}%)")
    
    if health_scores:
        avg_health = sum(health_scores) / len(health_scores)
        print(f"\n🏥 Health Score Analysis:")
        print(f"   Average Health Score: {avg_health:.1f}")
        print(f"   Highest: {max(health_scores)}")
        print(f"   Lowest: {min(health_scores)}")
    
    print(f"\n💎 Value Rating Distribution:")
    for rating, count in sorted(value_ratings.items()):
        percentage = (count / total_products) * 100
        print(f"   {rating}: {count} ({percentage:.1f}%)")

def main():
    """Main function to test all store types with real data"""
    print("🚀 REAL PRODUCT DATA TESTING")
    print("=" * 60)
    print("Testing AI enhancement with actual product data from all stores")
    print("Now with your upgraded API plan! 🎉")
    
    # Load environment
    load_dotenv()
    
    # Initialize processor
    processor = ProductProcessor(os.getenv("GEMINI_API_KEY"))
    
    # Get test data for all stores
    test_cases = [
        #test_real_dm_products(),
        #test_real_mercator_products(),
        #test_real_spar_products(),
        #test_real_tus_products(),
        test_real_lidl_products()
    ]
    
    all_results = []
    
    # Process each store type
    for products, store_type in test_cases:
        results = process_store_products(products, store_type, processor)
        all_results.extend(results)
        
        print(f"\n✅ {store_type.upper()} completed: {len(results)} products enhanced")
    
    # Save and analyze results
    save_results(all_results)
    analyze_results(all_results)
    
    print("\n🎉 TESTING COMPLETE!")
    print("=" * 30)
    print(f"📦 Total products processed: {len(all_results)}")
    print(f"💾 Results saved to: real_products_enhanced.json")
    print(f"🔍 Review the file to see detailed AI analysis")
    
    # Show sample result
    if all_results:
        print(f"\n📋 SAMPLE ENHANCED PRODUCT:")
        print("-" * 40)
        sample = all_results[0]
        print(f"Product: {sample.get('product_name')}")
        print(f"Store: {sample.get('store_name')}")
        print(f"Category: {sample.get('ai_main_category')}")
        print(f"Health Score: {sample.get('ai_health_score')}")
        print(f"Summary: {sample.get('ai_product_summary', '')[:150]}...")

if __name__ == "__main__":
    main()