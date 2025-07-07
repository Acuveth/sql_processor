#!/usr/bin/env python3
"""
Database Integration Example
Shows how to use the Product AI Enhancer and insert results into database
"""

import json
import mysql.connector
from mysql.connector import Error
from typing import Dict, List, Any
import logging
from datetime import datetime
from product_ai_enhancer import ProductProcessor, ProductNormalizer, ProductAIEnhancer

# Import the ProductProcessor from the previous script
# from product_ai_enhancer import ProductProcessor

logger = logging.getLogger(__name__)

class DatabaseInserter:
    """Handles database insertion of enhanced product data"""
    
    def __init__(self, host: str, database: str, user: str, password: str, port: int = 3306):
        """Initialize database connection"""
        self.connection_config = {
            'host': host,
            'database': database,
            'user': user,
            'password': password,
            'port': port,
            'charset': 'utf8mb4',
            'collation': 'utf8mb4_unicode_ci'
        }
        self.connection = None
        
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = mysql.connector.connect(**self.connection_config)
            if self.connection.is_connected():
                logger.info("Successfully connected to database")
                return True
        except Error as e:
            logger.error(f"Error connecting to database: {e}")
            return False
        return False
    
    def disconnect(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("Database connection closed")
    
    def insert_product(self, product_data: Dict[str, Any]) -> bool:
        """Insert enhanced product data into appropriate store table"""
        if not self.connection or not self.connection.is_connected():
            logger.error("No database connection")
            return False
        
        try:
            store_name = product_data.get('store_name', '').lower()
            table_name = f"{store_name}_products"
            
            # Prepare data for insertion
            insert_data = self._prepare_insert_data(product_data)
            
            # Build INSERT statement
            columns = list(insert_data.keys())
            placeholders = ', '.join(['%s'] * len(columns))
            column_names = ', '.join(columns)
            
            # Handle ON DUPLICATE KEY UPDATE
            update_clause = ', '.join([f"{col} = VALUES({col})" for col in columns if col != 'id'])
            
            query = f"""
                INSERT INTO {table_name} ({column_names})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {update_clause}
            """
            
            cursor = self.connection.cursor()
            cursor.execute(query, list(insert_data.values()))
            self.connection.commit()
            
            logger.info(f"Successfully inserted/updated product {product_data.get('product_id')} in {table_name}")
            return True
            
        except Error as e:
            logger.error(f"Error inserting product: {e}")
            if self.connection:
                self.connection.rollback()
            return False
        finally:
            if 'cursor' in locals():
                cursor.close()
    
    def _prepare_insert_data(self, product_data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare product data for database insertion"""
        # Convert lists/dicts to JSON strings
        json_fields = [
            'ai_recipe_compatibility', 'ai_key_selling_points', 'ai_diet_compatibility'
        ]
        
        prepared_data = {}
        
        for key, value in product_data.items():
            if key in json_fields and isinstance(value, (list, dict)):
                prepared_data[key] = json.dumps(value, ensure_ascii=False)
            elif value is None:
                prepared_data[key] = None
            elif isinstance(value, bool):
                prepared_data[key] = 1 if value else 0
            elif isinstance(value, (int, float, str)):
                prepared_data[key] = value
            else:
                prepared_data[key] = str(value)
        
        # Add timestamps
        now = datetime.now()
        prepared_data['updated_at'] = now
        if 'created_at' not in prepared_data:
            prepared_data['created_at'] = now
        
        return prepared_data
    
    def bulk_insert_products(self, products: List[Dict[str, Any]]) -> int:
        """Bulk insert multiple products"""
        success_count = 0
        
        for product in products:
            if self.insert_product(product):
                success_count += 1
        
        logger.info(f"Successfully inserted {success_count}/{len(products)} products")
        return success_count

class ProductPipeline:
    """Complete pipeline for processing and storing products"""
    
    def __init__(self, gemini_api_key: str, db_config: Dict[str, Any]):
        """Initialize pipeline with API key and database config"""
        self.processor = ProductProcessor(gemini_api_key)
        self.db_inserter = DatabaseInserter(**db_config)
        
    def process_and_store_products(self, products_json: List[Dict], store_type: str) -> Dict[str, int]:
        """Process products with AI and store in database"""
        stats = {
            'total': len(products_json),
            'processed': 0,
            'stored': 0,
            'failed': 0
        }
        
        # Connect to database
        if not self.db_inserter.connect():
            logger.error("Failed to connect to database")
            return stats
        
        try:
            # Process products with AI enhancement
            logger.info(f"Processing {len(products_json)} {store_type} products...")
            enhanced_products = self.processor.process_batch(products_json, store_type)
            stats['processed'] = len(enhanced_products)
            
            # Store in database
            logger.info(f"Storing {len(enhanced_products)} enhanced products...")
            stats['stored'] = self.db_inserter.bulk_insert_products(enhanced_products)
            stats['failed'] = stats['total'] - stats['stored']
            
        finally:
            self.db_inserter.disconnect()
        
        return stats

# Example usage and test functions
def test_dm_products():
    """Test with DM products"""
    dm_products = [
        {
            "gtin": 4058172925122,
            "dan": 1461618,
            "name": "Bio mleta bourbonska vanilja",
            "brandName": "dmBio",
            "title": "Bio mleta bourbonska vanilja, 5 g",
            "price": {"value": 4.95, "currencyIso": "EUR"},
            "categoryNames": ["Sestavine za peko"],
            "ratingValue": 4.7737,
            "ratingCount": 190
        }
    ]
    
    return dm_products, "dm"

def test_mercator_products():
    """Test with Mercator products"""
    mercator_products = [
        {
            "data": {
                "cinv": "17931243",
                "code": "00366029",
                "name": "Kisla smetana, Mercator, 20 % m.m., 400 g",
                "current_price": "1.19",
                "brand_name": "MERCATOR",
                "category1": "MLEKO, JAJCA IN MLEČNI IZDELKI",
                "category2": "SMETANE",
                "category3": "KISLA SMETANA",
                "gtins": [{"gtin": "3838900940273"}],
                "allergens": [
                    {"value": "73_true", "hover_text": "Vsebuje mleko"}
                ]
            }
        }
    ]
    
    return mercator_products, "mercator"

def test_spar_products():
    """Test with Spar products"""
    spar_products = [
        {
            "masterValues": {
                "title": "PUDING VANILIJA S SMETANO SPAR, 200G",
                "name": "SPAR PUDING VANILIJA 200G",
                "ecr-brand": "Spar",
                "price": 0.37,
                "regular-price": 0.37,
                "category-names": "Pudingi|VSI IZDELKI|Jogurti, deserti in pudingi|HLAJENI IN MLEČNI IZDELKI",
                "is-on-promotion": "false"
            },
            "id": "292934"
        }
    ]
    
    return spar_products, "spar"

def test_tus_products():
    """Test with TUS products"""
    tus_products = [
        {
            "ean": "4005401164135",
            "name": "Barvice Faber Castell, Black Edition, kovina, 12/1",
            "current_price_numeric": 8.99,
            "regular_price_numeric": 9.99,
            "discount_percentage": 10.01,
            "id": "360200",
            "has_discount": True
        }
    ]
    
    return tus_products, "tus"

def main():
    """Main example showing complete pipeline usage"""
    import os
    
    # Configuration
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
    DB_CONFIG = {
        'host': os.getenv("DB_HOST", "localhost"),
        'database': os.getenv("DB_NAME", "store_products"),
        'user': os.getenv("DB_USER", "root"),
        'password': os.getenv("DB_PASSWORD", ""),
        'port': int(os.getenv("DB_PORT", 3306))
    }
    
    if not GEMINI_API_KEY:
        print("Please set GEMINI_API_KEY environment variable")
        return
    
    # Initialize pipeline
    pipeline = ProductPipeline(GEMINI_API_KEY, DB_CONFIG)
    
    # Test with different store types
    test_cases = [
        test_dm_products(),
        test_mercator_products(),
        test_spar_products(),
        test_tus_products()
    ]
    
    total_stats = {'total': 0, 'processed': 0, 'stored': 0, 'failed': 0}
    
    for products, store_type in test_cases:
        print(f"\n--- Processing {store_type.upper()} products ---")
        
        stats = pipeline.process_and_store_products(products, store_type)
        
        print(f"Results for {store_type}:")
        print(f"  Total: {stats['total']}")
        print(f"  Processed: {stats['processed']}")
        print(f"  Stored: {stats['stored']}")
        print(f"  Failed: {stats['failed']}")
        
        # Update total stats
        for key in total_stats:
            total_stats[key] += stats[key]
    
    print(f"\n--- OVERALL RESULTS ---")
    print(f"Total products: {total_stats['total']}")
    print(f"Successfully processed: {total_stats['processed']}")
    print(f"Successfully stored: {total_stats['stored']}")
    print(f"Failed: {total_stats['failed']}")
    print(f"Success rate: {(total_stats['stored']/total_stats['total']*100):.1f}%")

# Batch processing function for large datasets
def process_large_dataset(json_file_path: str, store_type: str, batch_size: int = 50):
    """Process large JSON dataset in batches"""
    import os
    
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
    DB_CONFIG = {
        'host': os.getenv("DB_HOST", "localhost"),
        'database': os.getenv("DB_NAME", "store_products"),
        'user': os.getenv("DB_USER", "root"),
        'password': os.getenv("DB_PASSWORD", ""),
        'port': int(os.getenv("DB_PORT", 3306))
    }
    
    if not GEMINI_API_KEY:
        print("Please set GEMINI_API_KEY environment variable")
        return
    
    # Load JSON data
    with open(json_file_path, 'r', encoding='utf-8') as f:
        products_data = json.load(f)
    
    # Handle different JSON structures
    if isinstance(products_data, dict) and 'products' in products_data:
        products = products_data['products']
    elif isinstance(products_data, list):
        products = products_data
    else:
        print("Unknown JSON structure")
        return
    
    # Initialize pipeline
    pipeline = ProductPipeline(GEMINI_API_KEY, DB_CONFIG)
    
    # Process in batches
    total_products = len(products)
    total_processed = 0
    total_stored = 0
    
    for i in range(0, total_products, batch_size):
        batch = products[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (total_products // batch_size) + 1
        
        print(f"\nProcessing batch {batch_num}/{total_batches} ({len(batch)} products)...")
        
        stats = pipeline.process_and_store_products(batch, store_type)
        total_processed += stats['processed']
        total_stored += stats['stored']
        
        print(f"Batch {batch_num} results: {stats['stored']}/{stats['total']} stored")
    
    print(f"\n--- FINAL RESULTS ---")
    print(f"Total products: {total_products}")
    print(f"Total processed: {total_processed}")
    print(f"Total stored: {total_stored}")
    print(f"Success rate: {(total_stored/total_products*100):.1f}%")

if __name__ == "__main__":
    # Run basic tests
    main()
    
    # Uncomment to process large dataset:
    # process_large_dataset("mercator_products.json", "mercator", batch_size=25)