#!/usr/bin/env python3
"""
Product AI Enhancement Script
Processes product JSON data from different stores and enhances with AI-generated intelligence
"""

import json
import logging
import re
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime
import google.generativeai as genai

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class ProductData:
    """Normalized product data structure"""
    # Core identification
    product_id: str
    product_code: Optional[str] = None
    ean_code: Optional[str] = None
    erp_number: Optional[str] = None
    
    # Basic info
    product_name: str = ""
    product_title: str = ""
    brand_name: str = ""
    product_description: str = ""
    
    # Pricing
    current_price: Optional[float] = None
    regular_price: Optional[float] = None
    discount_percentage: Optional[float] = None
    has_discount: bool = False
    currency_code: str = "EUR"
    
    # Categories
    store_category: Optional[str] = None
    category_level_1: Optional[str] = None
    category_level_2: Optional[str] = None
    category_level_3: Optional[str] = None
    
    # URLs and media
    product_url: Optional[str] = None
    image_url: Optional[str] = None
    
    # Ratings
    rating_value: Optional[float] = None
    rating_count: Optional[int] = None
    
    # Store specific
    store_name: str = ""
    
    # Raw allergen data for AI processing
    allergens_data: Optional[Dict] = None
    categories_data: Optional[List] = None

class ProductNormalizer:
    """Normalizes product data from different store formats"""
    
    @staticmethod
    def normalize_lidl_product(data: Dict, store_name: str = "lidl") -> ProductData:
        """Normalize Lidl product JSON - FIXED VERSION"""
        try:
            # Extract basic product info
            product_id = str(data.get("product_id", data.get("item_id", "")))
            
            # FIXED: Handle None values properly
            price_raw = data.get("price")
            old_price_raw = data.get("old_price")
            
            # Convert to float, handling None values
            current_price = float(price_raw) if price_raw is not None else 0.0
            old_price = float(old_price_raw) if old_price_raw is not None else 0.0
            
            # Calculate discount - FIXED: Now safe from None comparisons
            discount_percentage = 0.0
            has_discount = False
            regular_price = None
            
            if old_price > 0 and current_price > 0 and current_price < old_price:
                discount_percentage = ((old_price - current_price) / old_price) * 100
                has_discount = True
                regular_price = old_price
            elif data.get("discount_percentage"):
                discount_percentage_raw = data.get("discount_percentage")
                if discount_percentage_raw is not None:
                    discount_percentage = float(discount_percentage_raw)
                    has_discount = True
                    if current_price > 0 and discount_percentage > 0:
                        regular_price = current_price / (1 - discount_percentage / 100)
            
            # Extract categories from breadcrumbs and world_of_needs
            category_breadcrumbs = data.get("category_breadcrumbs", [])
            categories_data = []
            category_level_1 = ""
            category_level_2 = ""
            
            # FIXED: Handle None breadcrumbs
            if category_breadcrumbs and isinstance(category_breadcrumbs, list):
                for i, breadcrumb in enumerate(category_breadcrumbs):
                    if breadcrumb and isinstance(breadcrumb, dict):
                        breadcrumb_name = breadcrumb.get("name", "")
                        if breadcrumb_name:
                            if i == 0:
                                category_level_1 = breadcrumb_name
                            elif i == 1:
                                category_level_2 = breadcrumb_name
                            categories_data.append(breadcrumb_name)
            
            # Add world_of_needs_name as primary category
            won_name = data.get("world_of_needs_name", "")
            if won_name:
                categories_data.insert(0, won_name)
                if not category_level_1:
                    category_level_1 = won_name
            
            # Extract image URL - FIXED: Handle None values
            main_image = data.get("main_image", "")
            image_list = data.get("image_list", [])
            image_url = ""
            
            if main_image:
                image_url = main_image
            elif image_list and isinstance(image_list, list) and len(image_list) > 0:
                image_url = image_list[0] if image_list[0] else ""
            
            # FIXED: Handle None values in all fields
            product_name = data.get("name", "")
            if product_name is None:
                product_name = ""
                
            product_title = data.get("full_title", data.get("title", ""))
            if product_title is None:
                product_title = ""
                
            product_description = data.get("more_details", "")
            if product_description is None:
                product_description = ""
                
            product_code = data.get("code", "")
            if product_code is None:
                product_code = ""
                
            # Handle EAN codes
            ians = data.get("ians", [])
            ean_code = ""
            if ians and isinstance(ians, list) and len(ians) > 0:
                ean_code = str(ians[0]) if ians[0] is not None else ""
            
            # Handle URLs
            product_url = data.get("canonical_url", data.get("url", ""))
            if product_url is None:
                product_url = ""
                
            # Handle ERP number
            erp_number = data.get("erp_number", "")
            if erp_number is None:
                erp_number = ""
                
            # Handle currency
            currency_code = data.get("currency_code", "EUR")
            if currency_code is None:
                currency_code = "EUR"
            
            # Create normalized product
            return ProductData(
                product_id=product_id,
                product_code=product_code,
                ean_code=ean_code,
                erp_number=erp_number,
                product_name=product_name,
                product_title=product_title,
                brand_name="",  # Lidl products often don't have separate brand
                product_description=product_description,
                current_price=current_price,
                regular_price=regular_price,
                discount_percentage=discount_percentage if discount_percentage > 0 else None,
                has_discount=has_discount,
                currency_code=currency_code,
                product_url=product_url,
                image_url=image_url,
                store_category=won_name,
                category_level_1=category_level_1,
                category_level_2=category_level_2,
                category_level_3="",
                rating_value=None,  # Lidl doesn't seem to have ratings in this format
                rating_count=None,
                store_name=store_name,
                categories_data=categories_data
            )
            
        except Exception as e:
            logger.error(f"Error normalizing Lidl product: {e}")
            logger.error(f"Product data: {data}")
            return None

    @staticmethod
    def normalize_dm_product(data: Dict, store_name: str = "dm") -> ProductData:
        """Normalize DM product JSON"""
        try:
            price_info = data.get("price", {})
            current_price = price_info.get("value", 0.0) if price_info else 0.0
            
            return ProductData(
                product_id=str(data.get("dan", "")),
                ean_code=str(data.get("gtin", "")),
                product_name=data.get("name", ""),
                product_title=data.get("title", ""),
                brand_name=data.get("brandName", ""),
                current_price=current_price,
                currency_code=price_info.get("currencyIso", "EUR") if price_info else "EUR",
                product_url=data.get("relativeProductUrl", ""),
                image_url=data.get("imageUrlTemplates", [""])[0] if data.get("imageUrlTemplates") else "",
                rating_value=data.get("ratingValue", 0.0),
                rating_count=data.get("ratingCount", 0),
                store_category=", ".join(data.get("categoryNames", [])),
                store_name=store_name,
                categories_data=data.get("categoryNames", [])
            )
        except Exception as e:
            logger.error(f"Error normalizing DM product: {e}")
            return None

    @staticmethod
    def normalize_mercator_product(data: Dict, store_name: str = "mercator") -> ProductData:
        """Normalize Mercator product JSON"""
        try:
            product_data = data.get("data", {})
            
            # Extract EAN from gtins
            ean_code = ""
            gtins = product_data.get("gtins", [])
            if gtins and len(gtins) > 0:
                ean_code = gtins[0].get("gtin", "")
            
            # Calculate discount
            current_price = float(product_data.get("current_price", 0))
            normal_price = float(product_data.get("normal_price", 0))
            discount_percentage = 0.0
            if normal_price > 0 and current_price < normal_price:
                discount_percentage = ((normal_price - current_price) / normal_price) * 100
            
            # Process allergens
            allergens = product_data.get("allergens", [])
            allergen_info = {}
            for allergen in allergens:
                value_parts = allergen.get("value", "").split("_")
                if len(value_parts) == 2:
                    allergen_info[value_parts[0]] = value_parts[1] == "true"
            
            return ProductData(
                product_id=product_data.get("cinv", ""),
                product_code=product_data.get("code", ""),
                ean_code=ean_code,
                product_name=product_data.get("name", ""),
                product_title=product_data.get("name", ""),
                brand_name=product_data.get("brand_name", ""),
                current_price=current_price,
                regular_price=normal_price if normal_price > 0 else None,
                discount_percentage=discount_percentage if discount_percentage > 0 else None,
                has_discount=discount_percentage > 0,
                category_level_1=product_data.get("category1", ""),
                category_level_2=product_data.get("category2", ""),
                category_level_3=product_data.get("category3", ""),
                product_url=data.get("url", ""),
                image_url=data.get("mainImageSrc", ""),
                rating_value=float(product_data.get("rating", 0)),
                rating_count=int(product_data.get("ratings_num", 0)),
                store_name=store_name,
                allergens_data=allergen_info,
                categories_data=[product_data.get("category1", ""), product_data.get("category2", ""), product_data.get("category3", "")]
            )
        except Exception as e:
            logger.error(f"Error normalizing Mercator product: {e}")
            return None

    @staticmethod
    def normalize_spar_product(data: Dict, store_name: str = "spar") -> ProductData:
        """Normalize Spar product JSON"""
        try:
            master_values = data.get("masterValues", {})
            
            # Handle promotion pricing
            current_price = float(master_values.get("price", 0))
            best_price = float(master_values.get("best-price", 0))
            regular_price = float(master_values.get("regular-price", 0))
            
            # Calculate discount
            discount_percentage = 0.0
            has_discount = master_values.get("is-on-promotion", "false") == "true"
            if has_discount and regular_price > 0 and best_price < regular_price:
                discount_percentage = ((regular_price - best_price) / regular_price) * 100
                current_price = best_price
            
            # Process categories
            category_names = master_values.get("category-names", "").split("|")
            categories_clean = [cat.strip() for cat in category_names if cat.strip()]
            
            return ProductData(
                product_id=data.get("id", ""),
                product_code=master_values.get("code-internal", ""),
                product_name=master_values.get("name", ""),
                product_title=master_values.get("title", ""),
                brand_name=master_values.get("ecr-brand", ""),
                product_description=master_values.get("description", ""),
                current_price=current_price,
                regular_price=regular_price if regular_price > 0 else None,
                discount_percentage=discount_percentage if discount_percentage > 0 else None,
                has_discount=has_discount,
                product_url=master_values.get("url", ""),
                image_url=master_values.get("image-url", ""),
                store_category=master_values.get("category-name", ""),
                category_level_1=categories_clean[0] if len(categories_clean) > 0 else "",
                category_level_2=categories_clean[1] if len(categories_clean) > 1 else "",
                category_level_3=categories_clean[2] if len(categories_clean) > 2 else "",
                store_name=store_name,
                categories_data=categories_clean
            )
        except Exception as e:
            logger.error(f"Error normalizing Spar product: {e}")
            return None

    @staticmethod
    def normalize_tus_product(data: Dict, store_name: str = "tus") -> ProductData:
        """Normalize TUS product JSON"""
        try:
            current_price = data.get("current_price_numeric", 0.0)
            regular_price = data.get("regular_price_numeric", 0.0)
            discount_percentage = data.get("discount_percentage", 0.0)
            
            return ProductData(
                product_id=data.get("id", ""),
                ean_code=data.get("ean", ""),
                product_code=data.get("sku", ""),
                product_name=data.get("name", ""),
                product_title=data.get("name", ""),
                current_price=current_price,
                regular_price=regular_price if regular_price > 0 else None,
                discount_percentage=discount_percentage if discount_percentage > 0 else None,
                has_discount=data.get("has_discount", False),
                product_url=data.get("url", ""),
                image_url=data.get("image_url", ""),
                store_name=store_name
            )
        except Exception as e:
            logger.error(f"Error normalizing TUS product: {e}")
            return None

class ProductAIEnhancer:
    """Enhances products with AI-generated intelligence using Google Gemini"""
    
    # Universal categories - DO NOT CHANGE
    UNIVERSAL_CATEGORIES = [
        "Meso", "Ribe", "Sadje", "Zelenjava", "Mlečni izdelki", "Jajca", 
        "Kruh", "Pekovski izdelki", "Testenine", "Riž", "Žita in kosmiči", 
        "Konzerve", "Trajni izdelki", "Zamrznjena hrana", "Sladkarije", 
        "Prigrizki", "Nealkoholne pijače", "Alkoholne pijače", "Čaji in kave", 
        "Začimbe", "Omake in dodatki", "Olja", "Kis", "Otroška hrana", 
        "Biološka hrana", "Hlajeni izdelki", "Sladoled", "Moke in peka", "Drugo"
    ]
    
    def __init__(self, api_key: str):
        """Initialize with Google Gemini API key"""
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-1.5-pro')
        
    def create_ai_prompt(self, product: ProductData) -> str:
        """Create comprehensive AI prompt for product enhancement"""
        
        prompt = f"""
Analiziraj naslednji izdelek in vrni strukturiran JSON z vsemi AI polji. OBVEZNO uporabi eno od predpisanih glavnih kategorij.

IZDELEK:
- Ime: {product.product_name}
- Naslov: {product.product_title}
- Znamka: {product.brand_name}
- Opis: {product.product_description}
- Kategorija trgovine: {product.store_category}
- Kategorije: {product.categories_data}
- Cena: {product.current_price} EUR
- Alergeni: {product.allergens_data}

OBVEZNE GLAVNE KATEGORIJE (izberi TOČNO eno):
{', '.join(self.UNIVERSAL_CATEGORIES)}

SPECIFIČNE PODKATEGORIJE - POMEMBNO:
- Za MED (kakršenkoli): uporabi podkategorijo "Med" (ne "Sladila")
- Za OLJE: uporabi tip olja (npr. "Olivno olje", "Sončnično olje")  
- Za SIR: uporabi tip sira (npr. "Trdi sir", "Skuta")
- Za SADJE: uporabi ime sadja (npr. "Jabolka", "Banane")
- Za ZAČIMBE: uporabi ime začimbe (npr. "Vanilija", "Poper")

POSEBNI PRIMERI:
✅ Ekološki/Bio med → podkategorija: "Med" (ne "Sladila")
✅ Vanilija → podkategorija: "Vanilija" (ne "Začimbe")
✅ Kisla smetana → podkategorija: "Kisla smetana" (ne "Smetana")


Vrni JSON z naslednjimi polji:

{{
    "ai_main_category": "izberi iz seznama zgoraj",
    "ai_subcategory": "podkategorija",
    "ai_confidence": "high/medium/low",
    
    // Vsebinska obogatitev
    "ai_product_summary": "2-stavčni povzetek",
    "ai_usage_suggestions": "predlogi uporabe",
    "ai_recipe_compatibility": ["pasta-dishes", "salads"], 
    "ai_storage_tips": "nasveti za shranjevanje",
    "ai_preparation_tips": "nasveti za pripravo",
    "ai_pairing_suggestions": "s čim kombinirati",
    "ai_alternative_uses": "alternativne uporabe",
    "ai_key_selling_points": ["key-point-1", "key-point-2"],
    
    // Nakupovalna inteligenca
    "ai_optimal_quantity": 1-10,
    "ai_replacement_urgency": "immediate/within-week/when-convenient",
    "ai_bulk_discount_worthy": true/false,
    "ai_substitute_products": "podobni izdelki",
    "ai_seasonal_availability": "year-round/spring-summer/autumn-winter",
    "ai_stockup_recommendation": true/false,
    "ai_purchase_frequency": "daily/weekly/monthly/occasional",
    "ai_target_demographic": "families/young-adults/seniors/health-conscious",
    "ai_meal_category": "breakfast/lunch/dinner/snack/ingredient",
    "ai_preparation_complexity": "simple/moderate/complex",
    
    // Svežina in shranjevanje
    "ai_freshness_indicator": "very-fresh/fresh/moderate/check-date",
    "ai_shelf_life_estimate": dni_trajanja,
    "ai_storage_requirements": "cool-dry/refrigerated/frozen/room-temperature",
    
    // Zdravje in prehrana
    "ai_health_score": 1-100,
    "ai_nutrition_grade": "A/B/C/D/E",
    "ai_allergen_risk": "high/medium/low/none",
    "ai_allergen_list": "gluten, nuts, dairy",
    "ai_diet_compatibility": ["vegan", "keto", "gluten-free"],
    "ai_organic_verified": true/false,
    "ai_additive_score": 1-100,
    "ai_processing_level": "minimal/moderate/highly-processed",
    "ai_sugar_content": "none/low/medium/high",
    "ai_sodium_level": "none/low/medium/high",
    
    // Vrednost in kakovost
    "ai_value_rating": "excellent/good/fair/poor",
    "ai_price_tier": "budget/mid-range/premium",
    "ai_deal_quality": "excellent/good/fair/poor",
    "ai_quality_tier": "premium/standard/basic",
    "ai_environmental_score": 1-100
}}

POMEMBNO: 
- Uporabi SAMO slovenske kategorije iz seznama
- Vsi odgovori morajo biti v slovenščini
- JSON mora biti veljaven
- Za MED vedno uporabi podkategorijo "Med"
- Če informacij ni dovolj, uporabi razumne ocene
"""
        return prompt
    
    def enhance_product(self, product: ProductData) -> Dict[str, Any]:
        """Enhance single product with AI intelligence"""
        try:
            prompt = self.create_ai_prompt(product)
            
            response = self.model.generate_content(prompt)
            response_text = response.text
            
            # Clean response text to extract JSON
            json_start = response_text.find('{')
            json_end = response_text.rfind('}') + 1
            
            if json_start == -1 or json_end == 0:
                logger.error("No JSON found in AI response")
                return self._get_default_ai_fields(product)
            
            json_text = response_text[json_start:json_end]
            
            # Remove comments from JSON
            json_text = re.sub(r'//.*?\n', '\n', json_text)
            json_text = re.sub(r'/\*.*?\*/', '', json_text, flags=re.DOTALL)
            
            ai_data = json.loads(json_text)
            
            # Validate main category
            if ai_data.get("ai_main_category") not in self.UNIVERSAL_CATEGORIES:
                logger.warning(f"Invalid category: {ai_data.get('ai_main_category')}, defaulting to 'Drugo'")
                ai_data["ai_main_category"] = "Drugo"
            
            # Normalize unit price if we have price info
            if product.current_price:
                ai_data["ai_unit_price_normalized"] = product.current_price
            
            logger.info(f"Successfully enhanced product: {product.product_name}")
            return ai_data
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            return self._get_default_ai_fields(product)
        except Exception as e:
            logger.error(f"Error enhancing product {product.product_name}: {e}")
            return self._get_default_ai_fields(product)
    
    def _get_default_ai_fields(self, product: ProductData) -> Dict[str, Any]:
        """Get default AI fields when enhancement fails"""
        return {
            "ai_main_category": "Drugo",
            "ai_subcategory": "Nerazvrščeno",
            "ai_confidence": "low",
            "ai_product_summary": f"{product.product_name} - {product.brand_name}",
            "ai_usage_suggestions": "Splošna uporaba",
            "ai_recipe_compatibility": [],
            "ai_storage_tips": "Shranite v ustreznih pogojih",
            "ai_preparation_tips": "Pripravite po navodilih",
            "ai_pairing_suggestions": "Kombinirajte po okusu",
            "ai_alternative_uses": "Standardna uporaba",
            "ai_key_selling_points": ["kakovost"],
            "ai_optimal_quantity": 1,
            "ai_replacement_urgency": "when-convenient",
            "ai_bulk_discount_worthy": False,
            "ai_substitute_products": "Podobni izdelki iz iste kategorije",
            "ai_seasonal_availability": "year-round",
            "ai_stockup_recommendation": False,
            "ai_purchase_frequency": "occasional",
            "ai_target_demographic": "families",
            "ai_meal_category": "ingredient",
            "ai_preparation_complexity": "simple",
            "ai_freshness_indicator": "moderate",
            "ai_shelf_life_estimate": 30,
            "ai_storage_requirements": "room-temperature",
            "ai_health_score": 50,
            "ai_nutrition_grade": "C",
            "ai_allergen_risk": "medium",
            "ai_allergen_list": "Preverite etiketo",
            "ai_diet_compatibility": [],
            "ai_organic_verified": False,
            "ai_additive_score": 50,
            "ai_processing_level": "moderate",
            "ai_sugar_content": "medium",
            "ai_sodium_level": "medium",
            "ai_value_rating": "fair",
            "ai_price_tier": "mid-range",
            "ai_deal_quality": "fair",
            "ai_quality_tier": "standard",
            "ai_environmental_score": 50,
            "ai_unit_price_normalized": product.current_price or 0.0
        }

class ProductProcessor:
    """Main processor for handling product enhancement pipeline"""
    
    def __init__(self, gemini_api_key: str):
        self.normalizer = ProductNormalizer()
        self.ai_enhancer = ProductAIEnhancer(gemini_api_key)
    
    def process_product(self, product_json: Dict, store_type: str) -> Optional[Dict[str, Any]]:
        """Process single product through the complete pipeline"""
        try:
            # Normalize based on store type
            if store_type.lower() == "dm":
                product = self.normalizer.normalize_dm_product(product_json, "dm")
            elif store_type.lower() == "mercator":
                product = self.normalizer.normalize_mercator_product(product_json, "mercator")
            elif store_type.lower() == "spar":
                product = self.normalizer.normalize_spar_product(product_json, "spar")
            elif store_type.lower() == "tus":
                product = self.normalizer.normalize_tus_product(product_json, "tus")
            elif store_type.lower() == "lidl":  # ADD THIS LINE
                product = self.normalizer.normalize_lidl_product(product_json, "lidl")  # ADD THIS LINE
            else:
                logger.error(f"Unknown store type: {store_type}")
                return None
            
            # Enhance with AI
            ai_fields = self.ai_enhancer.enhance_product(product)
            
            # Combine normalized data with AI enhancements
            result = {
                # Core product data
                "store_name": product.store_name,
                "product_id": product.product_id,
                "product_code": product.product_code,
                "ean_code": product.ean_code,
                "erp_number": product.erp_number,
                "product_name": product.product_name,
                "product_title": product.product_title,
                "brand_name": product.brand_name,
                "product_description": product.product_description,
                "current_price": product.current_price,
                "regular_price": product.regular_price,
                "discount_percentage": product.discount_percentage,
                "has_discount": product.has_discount,
                "currency_code": product.currency_code,
                "product_url": product.product_url,
                "image_url": product.image_url,
                "store_category": product.store_category,
                "category_level_1": product.category_level_1,
                "category_level_2": product.category_level_2,
                "category_level_3": product.category_level_3,
                "rating_value": product.rating_value,
                "rating_count": product.rating_count,
                "scraped_at": datetime.now().isoformat(),
                
                # AI enhancements
                **ai_fields
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing product: {e}")
            return None
    
    def process_batch(self, products: List[Dict], store_type: str) -> List[Dict[str, Any]]:
        """Process batch of products"""
        results = []
        
        for i, product_json in enumerate(products):
            logger.info(f"Processing product {i+1}/{len(products)}")
            
            result = self.process_product(product_json, store_type)
            if result:
                results.append(result)
            else:
                logger.warning(f"Failed to process product {i+1}")
        
        return results

# Example usage function
def main():
    """Example usage of the product processor"""
    import os
    
    # Get API key from environment variable
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("Please set GEMINI_API_KEY environment variable")
        return
    
    # Initialize processor
    processor = ProductProcessor(api_key)
    
    # Example DM product
    dm_product = {
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
    
    # Process single product
    result = processor.process_product(dm_product, "dm")
    
    if result:
        print("Enhanced product data:")
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print("Failed to process product")

if __name__ == "__main__":
    main()