import os
import json
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

# Configure the Gemini API
api_key = os.getenv("GEMINI_API_KEY")
if api_key:
    genai.configure(api_key=api_key)

def sanitize_product_data(product):
    """
    Sanitizes product data using Gemini LLM to remove Coupang-banned keywords.
    Replaces banned words with safe alternatives while preserving meaning.
    """
    if not api_key:
        print("[Gemini] Warning: GEMINI_API_KEY not found. Skipping sanitization.")
        return product

    try:
        model = genai.GenerativeModel("gemini-flash-latest")
        
        prompt = f"""
        You are an expert e-commerce catalog optimizer for Coupang. 
        Your task is to review and sanitize the following product data to ensure it complies with Coupang's listing policies.
        
        CRITICAL RULES:
        1. Remove/Replace Superlatives: Terms like "Best", "No. 1", "Top", "Greatest", "World's Best" are strictly banned. Replace them with factual, neutral alternatives.
        2. Remove Competitor Marks: Mentions of "Amazon", "Flipkart", "eBay", "Walmart", "Naver", etc., must be removed or replaced with generic terms.
        3. Remove Misleading Shipping/Price info: Terms like "Rocket Delivery", "Free Shipping", "Lowest Price", "Fastest Shipping" must be removed.
        4. Product Title Limit: Ensure the Product Name is concise and under 200 characters.
        5. Maintain Meaning: Ensure the sanitized text accurately and professionally describes the product.
        
        Input Product Data:
        - Product Name: {product.get('Product Name')}
        - Brand: {product.get('Brand')}
        - Manufacturer: {product.get('Manufacturer')}
        - Detailed Description: {product.get('Detailed Description')}
        - Search Keywords: {product.get('Search Keywords')}
        
        Return the updated data in strict JSON format with the following keys:
        "Product Name", "Brand", "Manufacturer", "Detailed Description", "Search Keywords"
        
        Return ONLY the JSON object.
        """
        
        response = model.generate_content(prompt)
        text = response.text.strip()
        
        # Handle markdown code blocks if present
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].split("```")[0].strip()
            
        sanitized = json.loads(text)
        
        # Update product dict with sanitized fields
        for key in ["Product Name", "Brand", "Manufacturer", "Detailed Description", "Search Keywords"]:
            if key in sanitized:
                product[key] = sanitized[key]
        
        return product
    except Exception as e:
        print(f"[Gemini] Error during sanitization: {e}")
        return product
