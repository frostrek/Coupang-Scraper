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
        model = genai.GenerativeModel("gemini-2.5-pro")
        
        prompt = f"""
You are a strict e-commerce catalog compliance engine and Precision Data Extractor for Coupang, South Korea's largest e-commerce platform.
Your job is twofold:
1) Sanitize the input product data to make it policy-compliant.
2) Read the "Raw Specifications" text dump to definitively extract the exact Brand and Manufacturer.

---

## SECTION 1 — TITLE SANITIZATION

Apply ALL of the following rules to the "Product Name":
1. BANNED SUPERLATIVES: Remove "Best", "No.1", "#1", "Top", "Greatest", "World's Best", "Unbeatable". Replace with factual alternatives.
2. BANNED COMPETITOR NAMES: Remove Amazon, Flipkart, eBay, Walmart, Naver, Coupang, Aliexpress.
3. BANNED CLAIMS: Remove "Rocket Delivery", "Lowest Price", "FDA Approved", "Cures", etc.
4. TITLE LENGTH: Must remain under 100 characters.
5. NO EMOJIS OR LOGOS.

---

## SECTION 2 — BRAND & MANUFACTURER EXTRACTION

The provided "Brand" and "Manufacturer" fields are often blank or inaccurate because of HTML scraping limitations.
You MUST read the "Raw Specifications Text" carefully to fix them.
1. Find the true Manufacturer: Look for strings like "Produced by", "Manufacturer:", "Mfg", or "Importer" in the Raw Specifications. Extract ONLY the company name into the "Manufacturer" field. Do not leave it blank if the information exists in the text.
2. Find the true Brand: If the Brand field is empty or generic, extract it from the Raw Specifications or the first 1-3 words of the Product Name.
3. If you absolutely cannot find them anywhere in the text, return the original values.

---

## SECTION 3 — DESCRIPTION & KEYWORDS

1. Sanitize the "Detailed Description" using Section 1 rules. Preserve volume, ingredients, and specs.
2. Sanitize "Search Keywords", preserving valid ones.

---

## OUTPUT FORMAT

Return ONLY a valid JSON object with exactly these five keys (no markdown formatting, no code blocks, no extra text):

{{
  "Product Name": "...",
  "Brand": "...",
  "Manufacturer": "...",
  "Detailed Description": "...",
  "Search Keywords": "..."
}}

---

## INPUT

- Product Name: {product.get('Product Name', '')}
- Brand (Current): {product.get('Brand', '')}
- Manufacturer (Current): {product.get('Manufacturer', '')}
- Search Keywords: {product.get('Search Keywords', '')}
- Detailed Description: {product.get('Detailed Description', '')}

## RAW SPECIFICATIONS TEXT
{product.get('_raw_specs', '')}
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
