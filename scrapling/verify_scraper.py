"""Verification script for testing the scraper functionality."""
import os
import sys
import traceback

# Add current dir to path
sys.path.append(os.getcwd())

from bs4 import BeautifulSoup

from app.scraper import (
    extract_products_from_soup,
    fetch_product_details,
    fetch_with_scrapling,
)
from app.llm_processor import sanitize_product_data

def test():
    """Run verification test for the scraper."""
    print("--- STARTING VERIFICATION V2 ---")
    try:
        keyword = "mCaffeine Body Care Gift Set"
        url = f"https://www.amazon.in/s?k={keyword.replace(' ', '+')}"

        print(f"1. Fetching search results for '{keyword}'...")
        html = fetch_with_scrapling(url, wait_sec=5)
        if not html:
            print("❌ Failed to fetch search results.")
            return

        soup = BeautifulSoup(html, 'lxml')
        products = extract_products_from_soup(soup, "https://www.amazon.in")
        print(f"✅ Found {len(products)} products on page.")

        if not products:
            print("❌ No products detected.")
            return

        # Pick the first product
        p = products[0]
        p_url = p.get('_product_url')  # Internal field for PDP URL
        print(f"2. Deep scraping: {p_url}")

        if p_url:
            full_p = fetch_product_details(p_url, p)
            
            print("\n3. Testing Gemini Sanitization...")
            sanitized_p = sanitize_product_data(full_p.copy())
            
            print("\n--- EXTRACTED & SANITIZED FIELDS ---")
            fields_to_check = [
                'Product Name',
                'Brand',
                'Manufacturer',
                'Sale Price',
                'Discount Base Price',
                'Weight',
                'SKU',
                'Model Number',
                'Main Image',
                'Detailed Description',
            ]
            for k in fields_to_check:
                val = str(full_p.get(k, 'N/A'))
                if k == 'Detailed Description':
                    val = val[:100] + "..." if len(val) > 100 else val
                print(f"{k:20}: {val}")

            if full_p.get('SKU') or full_p.get('Manufacturer'):
                print("\n✨ VERIFICATION PASSED: Deep fields captured.")
            else:
                print("\n⚠️ VERIFICATION INCOMPLETE: Some deep fields missing.")
        else:
            print("❌ No Product URL.")

    except Exception as e:
        print(f"❌ Error during verification: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    test()
