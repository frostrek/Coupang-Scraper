import sys
sys.stdout.reconfigure(encoding='utf-8')
from app.scraper import fetch_pdp_fast
import re
import json

url = 'https://www.amazon.in/dp/B074V7DDRY'
html = fetch_pdp_fast(url)

if html:
    print("Fetched HTML successfully")
    
    # Let's search for colorImages
    m_color = re.search(r'\"colorImages\"\s*:\s*(\{.*?\})', html, re.S)
    if m_color:
        print("Found colorImages data block")
        try:
            # truncate to print
            color_data = m_color.group(1)[:500]
            print("colorImages preview:", color_data)
        except Exception as e:
            print("Error parsing colorImages:", e)

    # Let's search for twister-js-init-dpx-data
    m_twister = re.search(r'data-a-state.*?twister-js-init-dpx-data.*?>(.*?)</script>', html, re.S)
    if m_twister:
        print("Found twister-js-init-dpx-data")
        print(m_twister.group(1)[:500])
        
    m_price = re.search(r'\"variationValues\"\s*:\s*(\{.*?\})', html, re.S)
    if m_price:
        print("Found variationValues")
else:
    print("Fetch failed")
