import sys, re
sys.stdout.reconfigure(encoding='utf-8')
from app.scraper import fetch_with_scrapling, _extract_variant_data

search_url = 'https://www.amazon.in/s?k=Maybelline+New+York+Super+Stay+Matte+Ink+Liquid+Lipstick'
print("Searching...")
search_html = fetch_with_scrapling(search_url, wait_sec=2)

asins = re.findall(r'data-asin="([A-Z0-9]{10})"', search_html)
if asins:
    asin = asins[0]
    print(f"Found ASIN: {asin}")
    url = f'https://www.amazon.in/dp/{asin}'
    print("Fetching PDP...")
    html = fetch_with_scrapling(url, wait_sec=2)
    with open('dump2.html', 'w', encoding='utf-8') as f:
        f.write(html)
    
    print("Extracting variants...")
    variants = _extract_variant_data(html, asin)
    print(f"Extracted: {len(variants)}")
else:
    print("No ASINs found")
