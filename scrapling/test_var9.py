import sys, re
sys.stdout.reconfigure(encoding='utf-8')
from app.scraper import fetch_with_scrapling

search_url = 'https://www.amazon.in/s?k=Maybelline+New+York+Super+Stay+Matte+Ink+Liquid+Lipstick'
search_html = fetch_with_scrapling(search_url, wait_sec=2)

asins = re.findall(r'data-asin="([A-Z0-9]{10})"', search_html)
print(f"Found {len(asins)} ASINs")

for asin in asins[:5]:
    print(f"Testing {asin}...")
    html = fetch_with_scrapling(f'https://www.amazon.in/dp/{asin}', wait_sec=1)
    if 'asin_variation_values' in html:
        print(f"BINGO! {asin} has variants")
        with open('dump_variant.html', 'w', encoding='utf-8') as f:
            f.write(html)
        break
