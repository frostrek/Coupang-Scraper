import sys, re
sys.stdout.reconfigure(encoding='utf-8')
from app.scraper import fetch_with_scrapling, _extract_variant_data

search_url = 'https://www.amazon.in/s?k=Maybelline+New+York+Super+Stay+Matte+Ink+Liquid+Lipstick'
search_html = fetch_with_scrapling(search_url, wait_sec=2)

asins = re.findall(r'data-asin="([A-Z0-9]{10})".*?<h2[^>]*>.*?<span[^>]*>([^<]+)</span>', search_html, re.S)

for asin, title in asins:
    if 'maybelline' in title.lower():
        print(f"Testing {asin}: {title.strip()}")
        html = fetch_with_scrapling(f'https://www.amazon.in/dp/{asin}', wait_sec=1)
        variants = _extract_variant_data(html, asin)
        if variants:
            print(f"BINGO! {len(variants)} variants found")
            for v in variants:
                print(v)
            break
        else:
            print("No variants")
