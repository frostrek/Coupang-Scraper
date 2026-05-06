import sys, re
sys.stdout.reconfigure(encoding='utf-8')
from app.scraper import fetch_with_scrapling

url = 'https://www.amazon.in/dp/B0FMS79CBR'
html = fetch_with_scrapling(url, wait_sec=2)
if 'twister' in html.lower():
    print("Found twister")
if 'variation' in html.lower():
    print("Found variation")
with open('dump_twister.html', 'w', encoding='utf-8') as f:
    f.write(html)
