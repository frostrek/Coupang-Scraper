import sys, re
sys.stdout.reconfigure(encoding='utf-8')
from app.scraper import fetch_with_scrapling

url = 'https://www.amazon.com/dp/B074V7DDRY'
html = fetch_with_scrapling(url, wait_sec=2)

with open('dump3.html', 'w', encoding='utf-8') as f:
    f.write(html)
print("Dumped B074V7DDRY")
