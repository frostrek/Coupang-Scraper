import sys, re
sys.stdout.reconfigure(encoding='utf-8')
from app.scraper import fetch_with_scrapling

url = 'https://www.amazon.in/dp/B0FMS7SJP6'
html = fetch_with_scrapling(url, wait_sec=2)

with open("dump.html", "w", encoding="utf-8") as f:
    f.write(html)
print("Dumped to dump.html")
