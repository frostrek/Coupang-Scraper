import sys
sys.stdout.reconfigure(encoding='utf-8')
from curl_cffi import requests
r = requests.get('https://www.amazon.in/dp/B07W59D16G', impersonate='chrome120', headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
with open('dump_raw.html', 'w', encoding='utf-8') as f:
    f.write(r.text)
print("Dumped raw to dump_raw.html")
