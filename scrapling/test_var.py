import requests, re, sys
sys.stdout.reconfigure(encoding='utf-8')
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}
# Maybelline Lipstick with lots of shades
r = requests.get('https://www.amazon.in/dp/B074V7DDRY', headers=headers)
html = r.text

ctx = re.search(r'asin_variation_values', html)
if ctx:
    print('Found asin_variation_values')
    window = html[max(0, ctx.start() - 200): ctx.start() + 120_000]
    asin_entry_re = re.compile(r'"([A-Z0-9]{10})"\s*:\s*\{([^{}]+)\}', re.S)
    matches = list(asin_entry_re.finditer(window))
    print(f'Regex matched {len(matches)} entries')
    for m in matches[:3]:
        print(m.group(1), m.group(2)[:50])
else:
    print('No asin_variation_values found')
