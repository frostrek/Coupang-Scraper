"""API routes for the scraper application."""
import re
import threading
import uuid
import os
import ipaddress
from urllib.parse import urlparse

from flask import request, jsonify, send_file
from . import app, jobs, OUTPUTS_DIR
from .scraper import scrape_job

# ─────────────────────────────────────────────────────────────────────────────
# SECURITY: Domain Allowlist (SSRF Prevention)
# Only these e-commerce domains are permitted for scraping.
# ─────────────────────────────────────────────────────────────────────────────
ALLOWED_DOMAINS = [
    'amazon.com', 'amazon.in', 'amazon.co.uk', 'amazon.de', 'amazon.co.jp',
    'amazon.fr', 'amazon.it', 'amazon.es', 'amazon.ca', 'amazon.com.au',
    'flipkart.com',
    'nykaa.com',
    'meesho.com',
    'snapdeal.com',
    'ebay.com', 'ebay.co.uk', 'ebay.de',
    'walmart.com',
    'myntra.com',
    'ajio.com',
    'coupang.com',
]


def _is_allowed_url(url: str) -> bool:
    """Validates that a URL belongs to an allowed e-commerce domain.
    
    Blocks:
    - Private/internal IP addresses (SSRF)
    - Non-allowlisted domains
    - Non-HTTP(S) schemes
    """
    try:
        parsed = urlparse(url)
        
        # Block non-HTTP schemes (e.g. file://, ftp://, javascript:)
        if parsed.scheme not in ('http', 'https'):
            return False
        
        hostname = parsed.hostname
        if not hostname:
            return False
        
        # Block private/internal IP addresses
        try:
            ip = ipaddress.ip_address(hostname)
            if ip.is_private or ip.is_loopback or ip.is_reserved or ip.is_link_local:
                return False
        except ValueError:
            pass  # It's a domain name, not an IP — continue to domain check
        
        # Check against allowed domains
        hostname_lower = hostname.lower()
        for allowed in ALLOWED_DOMAINS:
            if hostname_lower == allowed or hostname_lower.endswith('.' + allowed):
                return True
        
        return False
    except Exception:
        return False


def _sanitize_keyword(keyword: str) -> str:
    """Sanitize keyword input — allow only safe characters."""
    # Allow: alphanumeric, spaces, hyphens, underscores, dots, commas
    # Remove everything else to prevent injection attacks
    sanitized = re.sub(r'[^\w\s\-.,\'\"&+]', '', keyword)
    return sanitized.strip()[:200]  # Cap at 200 chars


@app.route('/')
def index():
    return send_file('static/index.html')


@app.route('/api/scrape', methods=['POST'])
def start_scrape():
    data    = request.json or {}
    url     = data.get('url','').strip()
    keyword = data.get('keyword','').strip()
    maxp    = max(1, min(int(data.get('max_products', 100)), 500))

    if not url:
        return jsonify({'error': 'URL is required'}), 400
    if not keyword:
        return jsonify({'error': 'Keyword is required'}), 400
    
    # Prepend scheme if missing
    if not url.startswith('http'):
        url = 'https://' + url

    # ── SSRF Protection: Validate domain against allowlist ──
    if not _is_allowed_url(url):
        return jsonify({
            'error': 'Unsupported website. Only major e-commerce platforms are allowed '
                     '(Amazon, Flipkart, eBay, Walmart, Myntra, Ajio, Nykaa, Meesho, Snapdeal, Coupang).'
        }), 403

    # Sanitize keyword
    keyword = _sanitize_keyword(keyword)
    if not keyword:
        return jsonify({'error': 'Keyword contains no valid characters.'}), 400

    # Check concurrency limits (Render free tier RAM is tightly restricted)
    active_jobs = sum(1 for j in jobs.values() if j.get('status') in ['running', 'queued'])
    if active_jobs >= 2:
        return jsonify({'error': 'Server is at maximum capacity (2 running jobs). Please wait for an existing job to finish and try again.'}), 429

    jid = str(uuid.uuid4())[:8]
    jobs[jid] = {'status':'queued','progress':0,'found':0,'log':[],
                 'last_message':'Queued','url':url,'keyword':keyword}
    
    threading.Thread(
        target=scrape_job, 
        args=(jid, jobs, url, keyword, maxp, OUTPUTS_DIR), 
        daemon=True
    ).start()
    
    return jsonify({'job_id': jid})


@app.route('/api/status/<jid>')
def get_status(jid):
    # Sanitize job ID to prevent path traversal
    if not re.match(r'^[a-f0-9\-]{8}$', jid):
        return jsonify({'error': 'Invalid job ID'}), 400
    job = jobs.get(jid)
    if not job: return jsonify({'error':'Not found'}), 404
    return jsonify(job)


@app.route('/api/download/<jid>')
def download(jid):
    if not re.match(r'^[a-f0-9\-]{8}$', jid):
        return jsonify({'error': 'Invalid job ID'}), 400
    job = jobs.get(jid)
    if not job or job.get('status') != 'done':
        return jsonify({'error':'Not ready'}), 404
    fp = job.get('filepath','')
    # Security: Ensure filepath is within the outputs directory (path traversal prevention)
    real_fp = os.path.realpath(fp)
    real_outputs = os.path.realpath(OUTPUTS_DIR)
    if not real_fp.startswith(real_outputs):
        return jsonify({'error': 'Access denied'}), 403
    if not os.path.exists(fp): return jsonify({'error':'File missing'}), 404
    return send_file(fp, as_attachment=True, download_name=os.path.basename(fp))


@app.route('/api/data/<jid>')
def get_data(jid):
    if not re.match(r'^[a-f0-9\-]{8}$', jid):
        return jsonify({'error': 'Invalid job ID'}), 400
    job = jobs.get(jid)
    if not job or job.get('status') != 'done':
        return jsonify({'error':'Not ready'}), 404
    products = job.get('products', [])
    return jsonify({'products': products, 'total': len(products)})
