"""API routes for the scraper application."""
import re
import threading
import uuid
import os
import ipaddress
from urllib.parse import urlparse

from flask import request, jsonify, send_file
from . import app, jobs, OUTPUTS_DIR, limiter
from .scraper import scrape_job
from .excel_utils import build_excel

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


def _safe_int(value, default=100, min_val=1, max_val=500):
    """Safely parse an integer with bounds. Never crashes."""
    try:
        return max(min_val, min(int(value), max_val))
    except (ValueError, TypeError):
        return default


# ─────────────────────────────────────────────────────────────────────────────
# RATE LIMIT ERROR HANDLER
# ─────────────────────────────────────────────────────────────────────────────
@app.errorhandler(429)
def rate_limit_handler(e):
    return jsonify({
        'error': 'Too many requests. Please wait a moment before trying again.',
        'retry_after': e.description
    }), 429


@app.route('/')
def index():
    return send_file('static/index.html')


@app.route('/api/scrape', methods=['POST'])
@limiter.limit("5 per minute")
def start_scrape():
    # Content-Type validation
    if not request.is_json:
        return jsonify({'error': 'Request must be JSON (Content-Type: application/json)'}), 400

    data    = request.json or {}
    url     = data.get('url','').strip()
    keyword = data.get('keyword','').strip()
    maxp    = _safe_int(data.get('max_products', 100), default=100, min_val=1, max_val=500)
    pincode = data.get('pincode', '').strip()
    delivery_filter = bool(data.get('delivery_filter', False))

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

    # ── Pincode Validation ──
    if delivery_filter:
        if not pincode:
            return jsonify({'error': 'Pincode is required when delivery filter is enabled.'}), 400
        # Sanitize: digits only
        pincode = re.sub(r'[^0-9]', '', pincode)
        # Validate length based on domain
        url_lower = url.lower()
        if 'amazon.in' in url_lower or 'flipkart' in url_lower:
            if len(pincode) != 6:
                return jsonify({'error': 'Indian pincode must be exactly 6 digits.'}), 400
        elif 'amazon.com' in url_lower and 'amazon.com.' not in url_lower:
            if len(pincode) != 5:
                return jsonify({'error': 'US ZIP code must be exactly 5 digits.'}), 400
        elif len(pincode) < 4 or len(pincode) > 10:
            return jsonify({'error': 'Pincode must be 4-10 digits.'}), 400

    # Check concurrency limits (Render free tier RAM is tightly restricted)
    active_jobs = sum(1 for j in jobs.values() if j.get('status') in ['running', 'queued'])
    if active_jobs >= 2:
        return jsonify({'error': 'Server is at maximum capacity (2 running jobs). Please wait for an existing job to finish and try again.'}), 429

    jid = str(uuid.uuid4())[:8]
    jobs[jid] = {
        'status': 'queued', 'progress': 0, 'found': 0, 'log': [],
        'last_message': 'Queued', 'url': url, 'keyword': keyword,
        'delivery_filter': delivery_filter, 'pincode': pincode,
        'cancelled': False,  # Cancellation flag
        'products': [],      # Live product list
        'max_products': maxp,
    }
    
    threading.Thread(
        target=scrape_job, 
        args=(jid, jobs, url, keyword, maxp, OUTPUTS_DIR),
        kwargs={'pincode': pincode, 'delivery_filter': delivery_filter},
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


# ─────────────────────────────────────────────────────────────────────────────
# MID-SCRAPE PARTIAL DOWNLOAD — Export Excel from products scraped so far
# ─────────────────────────────────────────────────────────────────────────────
@app.route('/api/download-partial/<jid>')
def download_partial(jid):
    if not re.match(r'^[a-f0-9\-]{8}$', jid):
        return jsonify({'error': 'Invalid job ID'}), 400
    job = jobs.get(jid)
    if not job:
        return jsonify({'error': 'Not found'}), 404
    
    products = job.get('products', [])
    if not products:
        return jsonify({'error': 'No products scraped yet. Wait for at least one product.'}), 404
    
    # Build a partial Excel from whatever we have right now
    try:
        fp = build_excel(
            products,
            job.get('keyword', 'unknown'),
            job.get('url', ''),
            OUTPUTS_DIR,
            partial=True
        )
        if not fp or not os.path.exists(fp):
            return jsonify({'error': 'Failed to generate partial export.'}), 500
        return send_file(fp, as_attachment=True, download_name=os.path.basename(fp))
    except Exception as e:
        return jsonify({'error': f'Export failed: {str(e)}'}), 500


# ─────────────────────────────────────────────────────────────────────────────
# CANCEL JOB — Gracefully stop a running scrape
# ─────────────────────────────────────────────────────────────────────────────
@app.route('/api/cancel/<jid>', methods=['POST'])
def cancel_job(jid):
    if not re.match(r'^[a-f0-9\-]{8}$', jid):
        return jsonify({'error': 'Invalid job ID'}), 400
    job = jobs.get(jid)
    if not job:
        return jsonify({'error': 'Not found'}), 404
    if job.get('status') not in ('running', 'queued'):
        return jsonify({'error': 'Job is not running'}), 400
    
    # Set cancellation flag — the scrape loop checks this each iteration
    job['cancelled'] = True
    return jsonify({'success': True, 'message': 'Cancellation requested. The job will stop after the current product finishes.'})


@app.route('/api/data/<jid>')
def get_data(jid):
    if not re.match(r'^[a-f0-9\-]{8}$', jid):
        return jsonify({'error': 'Invalid job ID'}), 400
    job = jobs.get(jid)
    if not job:
        return jsonify({'error': 'Not found'}), 404
    products = job.get('products', [])
    return jsonify({'products': products, 'total': len(products)})
