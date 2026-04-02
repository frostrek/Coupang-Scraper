from flask import request, jsonify, send_file
from . import app, jobs, OUTPUTS_DIR
from .scraper import scrape_job
import threading, uuid, os

@app.route('/')
def index():
    return send_file('static/index.html')

@app.route('/api/scrape', methods=['POST'])
def start_scrape():
    data    = request.json or {}
    url     = data.get('url','').strip()
    keyword = data.get('keyword','').strip()
    maxp    = min(int(data.get('max_products', 100)), 500)

    if not url:     return jsonify({'error':'URL is required'}), 400
    if not keyword: return jsonify({'error':'Keyword is required'}), 400
    if not url.startswith('http'): url = 'https://' + url

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
    job = jobs.get(jid)
    if not job: return jsonify({'error':'Not found'}), 404
    return jsonify(job)

@app.route('/api/download/<jid>')
def download(jid):
    job = jobs.get(jid)
    if not job or job.get('status') != 'done':
        return jsonify({'error':'Not ready'}), 404
    fp = job.get('filepath','')
    if not os.path.exists(fp): return jsonify({'error':'File missing'}), 404
    return send_file(fp, as_attachment=True, download_name=os.path.basename(fp))

@app.route('/api/data/<jid>')
def get_data(jid):
    job = jobs.get(jid)
    if not job or job.get('status') != 'done':
        return jsonify({'error':'Not ready'}), 404
    products = job.get('products', [])
    return jsonify({'products': products, 'total': len(products)})
