from flask import Flask
import os
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

app = Flask(__name__, static_folder='static', static_url_path='')

limiter = Limiter(
    get_remote_address,
    app=app,
    storage_uri="memory://"
)

# ── Output directory — works on Windows & Linux ───────────────────────────────
# Go up one level from app/ directory to root
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUTS_DIR = os.path.join(ROOT_DIR, "outputs")
os.makedirs(OUTPUTS_DIR, exist_ok=True)

# Shared state
jobs = {}

from . import routes
