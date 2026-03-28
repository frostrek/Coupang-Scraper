from flask import Flask
import os

app = Flask(__name__, static_folder='static', static_url_path='')

# ── Output directory — works on Windows & Linux ───────────────────────────────
# Go up one level from app/ directory to root
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUTS_DIR = os.path.join(ROOT_DIR, "outputs")
os.makedirs(OUTPUTS_DIR, exist_ok=True)

# Shared state
jobs = {}

from . import routes
