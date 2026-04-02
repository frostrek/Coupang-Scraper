# ── Stage 1: Build & install dependencies ────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /app

# Install system dependencies required for building Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM python:3.11-slim

WORKDIR /app

# Let Playwright automatically install all the missing OS dependencies for Chromium
# so we never miss obscure ones like libX11.so
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    wget \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy installed Python packages from builder stage
COPY --from=builder /install /usr/local

# Install Playwright browsers and missing OS dependencies automatically
RUN playwright install-deps chromium && playwright install chromium

# Copy application code
COPY . .

# Create outputs directory
RUN mkdir -p /app/outputs

# Expose the port Render expects
EXPOSE 10000

# Use gunicorn with gevent workers for production
# - gevent allows background threads (scraping jobs) to work properly
# - timeout 120s to handle long-running scrape requests
CMD ["gunicorn", \
     "--bind", "0.0.0.0:10000", \
     "--workers", "1", \
     "--worker-class", "gevent", \
     "--timeout", "120", \
     "--keep-alive", "5", \
     "--log-level", "info", \
     "app:app"]
