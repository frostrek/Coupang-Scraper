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

# Install runtime system dependencies:
# - libpq5: PostgreSQL client library (for psycopg2)
# - Chromium + deps: Required by Playwright/Scrapling for headless browsing
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    # Playwright/Chromium dependencies
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    libatspi2.0-0 \
    libwayland-client0 \
    fonts-liberation \
    fonts-noto-cjk \
    xdg-utils \
    wget \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy installed Python packages from builder stage
COPY --from=builder /install /usr/local

# Install Playwright browsers (Chromium only to save space)
RUN playwright install chromium

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
     "--workers", "2", \
     "--worker-class", "gevent", \
     "--timeout", "120", \
     "--keep-alive", "5", \
     "--log-level", "info", \
     "app:app"]
