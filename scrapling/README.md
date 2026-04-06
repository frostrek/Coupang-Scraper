# High-Performance Amazon & E-commerce Scraper

A production-grade, highly concurrent e-commerce scraping system designed to extract, normalize, and enrich product data using **Scrapling**, **Patchright**, and **Google Gemini AI**. 

Specifically hardened for Amazon (India/US) to achieve 100% data accuracy through PDP-level authority and AI-driven data sanitization.

## 🌟 Key Features
- **PDP-First Authority**: PDP (Product Detail Page) data strictly overrides search-level data to eliminate "phantom" prices or generic thumbnails.
- **AI-Powered Normalization**: Utilizes Gemini 2.0 Flash to standardize weights/volumes, enforce description word counts, and classify categories.
- **Anti-Blocking Stealth**: Uses `Scrapling` and `curl_cffi` to bypass TLS fingerprinting and bypass CAPTCHAs via advanced header rotation.
- **Metric Exclusivity**: Forces a strict choice between Weight (g/kg) and Volume (ml/L) to prevent contradictory Excel data.
- **Image Intelligence**: Automatically bypasses Amazon's lazy-load transparent GIF placeholders to capture high-resolution product imagery.

## 📦 Prerequisites
- **Python 3.10+**
- **Google Gemini API Key** (for data sanitization)
- **Supabase/PostgreSQL** (Optional - for duplicate prevention)

## 🛠 Installation & Setup

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd Coupang-Scraper/scrapling
   ```

2. **Create and Activate a Virtual Environment**:
   ```bash
   python -m venv venv
   # Windows
   .\venv\Scripts\activate
   # macOS/Linux
   source venv/bin/activate
   ```

3. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Install Browsers (CRITICAL)**:
   ```bash
   playwright install chromium
   ```

## ⚙️ Configuration

Create a `.env` file in the `scrapling/` directory:

```env
# Essential for AI Processing
GEMINI_API_KEY="your_api_key_here"

# Database Configuration (Optional - set to empty string to skip)
DATABASE_URL="postgresql://postgres:[password]@db.supabase.com:5432/postgres"

# Concurrency Control (Default: 2-5 for free tier hosting)
MAX_CONCURRENT_SCRAPES=2
```

## 🚀 Usage

Start the server:
```bash
python run.py
```

- Access the Dashboard at `http://127.0.0.1:5055`
- Paste an Amazon Search URL (e.g., `https://www.amazon.in/s?k=protein+powder`)
- Set the limit and click **Scrape**.
- Downloads will appear in the `outputs/` folder.

## ☁️ Deployment
For detailed instructions on deploying to AWS or Render, see:
- [AWS EC2 Deployment Guide](AWS_EC2_DEPLOYMENT.md)
- [Render Deployment Guide](RENDER_DEPLOYMENT.md)

## 📊 Data Mapping Rules
- **Sale Price**: The current price the customer pays.
- **Discount Base Price**: The original M.R.P. (crossed out). If no discount exists, this matches the Sale Price exactly.
- **Metric Exclusivity**: If a product has both Weight and Volume, the AI prioritizes the most logical one (e.g., SOAP = Weight, HAIR OIL = Volume).
