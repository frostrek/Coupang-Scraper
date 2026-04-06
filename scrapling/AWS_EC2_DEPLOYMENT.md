# AWS EC2 Deployment Guide

Follow these steps to deploy the Amazon Scraper on an AWS EC2 instance (recommended: **Ubuntu 22.04 LTS**).

## 1. Launch EC2 Instance
- **Instance Type**: `t3.medium` or higher (Scraping with browsers requires at least 4GB RAM).
- **Storage**: 20GB+ SSD.
- **Security Group**:
  - Allow **SSH** (Port 22) from your IP.
  - Allow **Custom TCP** (Port 5055) from anywhere (or your IP) to access the dashboard.

## 2. System Preparation
Connect to your instance via SSH and run:
```bash
sudo apt update && sudo apt upgrade -y
sudo apt install python3-pip python3-venv git libpq-dev -y
```

## 3. Clone and Setup App
```bash
git clone <your-repo-url>
cd Coupang-Scraper/scrapling

# Setup Virtual Environment
python3 -m venv venv
source venv/bin/activate

# Install Dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Install Playwright/Patchright Browsers
playwright install chromium
sudo playwright install-deps
```

## 4. Environment Configuration
Create the `.env` file:
```bash
nano .env
```
Paste your configuration:
```env
GEMINI_API_KEY="your_api_key"
DATABASE_URL="your_supabase_url"
```

## 5. Production Setup (Gunicorn + Systemd)

### Create a Systemd Service
```bash
sudo nano /etc/systemd/system/scraper.service
```
Paste the following (Replace `ubuntu` and path if different):
```ini
[Unit]
Description=Amazon Scraper Flask App
After=network.target

[Service]
User=ubuntu
Group=www-data
WorkingDirectory=/home/ubuntu/Coupang-Scraper/scrapling
Environment="PATH=/home/ubuntu/Coupang-Scraper/scrapling/venv/bin"
EnvironmentFile=/home/ubuntu/Coupang-Scraper/scrapling/.env
ExecStart=/home/ubuntu/Coupang-Scraper/scrapling/venv/bin/gunicorn --workers 4 --bind 0.0.0.0:5055 run:app

[Install]
WantedBy=multi-user.target
```

### Start the Service
```bash
sudo systemctl daemon-reload
sudo systemctl start scraper
sudo systemctl enable scraper
```

## 6. Access the Dashboard
Navigate to `http://your-ec2-public-ip:5055` in your browser.

> [!IMPORTANT]
> If the scraper is blocked by Amazon frequently on EC2, consider adding a residential proxy provider in the Scrapling configuration or rotating your Elastic IP.
