# AWS EC2 Deployment Guide

This guide will walk you through deploying the Coupang-Scraper onto an AWS EC2 instance. Because the scraper runs a full browser environment via Playwright and processes AI tasks, we recommend using at least a `t3.small` or `t3.medium` instance.

## 1. Instance Setup
1. Log in to your AWS Management Console and navigate to EC2.
2. Click **Launch Instance**.
3. **AMI**: Select **Ubuntu Server 22.04 LTS**.
4. **Instance Type**: Select `t3.small` (2 vCPUs, 2 GB RAM) or higher.
5. **Key Pair**: Create or select an existing key pair to securely connect via SSH.
6. **Network Settings**:
   - Check **Allow SSH traffic from** (Anywhere or My IP).
   - Check **Allow HTTP traffic from the internet**.
   - Check **Allow HTTPS traffic from the internet** (if you plan to set up SSL/TLS).

## 2. Connect and Prepare the OS
SSH into your instance using your key pair:
```bash
ssh -i /path/to/your-key.pem ubuntu@<your-ec2-public-ip>
```

Update system packages and install required foundational dependencies:
```bash
sudo apt update
sudo apt upgrade -y
sudo apt install -y python3-pip python3-venv git nginx
```

## 3. Clone and Setup Environment
Navigate to `/var/www/` or your home directory, and clone the repository:

```bash
git clone <your-repository-url> scraper
cd scraper/scrapling
```

Create and activate a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate
```

Install the dependencies:
```bash
pip install -r requirements.txt
```

**Crucial Step for Playwright:**
Install the browsers and Linux OS dependencies required by Playwright:
```bash
playwright install chromium
sudo npx playwright install-deps
```

## 4. Configuration
Create your `.env` file from a template or securely paste it:
```bash
nano .env
```
Populate it with:
```env
GEMINI_API_KEY="your_api_key_here"
DATABASE_URL="postgresql://..." # Or leave empty if not used
MAX_CONCURRENT_SCRAPES=2
# Note: Rate-limiting limits are automatically enforced via Flask-Limiter (in-memory)
```

## 5. Setup Gunicorn as a systemd Service
We will use `systemd` to keep Gunicorn running continuously and automatically restart it on crashes.

Create a new service file:
```bash
sudo nano /etc/systemd/system/scraper.service
```

Add the following configuration (Adjust paths if you cloned into a different directory):
```ini
[Unit]
Description=Gunicorn instance to serve Coupang-Scraper
After=network.target

[Service]
User=ubuntu
Group=www-data
WorkingDirectory=/home/ubuntu/scraper/scrapling
Environment="PATH=/home/ubuntu/scraper/scrapling/venv/bin"
ExecStart=/home/ubuntu/scraper/scrapling/venv/bin/gunicorn --worker-class gevent --workers 2 --bind 127.0.0.1:5055 run:app

[Install]
WantedBy=multi-user.target
```

Enable and start the service:
```bash
sudo systemctl start scraper
sudo systemctl enable scraper
sudo systemctl status scraper
sudo journalctl -u scraper -f   # To observe logs 
```

## 6. Setup Nginx Reverse Proxy
Now configure Nginx to route internet traffic on port 80 to our local Gunicorn server.

Create a new Nginx configuration block:
```bash
sudo nano /etc/nginx/sites-available/scraper
```

Paste the following:
```nginx
server {
    listen 80;
    server_name your_domain_or_public_ip;

    location / {
        proxy_pass http://127.0.0.1:5055;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_addrs;
        
        # Extended timeouts for long-running scraping requests
        proxy_read_timeout 300;
        proxy_connect_timeout 300;
        proxy_send_timeout 300;
    }
}
```

Link, test, and restart Nginx:
```bash
sudo ln -s /etc/nginx/sites-available/scraper /etc/nginx/sites-enabled
sudo nginx -t
sudo systemctl restart nginx
```

## 7. Next Steps
Your API and Dashboard should now be fully live on the EC2 instance's IP address!

- We strongly recommend setting up **SSL with Certbot** (Let's Encrypt) to secure your `.env` secrets and requests.
- Monitor your background workers using `htop` to ensure the instances do not trigger OOM (Out-Of-Memory) kills from Playwright browser usage.
