# Detailed Render.com Deployment Guide

This project is fully containerized with **Docker** and pre-configured with a **Render Blueprint** (`render.yaml`) to make hosting as seamless as possible. Since you are using browser-based scraping (Scrapling/Playwright), Render's Docker runtime is the most stable and reliable choice.

## 🚀 Pre-Deployment Checklist
1. **GitHub Access**: Ensure your private repository contains the latest `Dockerfile`, `render.yaml`, and `requirements.txt`.
2. **Supabase**: Have your `DATABASE_URL` ready (use the **Transaction Connection String**, usually port `6543`).
3. **Gemini**: Have your `GEMINI_API_KEY` ready.

---

## 🛠️ Step-by-Step Deployment

### Step 1: Connect GitHub to Render
1. Log in to [dashboard.render.com](https://dashboard.render.com).
2. Click **New** (top right) → **Web Service**.
3. Choose **Connect a repository**.
4. Select your private GitHub repository from the list. 
   - *If it's not showing, click "Configure" to grant Render access to that specific private repo.*

### Step 2: Configure Service Settings
Render will automatically detect the settings from the `render.yaml` Blueprint, but if you are doing a manual setup, use these:
- **Name**: `amazon-data-harvester` (or your choice)
- **Region**: `Singapore (so-1)` or `Oregon (us-west-2)`
- **Branch**: `main`
- **Runtime**: **Docker** ⬅️ *Crucial for Playwright/Scrapling support*
- **Plan**: **Free** (or Starter for higher concurrency)

### Step 3: Add Environment Variables
Before clicking "Create Web Service", scroll down to the **Environment** section (or do this after creation):
1. Click **Add Environment Variable**.
2. Key: `GEMINI_API_KEY` | Value: `[Your Gemini Key]`
3. Key: `DATABASE_URL` | Value: `[Your Supabase String]`
4. Key: `PYTHONUNBUFFERED` | Value: `1`

### Step 4: Finalize & Monitor
1. Click **Create Web Service**.
2. Render will begin building the Docker image. 
   - *Note: The first build involves downloading Chromium and OS dependencies, so it may take 5–8 minutes. Subsequent builds will be much faster.*
3. Watch the logs. Once you see `[INFO] Booting worker with pid...`, your app is **LIVE**.

---

## ⚠️ Important Production Notes

### 1. Ephemeral Filesystem (Free Tier)
Render's filesystem is **ephemeral**. This means:
- Any Excel files generated in the `outputs/` folder will be **deleted** if the server restarts or you redeploy.
- **Solution**: Because we are saving every product to your **Supabase Data Warehouse** in real-time, your data is 100% safe! You can always regenerate an Excel sheet from the database later if needed.

### 2. Startup Delay (Free Tier)
If you use the Render **Free Plan**, the server will "sleep" after 15 minutes of inactivity. The first time you visit the URL after it sleeps, it may take **30–60 seconds** to wake up.

### 3. Memory Limits
The Render Free tier provides **512MB RAM**.
- I have optimized the scraper to share a single browser instance (`Shared Fetcher`) to prevent it from crashing the server.
- If you find the server crashing during 50+ product scrapes, consider upgrading to the **Starter Plan ($7/mo)** which provides more RAM and faster CPUs for the Scraping engine.

### 4. Health Checks
The project includes a `/` root route and a `healthCheckPath` in `render.yaml`. This ensures Render knows the app is alive and ready to process your scraping requests.
