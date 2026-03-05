# Inventory SKU Parser

**The SEO-optimized SKU tracking and extraction tool for E-commerce & Inventory Management.**

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Made with Python & Next.js](https://img.shields.io/badge/Stack-Python%20%7C%20Next.js-success.svg)](#)

## Overview
**Inventory SKU Parser** is an efficient, intelligent parsing engine specifically designed to process bulk eCommerce inventory lists, automatically detect missing SKUs, extract product titles, and generate standardized Web/Product SKUs.

If you process unstructured data from suppliers, large CSV/Excel drops, or require strict inventory compliance for SEO-friendly URLs, this application standardizes the data seamlessly.

### Core Features
- **Intelligent SKU Extraction:** Utilizes NLP and trained patterns to structure unformatted product titles.
- **Bulk Processing:** Supports `.xlsx` and `.csv` file uploads, capable of processing thousands of rows in seconds.
- **Single SKU Generator:** A dedicated UI component for manual SKU generation to standardize new inventory entries.
- **Analytics & Error Checking:** Built-in duplicate detection for both Titles and SKUs to ensure inventory accuracy.
- **Modern Next.js Frontend:** Responsive user interface utilizing Tailwind CSS and React.
- **Scalable Python Backend:** Powered by Flask and Gunicorn, engineered for multi-tenant deployment.

## Installation

### 1. Backend Setup (Python Engine)
```bash
# Clone the repository
git clone https://github.com/arslanbasharat-o-o/inventory-Sku-Parser.git
cd inventory-Sku-Parser

# Create a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run the parser engine
python app.py
```

### 2. Frontend Setup (Next.js Dashboard)
```bash
# Navigate to the frontend directory
cd frontend

# Install Node dependencies
npm install

# Start the development server
npm run dev
```

## E-commerce SEO Benefits
Standardizing product SKUs translates directly into improved inventory management and cleaner URL structures for your storefront. This parser ensures data consistency across:
- **Google Merchant Center Feeds**
- **Shopify/WooCommerce Product Slugs**
- **Internal Site Search Architecture**

## Architecture Stack
- **Backend Environment:** Python, Flask, Gunicorn
- **Frontend Environment:** Next.js (React), Tailwind CSS
- **AI/Pattern Matching:** Custom regex engines, learned pattern storage (`learned_patterns.json`)

## Deployment
The repository includes configuration files for scalable deployments:
- Docker support (`deploy/docker/Dockerfile.api`, `docker-compose.scaling.yml`)
- Kubernetes manifests (`deploy/k8s/`)

## Contributing
Contributions, issues, and feature requests are welcome. Feel free to open an issue or submit a pull request for major changes.

---
**License:** MIT License
