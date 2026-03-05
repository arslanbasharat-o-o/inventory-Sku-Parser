<div align="center">
  <h1>🚀 Inventory SKU Parser & Optimizer</h1>
  <p><strong>The ultimate SEO-optimized SKU tracking and extraction tool for E-commerce & Inventory Management.</strong></p>

  [![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
  [![Made with Python & Next.js](https://img.shields.io/badge/Stack-Python%20%7C%20Next.js-success.svg)](#)

</div>

## 🔍 What is Inventory SKU Parser?
**Inventory SKU Parser** is a highly efficient, intelligent parsing engine specifically designed to process bulk eCommerce inventory lists, automatically detect missing SKUs, extract product titles, and generate standardized Web/Product SKUs.

If you deal with unstructured data from suppliers, large CSV/Excel drops, or need strict inventory compliance for SEO-friendly URLs, this tool standardizes everything instantly. 

### ✨ Core Features
*   **Intelligent SKU Extraction:** Uses NLP and trained patterns to understand unformatted titles.
*   **Bulk Processing:** Upload `.xlsx` or `.csv` files and watch the parser process thousands of rows in seconds.
*   **Single SKU Generator:** A quick UI for manual SKU generation to standardize new inventory on the fly.
*   **Analytics & Error Checking:** Built-in duplicate detection for both Titles and SKUs to ensure perfect inventory health.
*   **Modern Next.js Frontend:** Beautiful, responsive UI built with Tailwind CSS.
*   **Scalable Python Backend:** Powered by Flask/Gunicorn, ready for multi-tenant deployment.

## 🚀 Getting Started

### 1. Backend Setup (Python Engine)
```bash
# Clone the repo
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

## 📈 Why this helps E-commerce SEO
Standardizing your SKUs translates directly into better inventory management and cleaner URL structures for your storefront. This parser ensures consistency across:
*   **Google Merchant Center Feeds**
*   **Shopify/WooCommerce Product Slugs**
*   **Internal Site Search**

## 🏗️ Architecture Stack
*   **Backend:** Python, Flask, Gunicorn
*   **Frontend:** Next.js (React), Tailwind CSS, Lucide Icons
*   **AI/Pattern Matching:** Custom regex engines, learned pattern storage (`learned_patterns.json`)

## 🤝 Contributing
Contributions, issues, and feature requests are welcome! Feel free to check the [issues page](#).

---
<div align="center">
  <sub>Built with ❤️ for E-commerce & SEO professionals.</sub>
</div>
