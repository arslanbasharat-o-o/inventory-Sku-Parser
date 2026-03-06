# Inventory SKU Parser

Production-grade SKU intelligence engine for mobile phone repair parts inventory.

[![Python Backend CI](https://github.com/arslanbasharat-o-o/inventory-Sku-Parser/actions/workflows/python-backend-ci.yml/badge.svg)](https://github.com/arslanbasharat-o-o/inventory-Sku-Parser/actions/workflows/python-backend-ci.yml)
[![Next.js Frontend CI](https://github.com/arslanbasharat-o-o/inventory-Sku-Parser/actions/workflows/nextjs-frontend-ci.yml/badge.svg)](https://github.com/arslanbasharat-o-o/inventory-Sku-Parser/actions/workflows/nextjs-frontend-ci.yml)

## Version
- Current release: `v3.0.0`
- Changelog: [`CHANGELOG.md`](./CHANGELOG.md)

## What It Does
- Parses messy inventory titles into standardized SKUs.
- Uses a 5-layer parser pipeline (rules, ontology, fuzzy, learning, vectors).
- Enforces battery standardization to `BATT`.
- Supports multi-component titles with part-priority resolution.
- Provides live title analysis API + batch Excel/CSV processing.
- Generates validation reports for parser quality and performance.

## Architecture
- `backend/app.py`: Flask API for bulk parser workflow and downloads.
- `backend/fastapi_app.py`: FastAPI live analyzer and structured parser endpoints.
- `backend/structured_sku_parser.py`: rule-first parser with OpenAI structured fallback.
- `backend/sku_intelligence_engine.py`: core parsing logic and learning subsystems.
- `backend/sku_validation_framework.py`: full validation suite and report generator.
- `frontend/`: Next.js dashboard.

## Quick Start

### 1) Backend setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Run Flask bulk parser service
```bash
python -m backend.app
```

### 3) Run FastAPI live analyzer service
```bash
uvicorn backend.fastapi_app:app --host 0.0.0.0 --port 8000 --reload
```

### 4) Run Next.js frontend
```bash
cd frontend
npm install
npm run dev
```

## API Endpoints
- `GET /healthz`
- `POST /analyze-title`
- `POST /analyze-title/batch`
- `POST /process-inventory-excel`
- `GET /cache/status`
- `DELETE /cache`

`POST /analyze-title` and `POST /generate-sku-api` support optional `product_description` to improve parsing when titles are incomplete.

## Validation
Run full parser validation:
```bash
python -m backend.sku_validation_framework --strict
```

Generated reports:
- `outputs/sku_validation_report.json`
- `outputs/sku_validation_report.md`

## CI/CD
Workflows:
- `.github/workflows/python-backend-ci.yml`
- `.github/workflows/nextjs-frontend-ci.yml`
- `.github/workflows/python-backend-cd.yml`
- `.github/workflows/nextjs-frontend-cd.yml`

Backend CD builds Docker image, runs `/healthz` smoke test, and can publish to GHCR on version tags.

## License
MIT (see [`LICENSE`](./LICENSE)).
