# FCC-Broadcast-Ad-Prediction

## Project Overview
Political Ad Revenue Estimator for broadcast TV credits (Gray TV, Nexstar, Scripps, Sinclair, Tegna) using FCC Online Public Inspection File (OPIF) data. Buy-side credit research tool.

## Architecture
Five-stage pipeline:
1. **Stage 1 - Station Enumerator**: Map broadcasters to station call signs via FCC LMS data
2. **Stage 2 - Political File Crawler**: Discover political ad documents on FCC OPIF (httpx + Playwright)
3. **Stage 3 - PDF Pipeline**: Download PDFs, extract text (PyMuPDF), OCR scanned docs (PaddleOCR/tesseract)
4. **Stage 4 - LLM Extractor**: Claude structured outputs to extract dollar amounts, dates, advertiser details
5. **Stage 5 - Financial Model**: Aggregate by operator/DMA/quarter, generate Excel workbook

## Key Technical Decisions
- **SQLite** (WAL mode) as document registry connecting all stages
- **Anthropic structured outputs** with Pydantic models for schema-guaranteed extraction
- **Zero imputation policy**: failed extractions = NULL, never estimated
- **INVOICE vs CONTRACT** always tracked separately
- **Revenue attributed by flight_date**, not invoice_date or upload_ts
- **Operator mapping from versioned CSV** (static/operator_stations.csv), never inferred

## Commands
```bash
pip install -e ".[dev]"           # Install with dev deps
pip install -e ".[ocr,dev]"      # Install with OCR + dev deps
fcc-tracker --help                # Show all commands
fcc-tracker run --operators gray --year 2026 --update  # Full pipeline
fcc-tracker status                # Pipeline dashboard
pytest tests/ -v                  # Run tests
```

## Data Integrity Rules
1. amount=NULL if not extractable with high confidence
2. No extrapolation from partial coverage
3. Coverage metrics shown on every output
4. No smoothing/interpolation
5. INVOICE and CONTRACT dollars never mixed
6. Gross and Net extracted independently, never calculated from each other
7. Race metadata from folder path (primary), LLM extraction (flagged fallback)

## File Layout
- `src/fcc_ad_tracker/` - Main package
- `static/operator_stations.csv` - Versioned station-to-operator mapping
- `data/` - Runtime data (git-ignored)
- `output/` - Generated Excel/CSV (git-ignored)
- `tests/` - Pytest test suite
