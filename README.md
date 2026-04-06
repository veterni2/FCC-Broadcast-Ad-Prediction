# FCC-Broadcast-Ad-Prediction

**Political Ad Revenue Estimator for Broadcast TV Credits**

A buy-side credit research tool that ingests political advertising data filed with the FCC by broadcast TV stations, extracts dollar amounts from the underlying PDFs, aggregates by operator and DMA, and produces a quarterly political ad revenue model that updates in near-real-time — ahead of earnings releases.

## Target Broadcasters

| Operator | Stations | Credit Profile | Priority |
|----------|----------|---------------|----------|
| Gray Television | ~180 | CCC-range HY | 1 (MVP) |
| Nexstar Media Group | ~200 | B1/BB HY bonds | 2 |
| E.W. Scripps | ~60 | Midsize | 3 |
| Sinclair Broadcast Group | ~185 | Restructuring | 4 |
| Tegna | ~60 | Post-merger | 5 |

## Data Source

[FCC Online Public Inspection File (OPIF)](https://publicfiles.fcc.gov) — every TV broadcast station in America files political advertising orders, contracts, and invoices here. This tool systematically ingests and structures that data.

## Architecture

Five-stage pipeline:

1. **Station Enumerator** — Maps broadcasters to FCC call signs via curated CSV (sourced from FCC Form 323 ownership reports)
2. **Political File Crawler** — Discovers political ad documents on FCC OPIF using the search API and Playwright for JS-rendered pages
3. **PDF Pipeline** — Downloads PDFs, extracts text (PyMuPDF), OCR for scanned documents (PaddleOCR/tesseract)
4. **LLM Extractor** — Claude structured outputs extract dollar amounts, dates, advertiser details into validated Pydantic models
5. **Financial Model** — Aggregates by operator/DMA/quarter, generates 5-tab Excel workbook

## Quick Start

```bash
# Install
pip install -e ".[dev]"

# Install Playwright browsers
playwright install chromium

# (Optional) Install OCR dependencies
pip install -e ".[ocr,dev]"

# Configure
cp .env.example .env
# Edit .env with your Anthropic API key

# Run the pipeline (Gray TV MVP)
fcc-tracker run --operators gray --year 2026

# Check pipeline status
fcc-tracker status
```

## CLI Commands

```bash
fcc-tracker run       --operators gray --year 2026 --update    # Full pipeline
fcc-tracker crawl     --operators gray --year 2026             # Stage 2 only
fcc-tracker download  --operators gray --year 2026 --limit 100 # Stage 3a only
fcc-tracker extract   --operators gray --year 2026 --limit 50  # Stages 3b+4
fcc-tracker model     --operators gray --year 2026             # Stage 5 only
fcc-tracker status                                              # Dashboard
fcc-tracker validate  --operators gray --year 2024             # vs. 10-K actuals
```

## Data Integrity Policy

This tool is used for investment research. The following rules are absolute:

1. **Zero imputation** — If a dollar amount cannot be extracted with high confidence, the record gets `amount=NULL` and is excluded from revenue totals
2. **No extrapolation** — Coverage gaps are shown as gaps, never multiplied up
3. **Every output shows** — total $ extracted, doc count attempted vs. extracted, coverage rate
4. **INVOICE vs CONTRACT** — Never mixed in the same revenue line
5. **Gross vs Net** — Extracted independently, never calculated from each other
6. **Revenue attribution** — By flight date (when spots aired), not by invoice date or upload timestamp
7. **Operator mapping** — From versioned CSV, never inferred from call sign patterns

## Output

Excel workbook with 5 tabs:
1. **Operator Summary** — Quarterly revenue by operator (gross/net), document counts, coverage rate
2. **DMA Detail** — DMA-level breakout with race competitiveness overlay
3. **Weekly Velocity** — Filing velocity time series (leading indicator)
4. **Cycle Comparison** — 2022 vs 2024 vs 2026 normalized by week-of-cycle
5. **Raw Data** — Full extraction-level detail for audit trail

## Methodology Limitations

- **Coverage is incomplete** — Not every station files promptly; some documents resist OCR
- **Document diversity** — FCC filings range from clean digital PDFs to scanned fax images
- **Timing lag** — Stations have up to 10 business days to file political ad documents
- **Gross/net ambiguity** — Many documents show only one amount without labeling it
- **Revenue ≠ bookings** — Contracts reflect committed spend that may be revised or cancelled

## Technology Stack

| Component | Library |
|-----------|---------|
| PDF extraction | PyMuPDF |
| OCR | PaddleOCR + pytesseract |
| Web crawling | httpx + Playwright |
| LLM extraction | Anthropic Claude (structured outputs) |
| Excel output | XlsxWriter |
| CLI | Typer + Rich |
| Database | SQLite (WAL mode) |
| Config | pydantic-settings |

## License

MIT
