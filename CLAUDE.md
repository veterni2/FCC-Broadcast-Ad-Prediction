# FCC-Broadcast-Ad-Prediction

## Project Overview

Political Ad Revenue Estimator for broadcast TV credits. Scrapes FCC Online Public Inspection File (OPIF) political files for Gray TV, Nexstar, Scripps, Sinclair, and Tegna; extracts dollar amounts from PDFs using OCR and LLM; aggregates into a quarterly Excel model for buy-side credit research.

---

## FCC API Reality (Critical for Future Developers)

The FCC OPIF site (`publicfiles.fcc.gov`) does NOT have a usable JSON API for folder traversal. The following endpoints all return 404 and should not be attempted:

- `GET /api/service/political/filing/search`
- `GET /api/manager/folder/children/{uuid}`
- `GET /api/manager/folder/{uuid}`
- `GET /api/service/facility/search`

The site is server-rendered HTML with jQuery DataTables. **Playwright is required** for document discovery — there is no alternative.

The ONLY working API endpoint is PDF download:

```
GET https://publicfiles.fcc.gov/api/manager/download/{folder_uuid}/{file_uuid}.pdf
```

### URL Path as Source of Truth

The URL path itself carries race metadata — do not try to parse this from document content:

```
/tv-profile/{callsign}/political-files/{year}/{year_uuid}/federal/us-senate/{candidate-slug}/invoices/{folder_uuid}/
```

Fields parsed from path: callsign, year, race level (federal/state/local), office (us-senate, us-house, etc.), candidate slug, folder UUID.

---

## Architecture

Five-stage pipeline connected by a SQLite document registry.

```
src/fcc_ad_tracker/
├── stage1_stations/     # CSV -> DB station enumeration
├── stage2_crawler/      # Playwright HTML scraper
│   ├── browser.py       #   Core browser automation (page traversal, link extraction)
│   └── crawler.py       #   Orchestrator (concurrency, DB writes, retry logic)
├── stage3_pdf/          # httpx download + PyMuPDF text extraction + PaddleOCR/tesseract
├── stage4_llm/          # Claude tool-use extraction
│   ├── schemas.py       #   Pydantic models defining the extraction schema
│   ├── client.py        #   Anthropic API wrapper
│   └── extractor.py     #   Document -> structured record logic
├── stage5_model/        # Revenue aggregation + XlsxWriter 5-tab workbook
├── config/settings.py   # pydantic-settings env config
├── core/db.py           # SQLite WAL manager (6 tables)
└── utils/               # rate_limiter, dates, logging
```

### Stage Summary

| Stage | Input | Output | Key Lib |
|-------|-------|--------|---------|
| 1 - Stations | operator_stations.csv | stations table | csv, sqlite |
| 2 - Crawler | Station list | documents table (URLs) | Playwright |
| 3 - PDF | Document URLs | Raw text + extracted fields | httpx, PyMuPDF, PaddleOCR |
| 4 - LLM | Raw text | Structured ad records | anthropic SDK |
| 5 - Model | Ad records | Excel workbook | XlsxWriter |

---

## Commands

All 7 CLI commands:

```bash
# Install
pip install -e ".[dev]"               # Core + dev deps
pip install -e ".[ocr,dev]"           # Core + OCR + dev deps

# Full pipeline
fcc-tracker run --operators gray --year 2026 --update

# Individual stages
fcc-tracker crawl --operators gray --year 2026           # Stage 2: discover documents
fcc-tracker download --operators gray --year 2024 --limit 100  # Stage 3a: download PDFs
fcc-tracker extract --operators gray --year 2024 --limit 50    # Stages 3b+4: OCR + LLM
fcc-tracker model --operators gray --year 2024           # Stage 5: build Excel workbook

# Utilities
fcc-tracker status                                       # Pipeline dashboard (all operators)
fcc-tracker validate --operators gray --year 2024        # Compare extractions vs 10-K actuals
```

Valid `--operators` values: `gray`, `nexstar`, `scripps`, `sinclair`, `tegna` (comma-separated for multiple).

---

## Data Integrity Rules

1. `amount=NULL` if not extractable with high confidence — never estimate or fill in
2. No extrapolation from partial coverage — if documents are missing, coverage is shown as-is
3. Coverage metrics (documents found / extracted / failed) shown on every output tab
4. No smoothing or interpolation of time series
5. INVOICE and CONTRACT dollar amounts never mixed — tracked as separate record types
6. Gross and Net extracted independently — never calculate one from the other
7. Race metadata sourced from folder path (primary); LLM-extracted race metadata flagged as fallback and reviewed separately

---

## Development Notes

### Rate Limiting
- Hard limit: **1.5 requests/second** to `publicfiles.fcc.gov`
- Do not increase without monitoring for HTTP 429 responses
- Rate limiter is in `utils/rate_limiter.py` and applied in `stage2_crawler/browser.py`

### OCR Dependencies
- OCR extras are optional: `pip install -e ".[ocr,dev]"`
- PaddleOCR on Windows requires **Visual C++ Build Tools** — install from the Visual Studio installer before `pip install paddleocr`
- Tesseract fallback requires the Tesseract binary on PATH (from tesseract-ocr installer)
- If neither OCR backend is installed, scanned PDFs produce empty text and are flagged `ocr_status=skipped`

### Database
- SQLite WAL mode allows concurrent reads during an active crawl
- Default path: `data/documents.db` (overridden via `DB_PATH` env var or `settings.db_path`)
- 6 tables: `stations`, `documents`, `downloads`, `extractions`, `ad_records`, `validation_runs`
- Schema migrations are manual — check `core/db.py` `_create_tables()` when adding columns

### LLM Extraction
- Uses Claude tool use with `tool_choice={"type": "tool"}` for **guaranteed schema-compliant JSON**
- Do NOT switch to raw text parsing — the tool-use constraint is intentional to prevent hallucinated field names
- Extraction schema defined in `stage4_llm/schemas.py` as Pydantic models
- Failed extractions (API errors, schema violations) are recorded with `status=failed`; the document is retried on the next `extract` run up to 3 times

### Excel Output
- Stage 5 produces a 5-tab XlsxWriter workbook in `output/`
- None values are displayed as `--` in the workbook — zeros are never imputed for missing data
- Tab structure: Summary, By Operator, By DMA, By Race, Coverage

### Environment Variables
- `ANTHROPIC_API_KEY` — required for stage 4
- `DB_PATH` — override SQLite path (default: `data/documents.db`)
- `LOG_LEVEL` — default `INFO`; set to `DEBUG` for Playwright browser logs
- `FCC_RATE_LIMIT` — requests/sec float (default `1.5`)

---

## Testing

```bash
pytest tests/ -v          # Run all 73 tests
pytest tests/ -v -k cli   # CLI tests only
```

- 73 tests across all stages
- No live network calls — all HTTP and Playwright interactions are mocked
- Playwright tests require `playwright install chromium` (runs against recorded fixtures, not live FCC)
- Test fixtures in `tests/fixtures/` include sample HTML pages and PDF files

---

## File Layout

```
FCC-Broadcast-Ad-Prediction/
├── src/fcc_ad_tracker/      # Main package (see Architecture above)
├── static/
│   └── operator_stations.csv  # Versioned station-to-operator mapping (edit to add stations)
├── tests/                   # Pytest suite
├── data/                    # Runtime DB and downloads (git-ignored)
├── output/                  # Generated Excel/CSV files (git-ignored)
├── pyproject.toml           # Package config, deps, optional extras
└── CLAUDE.md                # This file
```

`static/operator_stations.csv` is the authoritative source for which call signs belong to which operator. Never infer operator from call sign patterns — always use this file.
