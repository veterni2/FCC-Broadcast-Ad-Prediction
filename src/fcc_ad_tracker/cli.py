"""CLI entrypoint for the FCC Political Ad Revenue Estimator.

Commands map to the five pipeline stages, plus status/validate utilities.
Each command filters by operator and year, supporting incremental updates.

Usage:
    fcc-tracker run --operators gray --year 2026 --update
    fcc-tracker crawl --operators gray --year 2026
    fcc-tracker download --operators gray --year 2026 --limit 100
    fcc-tracker extract --operators gray --year 2026 --limit 50
    fcc-tracker model --operators gray --year 2026
    fcc-tracker status
    fcc-tracker validate --operators gray --year 2024
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from .config.settings import get_settings
from .core.db import DatabaseManager
from .utils.logging import setup_logging

app = typer.Typer(
    name="fcc-tracker",
    help="Political Ad Revenue Estimator for Broadcast TV Credits using FCC OPIF data.",
    no_args_is_help=True,
    rich_markup_mode="rich",
)
console = Console()


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _validate_operators(operators: list[str]) -> list[str]:
    """Validate operator names against known operators."""
    known = get_settings().known_operators
    for op in operators:
        if op.lower() not in known:
            console.print(
                f"[yellow]Warning: '{op}' is not a known operator. "
                f"Known operators: {', '.join(known)}[/yellow]"
            )
    return [op.lower() for op in operators]


def _run_async(coro):  # type: ignore[no-untyped-def]
    """Run an async coroutine from a synchronous Typer command."""
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


@app.command()
def run(
    operators: list[str] = typer.Option(
        ["gray"], "--operators", "-o",
        help="Operator names to process (e.g., gray, nexstar).",
    ),
    year: int = typer.Option(
        2026, "--year", "-y",
        help="Campaign year to process.",
    ),
    update: bool = typer.Option(
        False, "--update", "-u",
        help="Incremental mode: only process new documents since last run.",
    ),
    top_dma: Optional[int] = typer.Option(
        None, "--top-dma",
        help="Only process stations in the top N DMAs.",
    ),
    limit: Optional[int] = typer.Option(
        None, "--limit", "-l",
        help="Maximum number of documents to process per stage.",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run",
        help="Show what would be done without making API calls.",
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v",
        help="Enable debug logging.",
    ),
) -> None:
    """Run the full pipeline: crawl -> download -> extract -> model."""
    setup_logging(verbose=verbose)
    operators = _validate_operators(operators)

    db = DatabaseManager()
    db.initialize()

    console.print(f"\n[bold green]FCC Political Ad Tracker — Full Pipeline[/bold green]")
    console.print(f"Operators: {', '.join(operators)}")
    console.print(f"Year: {year}")
    console.print(f"Mode: {'incremental' if update else 'full'}")
    if top_dma:
        console.print(f"DMA filter: top {top_dma}")
    if limit:
        console.print(f"Document limit: {limit}")
    if dry_run:
        console.print("[yellow]DRY RUN — no changes will be made[/yellow]")
        return

    # Stage 1: Station enumeration
    from .stage1_stations.enumerator import enumerate_stations
    console.print("\n[cyan]Stage 1:[/cyan] Enumerating stations...")
    stations = enumerate_stations(db=db, operators=operators, top_dma=top_dma)
    console.print(f"  [green]✓[/green] {len(stations)} stations registered")

    if not stations:
        console.print("[red]No stations found — aborting.[/red]")
        raise typer.Exit(1)

    # Stage 2: Crawl
    from .stage2_crawler.crawler import crawl_stations
    console.print("\n[cyan]Stage 2:[/cyan] Crawling FCC OPIF...")
    crawl_stats = _run_async(
        crawl_stations(
            db=db,
            stations=stations,
            year=year,
            operators_str=", ".join(operators),
            incremental=update,
        )
    )
    console.print(
        f"  [green]✓[/green] {crawl_stats['new_docs']} new docs, "
        f"{crawl_stats['skipped_existing']} existing, "
        f"{crawl_stats['errors']} errors"
    )

    # Stage 3a: Download PDFs
    from .stage3_pdf.downloader import download_documents
    console.print("\n[cyan]Stage 3a:[/cyan] Downloading PDFs...")
    dl_stats = _run_async(
        download_documents(
            db=db,
            operator=operators[0] if len(operators) == 1 else None,
            year=year,
            limit=limit,
        )
    )
    console.print(
        f"  [green]✓[/green] {dl_stats['downloaded']} downloaded, "
        f"{dl_stats['skipped']} skipped, "
        f"{dl_stats['failed']} failed"
    )

    # Stage 3b: Text extraction (PDF/OCR)
    from .stage3_pdf.pipeline import run_pdf_pipeline
    console.print("\n[cyan]Stage 3b:[/cyan] Extracting text from PDFs...")
    ocr_stats = _run_async(
        run_pdf_pipeline(
            db=db,
            operator=operators[0] if len(operators) == 1 else None,
            year=year,
            limit=limit,
        )
    )
    console.print(
        f"  [green]✓[/green] {ocr_stats['success']} extracted "
        f"({ocr_stats['ocr_used']} via OCR), {ocr_stats['failed']} failed"
    )

    # Stage 4: LLM extraction
    from .stage4_llm.extractor import run_llm_extraction
    console.print("\n[cyan]Stage 4:[/cyan] LLM extraction...")
    llm_stats = _run_async(
        run_llm_extraction(
            db=db,
            operator=operators[0] if len(operators) == 1 else None,
            year=year,
            limit=limit,
        )
    )
    console.print(
        f"  [green]✓[/green] {llm_stats.get('success', 0)} extracted, "
        f"{llm_stats.get('failed', 0)} failed, "
        f"${llm_stats.get('total_cost_usd', 0.0):.2f} API cost"
    )

    # Stage 5: Financial model
    from .stage5_model.aggregator import aggregate_revenue
    from .stage5_model.velocity import compute_filing_velocity
    from .stage5_model.cycle_compare import compare_cycles
    from .stage5_model.coverage import compute_coverage
    from .stage5_model.excel_writer import generate_workbook

    console.print("\n[cyan]Stage 5:[/cyan] Building financial model...")
    op_filter = operators[0] if len(operators) == 1 else None
    agg = aggregate_revenue(db=db, operator=op_filter, year=year)
    velocity_data = compute_filing_velocity(db=db, operator=op_filter, year=year)
    cycle_data = compare_cycles(db=db, operator=op_filter, cycles=[2022, 2024, 2026])
    cov = compute_coverage(db=db, operator=op_filter, year=year)
    raw_data = db.get_extractions_for_model(operator=op_filter, year=year)

    workbook_path = generate_workbook(
        operator_summary=agg["by_operator_quarter"],
        dma_detail=agg["by_dma"],
        velocity_data=velocity_data,
        cycle_comparison=cycle_data,
        raw_data=raw_data,
        coverage_stats=cov,
        operators=operators,
        year=year,
    )
    console.print(f"  [green]✓[/green] Workbook: {workbook_path}")

    console.print("\n[bold green]Pipeline complete.[/bold green]")
    _print_status(db)


@app.command()
def crawl(
    operators: list[str] = typer.Option(
        ["gray"], "--operators", "-o",
        help="Operator names to crawl.",
    ),
    year: int = typer.Option(
        2026, "--year", "-y",
        help="Campaign year to crawl.",
    ),
    update: bool = typer.Option(
        False, "--update", "-u",
        help="Only crawl stations not checked recently.",
    ),
    top_dma: Optional[int] = typer.Option(
        None, "--top-dma",
        help="Only crawl stations in the top N DMAs.",
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v",
        help="Enable debug logging.",
    ),
) -> None:
    """Stage 2: Discover political ad documents on FCC OPIF."""
    setup_logging(verbose=verbose)
    operators = _validate_operators(operators)

    db = DatabaseManager()
    db.initialize()

    # Stage 1: enumerate stations first
    from .stage1_stations.enumerator import enumerate_stations
    stations = enumerate_stations(db=db, operators=operators, top_dma=top_dma)

    if not stations:
        console.print("[red]No stations found — check operator_stations.csv.[/red]")
        raise typer.Exit(1)

    console.print(
        f"\n[bold]Crawling FCC OPIF for {', '.join(operators)} — {year}[/bold]"
    )
    console.print(f"Stations: {len(stations)} | Mode: {'incremental' if update else 'full'}")

    from .stage2_crawler.crawler import crawl_stations
    stats = _run_async(
        crawl_stations(
            db=db,
            stations=stations,
            year=year,
            operators_str=", ".join(operators),
            incremental=update,
        )
    )

    console.print(f"\n[green]Crawl complete:[/green]")
    console.print(f"  New documents:      {stats['new_docs']}")
    console.print(f"  Already known:      {stats['skipped_existing']}")
    console.print(f"  Station errors:     {stats['errors']}")
    console.print(f"  Stations crawled:   {stats['total_stations']}")


@app.command()
def download(
    operators: list[str] = typer.Option(
        ["gray"], "--operators", "-o",
        help="Operator names to download PDFs for.",
    ),
    year: Optional[int] = typer.Option(
        None, "--year", "-y",
        help="Campaign year filter.",
    ),
    limit: Optional[int] = typer.Option(
        None, "--limit", "-l",
        help="Maximum number of PDFs to download.",
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v",
        help="Enable debug logging.",
    ),
) -> None:
    """Stage 3a: Download PDFs for discovered documents."""
    setup_logging(verbose=verbose)
    operators = _validate_operators(operators)

    db = DatabaseManager()
    db.initialize()

    console.print(
        f"\n[bold]Downloading PDFs for {', '.join(operators)}"
        + (f" — {year}" if year else "") + "[/bold]"
    )

    from .stage3_pdf.downloader import download_documents
    stats = _run_async(
        download_documents(
            db=db,
            operator=operators[0] if len(operators) == 1 else None,
            year=year,
            limit=limit,
        )
    )

    console.print(f"\n[green]Download complete:[/green]")
    console.print(f"  Downloaded:  {stats['downloaded']}")
    console.print(f"  Skipped:     {stats['skipped']}")
    console.print(f"  Failed:      {stats['failed']}")


@app.command()
def extract(
    operators: list[str] = typer.Option(
        ["gray"], "--operators", "-o",
        help="Operator names to extract data from.",
    ),
    year: Optional[int] = typer.Option(
        None, "--year", "-y",
        help="Campaign year filter.",
    ),
    limit: Optional[int] = typer.Option(
        None, "--limit", "-l",
        help="Maximum number of documents to extract.",
    ),
    force: bool = typer.Option(
        False, "--force",
        help="Re-extract already processed documents.",
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v",
        help="Enable debug logging.",
    ),
) -> None:
    """Stages 3b+4: Text extraction (OCR) then LLM structured extraction."""
    setup_logging(verbose=verbose)
    operators = _validate_operators(operators)

    db = DatabaseManager()
    db.initialize()

    console.print(
        f"\n[bold]Extracting data for {', '.join(operators)}"
        + (f" — {year}" if year else "") + "[/bold]"
    )

    # Stage 3b: PDF text extraction
    from .stage3_pdf.pipeline import run_pdf_pipeline
    console.print("[cyan]Stage 3b:[/cyan] PDF text extraction...")
    pdf_stats = _run_async(
        run_pdf_pipeline(
            db=db,
            operator=operators[0] if len(operators) == 1 else None,
            year=year,
            limit=limit,
        )
    )
    console.print(
        f"  {pdf_stats['success']} extracted ({pdf_stats['ocr_used']} via OCR), "
        f"{pdf_stats['failed']} failed"
    )

    # Stage 4: LLM extraction
    from .stage4_llm.extractor import run_llm_extraction
    console.print("[cyan]Stage 4:[/cyan] LLM extraction...")
    llm_stats = _run_async(
        run_llm_extraction(
            db=db,
            operator=operators[0] if len(operators) == 1 else None,
            year=year,
            limit=limit,
        )
    )
    console.print(
        f"  {llm_stats.get('success', 0)} extracted, "
        f"{llm_stats.get('failed', 0)} failed, "
        f"${llm_stats.get('total_cost_usd', 0.0):.4f} API cost"
    )


@app.command()
def model(
    operators: list[str] = typer.Option(
        ["gray"], "--operators", "-o",
        help="Operator names to include in the model.",
    ),
    year: Optional[int] = typer.Option(
        None, "--year", "-y",
        help="Campaign year filter.",
    ),
    output_file: Optional[Path] = typer.Option(
        None, "--output", "-O",
        help="Output Excel file path. Default: output/political_ad_model_{operators}_{year}.xlsx",
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v",
        help="Enable debug logging.",
    ),
) -> None:
    """Stage 5: Build financial model from extracted data."""
    setup_logging(verbose=verbose)
    operators = _validate_operators(operators)

    db = DatabaseManager()
    db.initialize()

    console.print(
        f"\n[bold]Building financial model for {', '.join(operators)}"
        + (f" — {year}" if year else "") + "[/bold]"
    )

    from .stage5_model.aggregator import aggregate_revenue
    from .stage5_model.velocity import compute_filing_velocity
    from .stage5_model.cycle_compare import compare_cycles
    from .stage5_model.coverage import compute_coverage
    from .stage5_model.excel_writer import generate_workbook

    op_filter = operators[0] if len(operators) == 1 else None

    console.print("[cyan]Computing revenue aggregations...[/cyan]")
    agg = aggregate_revenue(db=db, operator=op_filter, year=year)

    console.print("[cyan]Computing filing velocity...[/cyan]")
    velocity = compute_filing_velocity(db=db, operator=op_filter, year=year)

    console.print("[cyan]Computing cycle comparison...[/cyan]")
    cycle_data = compare_cycles(db=db, operator=op_filter, cycles=[2022, 2024, 2026])

    console.print("[cyan]Computing coverage metrics...[/cyan]")
    coverage = compute_coverage(db=db, operator=op_filter, year=year)

    # Fetch raw data for audit trail tab
    raw_data = db.get_extractions_for_model(operator=op_filter, year=year)

    console.print("[cyan]Writing Excel workbook...[/cyan]")
    workbook_path = generate_workbook(
        operator_summary=agg["by_operator_quarter"],
        dma_detail=agg["by_dma"],
        velocity_data=velocity,
        cycle_comparison=cycle_data,
        raw_data=raw_data,
        coverage_stats=coverage,
        output_path=output_file,
        operators=operators,
        year=year,
    )

    console.print(f"\n[bold green]Model complete:[/bold green]")
    console.print(f"  Workbook: {workbook_path}")
    console.print(f"  Operator rows:  {len(agg['by_operator_quarter'])}")
    console.print(f"  DMA rows:       {len(agg['by_dma'])}")
    console.print(f"  Velocity weeks: {len(velocity)}")
    console.print(f"  Extractions:    {len(raw_data)}")
    if coverage:
        total_cov = sum(c.coverage_rate for c in coverage) / len(coverage)
        console.print(f"  Avg coverage:   {total_cov:.1%}")


@app.command()
def status(
    verbose: bool = typer.Option(
        False, "--verbose", "-v",
        help="Enable debug logging.",
    ),
) -> None:
    """Show pipeline status dashboard."""
    setup_logging(verbose=verbose)

    db = DatabaseManager()
    try:
        db.initialize()
    except Exception as e:
        console.print(f"[red]Database error: {e}[/red]")
        raise typer.Exit(1)

    _print_status(db)


def _print_status(db: DatabaseManager) -> None:
    """Print the pipeline status table."""
    try:
        stats = db.get_pipeline_status()
    except Exception as e:
        console.print(f"[red]Failed to read pipeline status: {e}[/red]")
        return

    console.print("\n[bold green]FCC Political Ad Tracker — Pipeline Status[/bold green]\n")

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Stage", style="dim")
    table.add_column("Metric")
    table.add_column("Count", justify="right")

    table.add_row("Stations", "Registered stations", str(stats["stations"]))
    table.add_row("Crawl", "Documents discovered", str(stats["total_documents"]))
    table.add_row("Download", "PDFs downloaded", str(stats["pdfs_downloaded"]))
    table.add_row("Text", "Text extracted", str(stats["text_extracted"]))
    table.add_row("LLM", "LLM processed", str(stats["llm_processed"]))
    table.add_row("", "— Successful", str(stats["extraction_success"]))
    table.add_row("", "— Failed", str(stats["extraction_failed"]))
    table.add_row("Cost", "Total API cost", f"${stats['total_cost_usd']:.4f}")

    console.print(table)

    if stats["total_documents"] > 0:
        coverage = stats["extraction_success"] / stats["total_documents"] * 100
        console.print(
            f"\n[dim]Coverage rate: {coverage:.1f}% of discovered documents[/dim]"
        )


@app.command()
def validate(
    operators: list[str] = typer.Option(
        ["gray"], "--operators", "-o",
        help="Operator names to validate.",
    ),
    year: int = typer.Option(
        2024, "--year", "-y",
        help="Year to validate (should have known actuals).",
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v",
        help="Enable debug logging.",
    ),
) -> None:
    """Compare extracted totals to reported actuals for accuracy assessment."""
    setup_logging(verbose=verbose)
    operators = _validate_operators(operators)

    db = DatabaseManager()
    db.initialize()

    console.print(f"\n[bold]Validating {', '.join(operators)} — {year}[/bold]")

    for operator in operators:
        extractions = db.get_extractions_for_model(
            operator=operator,
            year=year,
            document_type="INVOICE",
        )

        if not extractions:
            console.print(
                f"[yellow]{operator}:[/yellow] No invoice extractions found for {year}."
            )
            continue

        total_gross = sum(
            e.get("gross_amount") or 0.0
            for e in extractions
            if e.get("gross_amount") is not None
        )
        total_net = sum(
            e.get("net_amount") or 0.0
            for e in extractions
            if e.get("net_amount") is not None
        )

        console.print(
            f"\n[bold]{operator.title()}[/bold] — {year} Invoice Extractions"
        )
        console.print(f"  Docs with successful extraction: {len(extractions)}")
        console.print(f"  Total gross amount:  ${total_gross:,.0f}")
        console.print(f"  Total net amount:    ${total_net:,.0f}")
        console.print(
            "[dim]Compare to 10-K reported political ad revenue to assess coverage.[/dim]"
        )


if __name__ == "__main__":
    app()
