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
# Shared options
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
    log = setup_logging(verbose=verbose)
    operators = _validate_operators(operators)

    db = DatabaseManager()
    db.initialize()

    console.print(f"\n[bold green]FCC Political Ad Tracker[/bold green]")
    console.print(f"Operators: {', '.join(operators)}")
    console.print(f"Year: {year}")
    console.print(f"Mode: {'incremental' if update else 'full'}")
    if top_dma:
        console.print(f"DMA filter: top {top_dma}")
    if limit:
        console.print(f"Document limit: {limit}")
    if dry_run:
        console.print("[yellow]DRY RUN — no changes will be made[/yellow]")

    console.print("\n[dim]Pipeline stages:[/dim]")
    console.print("  1. Station enumeration — [yellow]not yet implemented[/yellow]")
    console.print("  2. Political file crawl — [yellow]not yet implemented[/yellow]")
    console.print("  3. PDF download + OCR — [yellow]not yet implemented[/yellow]")
    console.print("  4. LLM extraction — [yellow]not yet implemented[/yellow]")
    console.print("  5. Financial model — [yellow]not yet implemented[/yellow]")


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
    log = setup_logging(verbose=verbose)
    operators = _validate_operators(operators)

    db = DatabaseManager()
    db.initialize()

    console.print(f"\n[bold]Crawling FCC OPIF for {', '.join(operators)} — {year}[/bold]")
    console.print("[yellow]Stage 2 crawler not yet implemented.[/yellow]")


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
    log = setup_logging(verbose=verbose)
    operators = _validate_operators(operators)

    db = DatabaseManager()
    db.initialize()

    console.print(f"\n[bold]Downloading PDFs for {', '.join(operators)}[/bold]")
    console.print("[yellow]Stage 3 downloader not yet implemented.[/yellow]")


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
    log = setup_logging(verbose=verbose)
    operators = _validate_operators(operators)

    db = DatabaseManager()
    db.initialize()

    console.print(f"\n[bold]Extracting data for {', '.join(operators)}[/bold]")
    console.print("[yellow]Stages 3-4 extraction not yet implemented.[/yellow]")


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
    log = setup_logging(verbose=verbose)
    operators = _validate_operators(operators)

    db = DatabaseManager()
    db.initialize()

    console.print(f"\n[bold]Building financial model for {', '.join(operators)}[/bold]")
    console.print("[yellow]Stage 5 model not yet implemented.[/yellow]")


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
        stats = db.get_pipeline_status()
    except Exception as e:
        console.print(f"[red]Database error: {e}[/red]")
        console.print("[dim]Run a pipeline command first to initialize the database.[/dim]")
        raise typer.Exit(1)

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
    table.add_row("Cost", "Total API cost", f"${stats['total_cost_usd']:.2f}")

    console.print(table)

    if stats["total_documents"] > 0:
        coverage = stats["extraction_success"] / stats["total_documents"] * 100
        console.print(f"\n[dim]Coverage rate: {coverage:.1f}% of discovered documents[/dim]")


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

    console.print(f"\n[bold]Validating {', '.join(operators)} — {year}[/bold]")
    console.print("[yellow]Validation not yet implemented.[/yellow]")
    console.print("[dim]Requires extracted data and known actuals from 10-K filings.[/dim]")


if __name__ == "__main__":
    app()
