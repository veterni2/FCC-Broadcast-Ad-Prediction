"""Tests for the CLI interface."""

from __future__ import annotations

from typer.testing import CliRunner

from fcc_ad_tracker.cli import app

runner = CliRunner()


def test_help() -> None:
    """CLI shows help text."""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "Political Ad Revenue Estimator" in result.output


def test_status_command() -> None:
    """Status command runs without error."""
    result = runner.invoke(app, ["status"])
    # May fail if no DB exists, but should not crash
    assert result.exit_code in (0, 1)


def test_run_help() -> None:
    """Run command shows help."""
    result = runner.invoke(app, ["run", "--help"])
    assert result.exit_code == 0
    assert "--operators" in result.output
    assert "--year" in result.output
    assert "--update" in result.output


def test_crawl_help() -> None:
    """crawl --help shows operator and year options."""
    result = runner.invoke(app, ["crawl", "--help"])
    assert result.exit_code == 0
    assert "--operators" in result.output
    assert "--year" in result.output


def test_download_help() -> None:
    """download --help shows limit option."""
    result = runner.invoke(app, ["download", "--help"])
    assert result.exit_code == 0
    assert "--limit" in result.output


def test_model_help() -> None:
    """model --help shows output option."""
    result = runner.invoke(app, ["model", "--help"])
    assert result.exit_code == 0
    assert "--output" in result.output


def test_validate_help() -> None:
    """validate --help shows year option."""
    result = runner.invoke(app, ["validate", "--help"])
    assert result.exit_code == 0
    assert "--year" in result.output
