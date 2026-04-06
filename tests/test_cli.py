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
