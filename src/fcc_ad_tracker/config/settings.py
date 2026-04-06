"""Application settings using pydantic-settings.

All configuration is driven by environment variables (with FCC_ prefix)
or a .env file in the project root.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _find_project_root() -> Path:
    """Walk up from CWD to find the project root (contains pyproject.toml)."""
    current = Path.cwd()
    for parent in [current, *current.parents]:
        if (parent / "pyproject.toml").exists():
            return parent
    return current


PROJECT_ROOT = _find_project_root()


class FCCAPISettings(BaseSettings):
    """FCC OPIF API connection settings."""

    model_config = SettingsConfigDict(env_prefix="FCC_", env_file=".env", extra="ignore")

    base_url: str = "https://publicfiles.fcc.gov"
    search_url: str = "https://publicfiles.fcc.gov/api/service/political/filing/search"
    rate_limit_rps: float = 1.5
    max_concurrent: int = 3


class LLMSettings(BaseSettings):
    """Anthropic Claude API settings for document extraction."""

    model_config = SettingsConfigDict(env_prefix="FCC_LLM_", env_file=".env", extra="ignore")

    api_key: SecretStr = SecretStr("")
    model: str = "claude-sonnet-4-20250514"
    max_tokens: int = 4096
    temperature: float = 0.0
    cost_budget_per_run: float = 5.00

    @field_validator("api_key", mode="before")
    @classmethod
    def _api_key_from_env(cls, v: str) -> str:
        """Fall back to ANTHROPIC_API_KEY if FCC_LLM_API_KEY is not set."""
        if not v:
            return os.environ.get("ANTHROPIC_API_KEY", "")
        return v


class PDFSettings(BaseSettings):
    """PDF processing and OCR settings."""

    model_config = SettingsConfigDict(env_prefix="FCC_PDF_", env_file=".env", extra="ignore")

    storage_dir: Path = PROJECT_ROOT / "data" / "pdfs"
    ocr_engine: Literal["paddleocr", "tesseract"] = "paddleocr"
    min_text_chars_per_page: int = 50
    dpi: int = 300


class DBSettings(BaseSettings):
    """SQLite database settings."""

    model_config = SettingsConfigDict(env_prefix="FCC_", env_file=".env", extra="ignore")

    db_path: Path = PROJECT_ROOT / "data" / "documents.db"


class OutputSettings(BaseSettings):
    """Output file settings."""

    model_config = SettingsConfigDict(env_prefix="FCC_", env_file=".env", extra="ignore")

    output_dir: Path = PROJECT_ROOT / "output"


class Settings(BaseSettings):
    """Root settings composing all sub-settings."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    fcc: FCCAPISettings = FCCAPISettings()
    llm: LLMSettings = LLMSettings()
    pdf: PDFSettings = PDFSettings()
    db: DBSettings = DBSettings()
    output: OutputSettings = OutputSettings()

    # Operator definitions for CLI validation
    known_operators: list[str] = [
        "gray",
        "nexstar",
        "scripps",
        "sinclair",
        "tegna",
    ]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Get cached singleton settings instance."""
    return Settings()
