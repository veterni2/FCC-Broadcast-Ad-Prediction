"""Tests for Stage 2: Political file crawler — path metadata parsing."""

from __future__ import annotations

import pytest

from fcc_ad_tracker.stage2_crawler.browser import (
    _is_uuid,
    _parse_file_size,
    parse_path_metadata,
)


# ---------------------------------------------------------------------------
# _is_uuid
# ---------------------------------------------------------------------------


class TestIsUuid:
    def test_valid_uuid_lowercase(self):
        assert _is_uuid("aabbccdd-1122-3344-5566-778899aabbcc")

    def test_valid_uuid_uppercase(self):
        assert _is_uuid("AABBCCDD-1122-3344-5566-778899AABBCC")

    def test_valid_uuid_mixed_case(self):
        assert _is_uuid("a0b1c2d3-E4F5-6789-abcd-ef0123456789")

    def test_invalid_too_short(self):
        assert not _is_uuid("abc123")

    def test_invalid_empty(self):
        assert not _is_uuid("")

    def test_invalid_word(self):
        assert not _is_uuid("federal")

    def test_invalid_year(self):
        assert not _is_uuid("2024")

    def test_invalid_no_hyphens(self):
        assert not _is_uuid("aabbccdd11223344556677889900aabb")


# ---------------------------------------------------------------------------
# _parse_file_size
# ---------------------------------------------------------------------------


class TestParseFileSize:
    def test_kilobytes(self):
        assert _parse_file_size("125 KB") == 128_000

    def test_megabytes(self):
        result = _parse_file_size("2.3 MB")
        assert 2_300_000 < result < 2_500_000  # 2.3 * 1024^2 ≈ 2,411,724

    def test_bytes_unit(self):
        assert _parse_file_size("512 B") == 512

    def test_no_unit_treated_as_bytes(self):
        assert _parse_file_size("1024") == 1024

    def test_empty_string(self):
        assert _parse_file_size("") is None

    def test_invalid_string(self):
        assert _parse_file_size("unknown") is None

    def test_gigabytes(self):
        result = _parse_file_size("1 GB")
        assert result == 1_073_741_824

    def test_lowercase_unit(self):
        result = _parse_file_size("500 kb")
        assert result == 512_000


# ---------------------------------------------------------------------------
# parse_path_metadata — full URL forms
# ---------------------------------------------------------------------------


CALLSIGN = "WFAA"
BASE = f"https://publicfiles.fcc.gov/tv-profile/{CALLSIGN}/political-files"
YEAR_UUID = "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"
FOLDER_UUID = "cccccccc-4444-5555-6666-dddddddddddd"


class TestParsePathMetadata:
    # --- Root level ---

    def test_root_url_returns_empty_meta(self):
        meta = parse_path_metadata(BASE, CALLSIGN)
        assert meta.get("year") is None

    # --- Year only ---

    def test_year_only(self):
        url = f"{BASE}/2024"
        meta = parse_path_metadata(url, CALLSIGN)
        assert meta["year"] == 2024

    # --- Year + year UUID ---

    def test_year_with_uuid(self):
        url = f"{BASE}/2024/{YEAR_UUID}"
        meta = parse_path_metadata(url, CALLSIGN)
        assert meta["year"] == 2024
        assert meta["year_uuid"] == YEAR_UUID

    # --- federal / us-senate / candidate / invoices / folder_uuid ---

    def test_full_invoice_path(self):
        url = (
            f"{BASE}/2024/{YEAR_UUID}/federal/us-senate"
            f"/ted-cruz/invoices/{FOLDER_UUID}"
        )
        meta = parse_path_metadata(url, CALLSIGN)
        assert meta["year"] == 2024
        assert meta["race_level"] == "federal"
        assert meta["office_type"] == "us-senate"
        assert meta["candidate_slug"] == "ted-cruz"
        assert meta["doc_type"] == "invoices"
        assert meta["folder_uuid"] == FOLDER_UUID

    def test_full_contract_path(self):
        url = (
            f"{BASE}/2026/{YEAR_UUID}/federal/us-house"
            f"/jane-smith/contracts/{FOLDER_UUID}"
        )
        meta = parse_path_metadata(url, CALLSIGN)
        assert meta["race_level"] == "federal"
        assert meta["office_type"] == "us-house"
        assert meta["candidate_slug"] == "jane-smith"
        assert meta["doc_type"] == "contracts"
        assert meta["folder_uuid"] == FOLDER_UUID

    def test_state_governor_path(self):
        url = (
            f"{BASE}/2024/{YEAR_UUID}/state/governor"
            f"/greg-abbott/invoices/{FOLDER_UUID}"
        )
        meta = parse_path_metadata(url, CALLSIGN)
        assert meta["race_level"] == "state"
        assert meta["office_type"] == "governor"
        assert meta["candidate_slug"] == "greg-abbott"
        assert meta["doc_type"] == "invoices"

    def test_non_candidate_issue_ads(self):
        url = f"{BASE}/2024/{YEAR_UUID}/non-candidate-issue-ads/{FOLDER_UUID}"
        meta = parse_path_metadata(url, CALLSIGN)
        assert meta["race_level"] == "non-candidate-issue-ads"
        assert meta["folder_uuid"] == FOLDER_UUID
        assert meta["doc_type"] == "non-candidate"
        # office_type and candidate_slug should be None for non-candidate
        assert meta.get("office_type") is None
        assert meta.get("candidate_slug") is None

    def test_local_race(self):
        url = (
            f"{BASE}/2024/{YEAR_UUID}/local/mayor"
            f"/john-doe/invoices/{FOLDER_UUID}"
        )
        meta = parse_path_metadata(url, CALLSIGN)
        assert meta["race_level"] == "local"
        assert meta["office_type"] == "mayor"
        assert meta["candidate_slug"] == "john-doe"

    def test_path_without_year_uuid(self):
        """Some URL patterns skip the year UUID."""
        url = (
            f"{BASE}/2026/federal/us-senate/ted-cruz/invoices/{FOLDER_UUID}"
        )
        meta = parse_path_metadata(url, CALLSIGN)
        assert meta["year"] == 2026
        # Without year UUID, next segment (federal) is race_level
        assert meta["race_level"] == "federal"
        assert meta["doc_type"] == "invoices"

    def test_callsign_case_insensitive(self):
        """parse_path_metadata should match callsign case-insensitively."""
        url = (
            f"https://publicfiles.fcc.gov/tv-profile/wfaa/political-files"
            f"/2024/{YEAR_UUID}/federal/us-senate/ted-cruz/invoices/{FOLDER_UUID}"
        )
        meta = parse_path_metadata(url, CALLSIGN)
        assert meta["year"] == 2024
        assert meta["doc_type"] == "invoices"

    def test_unrelated_url_returns_empty(self):
        meta = parse_path_metadata("https://example.com/not-fcc", CALLSIGN)
        assert meta == {}

    def test_nab_doc_type(self):
        """'nab' is a valid (if rare) document type folder."""
        url = f"{BASE}/2024/{YEAR_UUID}/federal/us-senate/john-doe/nab/{FOLDER_UUID}"
        meta = parse_path_metadata(url, CALLSIGN)
        assert meta["doc_type"] == "nab"
