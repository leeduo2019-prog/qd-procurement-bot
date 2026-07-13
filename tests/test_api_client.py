"""
Tests for api_client - the direct backend API wrapper.

Run with: pytest tests/test_api_client.py
"""

import json
import os
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import requests

import api_client

FIXTURE = Path(__file__).parent / "fixtures" / "site_info_page_0303_city.json"


def _load_fixture():
    with open(FIXTURE, encoding="utf-8") as f:
        return json.load(f)


def _make_response(json_data, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    return resp


class TestFetchPageRequest:
    """Verify the HTTP request is constructed correctly."""

    @patch("api_client.requests.post")
    def test_posts_to_correct_endpoint(self, mock_post):
        mock_post.return_value = _make_response(_load_fixture())
        api_client.fetch_page("0303", "city")
        args, kwargs = mock_post.call_args
        url = args[0] if args else kwargs.get("url")
        assert url == f"{api_client.API_BASE}{api_client.PAGE_ENDPOINT}"
        assert kwargs.get("timeout") == api_client.DEFAULT_TIMEOUT

    @patch("api_client.requests.post")
    def test_payload_has_colcode_and_areatype(self, mock_post):
        mock_post.return_value = _make_response(_load_fixture())
        api_client.fetch_page("0303", "city", area=None, page=1, limit=15)
        payload = mock_post.call_args.kwargs["json"]
        assert payload["colCode"] == "0303"
        assert payload["areaType"] == "city"
        assert payload["page"] == 1
        assert payload["sort"] == "-pdate"
        assert payload["area"] is None

    @patch("api_client.requests.post")
    def test_payload_passes_area_for_district(self, mock_post):
        mock_post.return_value = _make_response(_load_fixture())
        api_client.fetch_page("0303", "county", area="370202", page=2)
        payload = mock_post.call_args.kwargs["json"]
        assert payload["areaType"] == "county"
        assert payload["area"] == "370202"
        assert payload["page"] == 2

    @patch("api_client.requests.post")
    def test_limit_clamped_to_max(self, mock_post):
        mock_post.return_value = _make_response(_load_fixture())
        api_client.fetch_page("0303", "city", limit=50)
        payload = mock_post.call_args.kwargs["json"]
        assert payload["limit"] == api_client.MAX_LIMIT


class TestFetchPageResponse:
    """Verify response parsing."""

    @patch("api_client.requests.post")
    def test_returns_records_and_total(self, mock_post):
        mock_post.return_value = _make_response(_load_fixture())
        result = api_client.fetch_page("0303", "city")
        assert result["total"] == 12307
        assert len(result["records"]) == 15
        first = result["records"][0]
        assert "subject" in first
        assert "id" in first
        assert "pdate" in first

    @patch("api_client.requests.post")
    def test_empty_records_handled(self, mock_post):
        empty = {"data": {"code": 100, "data": {"records": [], "total": 0}}}
        mock_post.return_value = _make_response(empty)
        result = api_client.fetch_page("0303", "city")
        assert result["records"] == []
        assert result["total"] == 0


class TestFetchPageErrors:
    """Verify error handling."""

    @patch("api_client.requests.post")
    def test_raises_on_non_success_code(self, mock_post):
        bad = {"data": {"code": 500, "message": "boom", "data": None}}
        mock_post.return_value = _make_response(bad)
        with pytest.raises(RuntimeError):
            api_client.fetch_page("0303", "city")

    @patch("api_client.requests.post")
    def test_propagates_http_error(self, mock_post):
        mock_post.side_effect = requests.exceptions.RequestException("network down")
        with pytest.raises(requests.exceptions.RequestException):
            api_client.fetch_page("0303", "city")
