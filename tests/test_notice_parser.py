"""
Tests for notice_parser.parse_api_record - mapping API records to notice dicts.

Run with: pytest tests/test_notice_parser.py
"""

import json
from pathlib import Path
from datetime import datetime

from notice_parser import parse_api_record

FIXTURE = Path(__file__).parent / "fixtures" / "site_info_page_0303_city.json"


def _first_record():
    with open(FIXTURE, encoding="utf-8") as f:
        data = json.load(f)
    return data["data"]["data"]["records"][0]


class TestParseApiRecord:
    """Map one API record to the internal notice dict."""

    def test_subject_becomes_title(self):
        rec = {"subject": "某项目造价咨询服务采购公告", "id": "abc", "pdate": "2026-07-10T21:34:23.000+08:00"}
        result = parse_api_record(rec)
        assert result["title"] == "某项目造价咨询服务采购公告"

    def test_builds_read_link_from_id(self):
        rec = {"subject": "标题", "id": "ENC123", "pdate": "2026-07-10T00:00:00.000+08:00"}
        result = parse_api_record(rec)
        assert result["link"] == "http://zfcg.qingdao.gov.cn/qdsite/#/read?id=ENC123"

    def test_extracts_date_from_iso_pdate(self):
        rec = {"subject": "标题", "id": "x", "pdate": "2026-07-10T21:34:23.000+08:00"}
        result = parse_api_record(rec)
        assert result["publish_date"] == "2026-07-10"

    def test_empty_pdate_gives_empty_date(self):
        rec = {"subject": "标题", "id": "x", "pdate": ""}
        result = parse_api_record(rec)
        assert result["publish_date"] == ""

    def test_returns_none_for_empty_subject(self):
        assert parse_api_record({"subject": "", "id": "x"}) is None
        assert parse_api_record({"subject": None, "id": "x"}) is None
        assert parse_api_record({}) is None

    def test_handles_missing_id(self):
        rec = {"subject": "标题", "pdate": "2026-07-10T00:00:00.000+08:00"}
        result = parse_api_record(rec)
        assert result["title"] == "标题"
        assert result["link"] == ""

    def test_includes_crawl_time(self):
        rec = {"subject": "标题", "id": "x", "pdate": "2026-07-10T00:00:00.000+08:00"}
        result = parse_api_record(rec)
        assert "crawl_time" in result
        # valid datetime format
        datetime.strptime(result["crawl_time"], "%Y-%m-%d %H:%M:%S")

    def test_parses_real_fixture_record(self):
        result = parse_api_record(_first_record())
        assert result is not None
        assert result["title"]  # non-empty
        assert result["link"].startswith("http://zfcg.qingdao.gov.cn/qdsite/#/read?id=")
        assert len(result["publish_date"]) == 10
