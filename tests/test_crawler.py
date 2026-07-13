"""
Tests for ProcurementCrawler.crawl orchestration (API-based, no Selenium).

Run with: pytest tests/test_crawler.py
"""

import os
from unittest.mock import patch
from datetime import datetime, timedelta

import pytest

from crawler import ProcurementCrawler
from store import NoticeStore


def _record(subject, pdate_iso, rid="x"):
    """Build a minimal API record matching the real shape."""
    return {
        "subject": subject,
        "id": rid,
        "pdate": pdate_iso,
        "area": "370200",
        "regionName": "市级",
    }


def _today_iso():
    return datetime.now().strftime("%Y-%m-%dT10:00:00.000+08:00")


class TestCrawl:
    """crawl() orchestration: pagination, cutoff, matching, dedup."""

    def _make_crawler(self, monkeypatch, tmp_path, area_type="all",
                      days_back=2, max_pages=5):
        # Redirect NoticeStore to a temp db so dedup is real but isolated.
        monkeypatch.setattr(
            "crawler.NoticeStore",
            lambda db_path=None: NoticeStore(db_path=str(tmp_path / "t.db")),
        )
        with patch.dict(os.environ, {"KEYWORDS": "造价,审计,预算,决算,结算"}):
            return ProcurementCrawler(
                area_type=area_type, days_back=days_back, max_pages=max_pages
            )

    def test_matches_keyword_and_stores(self, monkeypatch, tmp_path):
        c = self._make_crawler(monkeypatch, tmp_path, area_type="qingdao")
        monkeypatch.setattr(
            "crawler.fetch_page",
            lambda *a, **k: {
                "records": [_record("某项目造价咨询服务采购公告", _today_iso(), "r1")],
                "total": 1,
            } if k.get("page", a[3] if len(a) > 3 else 1) == 1 else {"records": [], "total": 1},
        )
        results = c.crawl()
        assert len(results) == 1
        assert "造价" in results[0]["matched_keywords"]
        assert results[0]["area_type"] == "city"
        assert c.store.exists("某项目造价咨询服务采购公告", datetime.now().strftime("%Y-%m-%d"))

    def test_dedups_already_stored(self, monkeypatch, tmp_path):
        c = self._make_crawler(monkeypatch, tmp_path, area_type="qingdao")
        today = datetime.now().strftime("%Y-%m-%d")
        # Pre-insert the same notice so store.exists() returns True.
        c.store.insert({
            "title": "某项目造价咨询公告", "link": "l", "publish_date": today,
            "area_type": "city", "matched_keywords": ["造价"],
        })
        monkeypatch.setattr(
            "crawler.fetch_page",
            lambda *a, **k: {"records": [_record("某项目造价咨询公告", _today_iso(), "r1")], "total": 1},
        )
        results = c.crawl()
        assert len(results) == 0  # deduped away

    def test_stops_pagination_at_cutoff(self, monkeypatch, tmp_path):
        c = self._make_crawler(monkeypatch, tmp_path, area_type="qingdao",
                               days_back=2, max_pages=5)
        old_iso = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%dT10:00:00.000+08:00")
        calls = []

        def fake_fetch(col_code, area_type, area=None, page=1, limit=15):
            calls.append(page)
            return {"records": [_record("造价公告A", old_iso, "a")], "total": 3}

        monkeypatch.setattr("crawler.fetch_page", fake_fetch)
        c.crawl()
        assert 2 not in calls  # page-1 record older than cutoff -> stop, no page 2

    def test_area_all_fetches_city_and_county(self, monkeypatch, tmp_path):
        c = self._make_crawler(monkeypatch, tmp_path, area_type="all")
        seen = []
        monkeypatch.setattr(
            "crawler.fetch_page",
            lambda *a, **k: (seen.append(a[1]) or {"records": [], "total": 0}),
        )
        c.crawl(max_pages=1)
        assert "city" in seen and "county" in seen

    def test_max_pages_respected(self, monkeypatch, tmp_path):
        c = self._make_crawler(monkeypatch, tmp_path, area_type="qingdao", max_pages=3)
        calls = []

        def fake_fetch(col_code, area_type, area=None, page=1, limit=15):
            calls.append(page)
            return {"records": [_record(f"造价公告{page}", _today_iso(), f"r{page}")], "total": 100}

        monkeypatch.setattr("crawler.fetch_page", fake_fetch)
        c.crawl()  # uses self.max_pages = 3
        assert max(calls) <= 3
        assert len(calls) == 3

    def test_skips_non_keyword_notices(self, monkeypatch, tmp_path):
        c = self._make_crawler(monkeypatch, tmp_path, area_type="qingdao")
        recs = [
            _record("绿化养护采购公告", _today_iso(), "no"),
            _record("造价咨询服务公告", _today_iso(), "yes"),
        ]
        monkeypatch.setattr("crawler.fetch_page", lambda *a, **k: {"records": recs, "total": 2})
        results = c.crawl()
        assert len(results) == 1
        assert "造价" in results[0]["matched_keywords"]

    def test_fetch_error_stops_that_target(self, monkeypatch, tmp_path):
        c = self._make_crawler(monkeypatch, tmp_path, area_type="qingdao")
        monkeypatch.setattr("crawler.fetch_page", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
        results = c.crawl()
        assert results == []
