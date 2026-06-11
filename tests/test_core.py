"""
Core logic tests — no Selenium/browser needed.
Run with: pytest tests/test_core.py
"""

import os
import re
import csv
import json
import tempfile
import pytest
from datetime import datetime
from unittest.mock import patch
from bs4 import BeautifulSoup, Tag

# Ensure .env is not loaded during tests
os.environ.setdefault("KEYWORDS", "造价,审计,预算,决算,结算")
os.environ.setdefault("DINGTALK_WEBHOOK", "")
os.environ.setdefault("DINGTALK_SECRET", "")

from crawler import ProcurementCrawler, NoticeStore
from dingtalk_notifier import DingTalkNotifier


# ---------------------------------------------------------------------------
# Keyword matching
# ---------------------------------------------------------------------------

class TestKeywordMatching:
    """Test _load_keywords, _match_keywords, _get_matched_keywords."""

    def _make_crawler(self, keywords_env="造价,审计,预算,决算,结算"):
        c = ProcurementCrawler.__new__(ProcurementCrawler)
        with patch.dict(os.environ, {"KEYWORDS": keywords_env}):
            c.keywords = c._load_keywords()
        return c

    def test_load_keywords_english_comma(self):
        c = self._make_crawler("造价,审计,预算")
        assert c.keywords == ["造价", "审计", "预算"]

    def test_load_keywords_chinese_comma(self):
        c = self._make_crawler("造价，审计，预算")
        assert c.keywords == ["造价", "审计", "预算"]

    def test_load_keywords_mixed_commas(self):
        c = self._make_crawler("造价，审计,预算，决算")
        assert c.keywords == ["造价", "审计", "预算", "决算"]

    def test_load_keywords_strips_whitespace(self):
        c = self._make_crawler(" 造价 , 审计 , 预算 ")
        assert c.keywords == ["造价", "审计", "预算"]

    def test_load_keywords_empty_entries_ignored(self):
        c = self._make_crawler("造价,,审计,")
        assert c.keywords == ["造价", "审计"]

    def test_match_keywords_hit(self):
        c = self._make_crawler()
        c.keywords = ["造价", "审计"]
        assert c._match_keywords("青岛市造价咨询服务采购公告") is True

    def test_match_keywords_miss(self):
        c = self._make_crawler()
        c.keywords = ["造价", "审计"]
        assert c._match_keywords("青岛市绿化养护采购公告") is False

    def test_get_matched_keywords(self):
        c = self._make_crawler()
        c.keywords = ["造价", "审计", "预算"]
        result = c._get_matched_keywords("青岛市造价及审计服务采购")
        assert result == ["造价", "审计"]


# ---------------------------------------------------------------------------
# Notice validation
# ---------------------------------------------------------------------------

class TestNoticeValidation:
    """Test _is_valid_notice static method."""

    def test_valid_procurement_title(self):
        assert ProcurementCrawler._is_valid_notice("青岛市财政局预算绩效评价项目招标公告") is True

    def test_valid_with_keywords(self):
        assert ProcurementCrawler._is_valid_notice("某项目造价咨询服务采购公告") is True
        assert ProcurementCrawler._is_valid_notice("某项目中标结果公告") is True
        assert ProcurementCrawler._is_valid_notice("竞争性磋商公告") is True

    def test_invalid_navigation_titles(self):
        assert ProcurementCrawler._is_valid_notice("首页") is False
        assert ProcurementCrawler._is_valid_notice("关于我们") is False
        assert ProcurementCrawler._is_valid_notice("登录") is False
        assert ProcurementCrawler._is_valid_notice("下一页") is False
        assert ProcurementCrawler._is_valid_notice("网站地图") is False

    def test_invalid_generic_titles(self):
        assert ProcurementCrawler._is_valid_notice("联系我们") is False
        assert ProcurementCrawler._is_valid_notice("帮助中心") is False


# ---------------------------------------------------------------------------
# Notice parsing
# ---------------------------------------------------------------------------

class TestParseNotice:
    """Test _parse_notice static method with real HTML fragments."""

    def test_parse_standard_structure(self):
        html = """
        <li>
            <span class="datelink1_n">
                <a href="/qdsite/notice/123">青岛市某项目造价咨询服务采购公告</a>
                <span class="date_new">2026-06-10</span>
            </span>
        </li>
        """
        container = BeautifulSoup(html, "html.parser").find("li")
        result = ProcurementCrawler._parse_notice(container)
        assert result is not None
        assert "造价咨询" in result["title"]
        assert result["publish_date"] == "2026-06-10"
        assert "http" in result["link"]

    def test_parse_relative_link_gets_prefixed(self):
        html = """
        <li>
            <span class="datelink1_n">
                <a href="/qdsite/notice/456">某项目公开招标公告</a>
                <span class="date_new">2026-06-09</span>
            </span>
        </li>
        """
        container = BeautifulSoup(html, "html.parser").find("li")
        result = ProcurementCrawler._parse_notice(container)
        assert result["link"].startswith("http://zfcg.qingdao.gov.cn")

    def test_parse_absolute_link_preserved(self):
        html = """
        <li>
            <span class="datelink1_n">
                <a href="http://example.com/notice">某项目公开招标公告</a>
                <span class="date_new">2026-06-09</span>
            </span>
        </li>
        """
        container = BeautifulSoup(html, "html.parser").find("li")
        result = ProcurementCrawler._parse_notice(container)
        assert result["link"] == "http://example.com/notice"

    def test_parse_short_title_returns_none(self):
        html = '<li><span class="datelink1_n"><a href="/x">短标题</a></span></li>'
        container = BeautifulSoup(html, "html.parser").find("li")
        result = ProcurementCrawler._parse_notice(container)
        assert result is None

    def test_parse_no_link_returns_none_or_fallback(self):
        html = '<li><span>无链接的文本</span></li>'
        container = BeautifulSoup(html, "html.parser").find("li")
        result = ProcurementCrawler._parse_notice(container)
        # Either None (no <a> tag) or valid dict with extracted text
        if result is not None:
            assert "title" in result

    def test_parse_date_from_regex_fallback(self):
        html = """
        <li>
            <span class="datelink1_n">
                <a href="/notice/789">某项目预算审核服务采购公告</a>
            </span>
            <span>发布日期：2026-06-08</span>
        </li>
        """
        container = BeautifulSoup(html, "html.parser").find("li")
        result = ProcurementCrawler._parse_notice(container)
        if result:
            assert "2026-06-08" in result.get("publish_date", "")


# ---------------------------------------------------------------------------
# NoticeStore (SQLite deduplication)
# ---------------------------------------------------------------------------

class TestNoticeStore:
    """Test SQLite-based deduplication."""

    def _make_store(self):
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        store = NoticeStore(db_path=tmp.name)
        return store, tmp.name

    def test_insert_and_exists(self):
        store, path = self._make_store()
        notice = {
            "title": "测试公告标题",
            "link": "http://example.com",
            "publish_date": "2026-06-10",
            "area_type": "qingdao",
            "matched_keywords": ["造价"],
        }
        assert store.exists("测试公告标题", "2026-06-10") is False
        assert store.insert(notice) is True
        assert store.exists("测试公告标题", "2026-06-10") is True

    def test_duplicate_insert_returns_false(self):
        store, path = self._make_store()
        notice = {
            "title": "重复公告",
            "link": "http://example.com",
            "publish_date": "2026-06-10",
        }
        assert store.insert(notice) is True
        assert store.insert(notice) is False

    def test_id_based_on_title_and_date(self):
        store, path = self._make_store()
        id1 = store._make_id("公告A", "2026-06-10")
        id2 = store._make_id("公告A", "2026-06-10")
        id3 = store._make_id("公告B", "2026-06-10")
        assert id1 == id2
        assert id1 != id3

    def test_get_recent_ids(self):
        store, path = self._make_store()
        notice = {
            "title": "近期公告",
            "link": "http://example.com",
            "publish_date": "2026-06-10",
            "crawl_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        store.insert(notice)
        recent = store.get_recent_ids(days=1)
        nid = store._make_id("近期公告", "2026-06-10")
        assert nid in recent

    def teardown_method(self):
        pass


# ---------------------------------------------------------------------------
# DingTalk markdown generation
# ---------------------------------------------------------------------------

class TestDingTalkMarkdown:
    """Test message formatting."""

    def _make_notifier(self):
        with patch.dict(os.environ, {
            "DINGTALK_WEBHOOK": "https://test.example.com/webhook",
            "DINGTALK_SECRET": "",
        }):
            return DingTalkNotifier()

    def test_empty_notices_template(self):
        notifier = self._make_notifier()
        md = notifier._generate_markdown([], "测试标题")
        assert "测试标题" in md
        assert "未有目标公告" in md

    def test_with_notices_template(self):
        notifier = self._make_notifier()
        notices = [
            {
                "title": "某项目造价咨询服务采购公告",
                "link": "http://example.com/1",
                "publish_date": "2026-06-10",
                "matched_keywords": ["造价"],
            }
        ]
        md = notifier._generate_markdown(notices, "测试标题")
        assert "匹配数量：**1**" in md
        assert "造价咨询服务" in md
        assert "查看公告" in md

    def test_keywords_from_env(self):
        with patch.dict(os.environ, {
            "DINGTALK_WEBHOOK": "https://test.example.com/webhook",
            "DINGTALK_SECRET": "",
            "KEYWORDS": "测试词A,测试词B",
        }):
            notifier = DingTalkNotifier()
            md = notifier._generate_markdown([], "标题")
            assert "测试词A" in md

    def test_generate_sign_with_secret(self):
        with patch.dict(os.environ, {
            "DINGTALK_WEBHOOK": "https://test.example.com/webhook",
            "DINGTALK_SECRET": "SECtest123",
        }):
            notifier = DingTalkNotifier()
            sign_data = notifier._generate_sign()
            assert "timestamp" in sign_data
            assert "sign" in sign_data
            assert len(sign_data["timestamp"]) == 13  # milliseconds

    def test_generate_sign_without_secret(self):
        with patch.dict(os.environ, {
            "DINGTALK_WEBHOOK": "https://test.example.com/webhook",
            "DINGTALK_SECRET": "",
        }):
            notifier = DingTalkNotifier()
            sign_data = notifier._generate_sign()
            assert sign_data == {}


# ---------------------------------------------------------------------------
# CSV / JSON file output
# ---------------------------------------------------------------------------

class TestFileOutput:
    """Test save_to_file and save_to_csv produce consistent output."""

    def _make_crawler_with_results(self):
        with patch.dict(os.environ, {"KEYWORDS": "造价,审计"}):
            c = ProcurementCrawler.__new__(ProcurementCrawler)
            c.keywords = ["造价", "审计"]
            c.results = [
                {
                    "title": "测试公告",
                    "link": "http://example.com",
                    "publish_date": "2026-06-10",
                    "area_type": "qingdao",
                    "matched_keywords": ["造价"],
                    "crawl_time": "2026-06-10 09:00:00",
                }
            ]
            return c

    def test_json_output(self):
        c = self._make_crawler_with_results()
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            path = f.name
        try:
            c.save_to_file(path)
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            assert len(data) == 1
            assert data[0]["title"] == "测试公告"
        finally:
            os.unlink(path)

    def test_csv_output(self):
        c = self._make_crawler_with_results()
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
            path = f.name
        try:
            c.save_to_csv(path)
            with open(path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            assert len(rows) == 1
            assert rows[0]["title"] == "测试公告"
        finally:
            os.unlink(path)
