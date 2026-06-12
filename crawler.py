"""
青岛政府采购爬虫
抓取涉及造价、审计、预算、决算、结算的采购公告

v2.1 - 模块化重构：
- 拆分为 store / browser / parser 三个子模块
- WebDriverWait 替代硬编码 sleep
- 统一 HTTPS 协议
- 控制台日志级别 INFO
"""

import os
import re
import csv
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from bs4 import BeautifulSoup

from store import NoticeStore
from browser import (
    setup_driver, close_driver, switch_to_tab,
    has_next_page, go_to_next_page, click_procurement_notice_tab,
)
from notice_parser import extract_notices

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger("qd_crawler")
logger.setLevel(logging.DEBUG)

# Console: INFO level for production
_console = logging.StreamHandler()
_console.setLevel(logging.INFO)
_console.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(_console)

# File: DEBUG level with daily rotation, keep 30 days
from logging.handlers import TimedRotatingFileHandler

_file = TimedRotatingFileHandler(
    os.path.join(LOG_DIR, "crawler.log"),
    when="midnight",
    backupCount=30,
    encoding="utf-8",
)
_file.setLevel(logging.DEBUG)
_file.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(_file)


# ---------------------------------------------------------------------------
# Crawler
# ---------------------------------------------------------------------------

class ProcurementCrawler:
    """政府采购爬虫 — 编排爬取流程。"""

    BASE_URL = "https://zfcg.qingdao.gov.cn/qdsite/#/site-list-varied?colCode=04"

    def __init__(self, area_type: str = "all", days_back: int = 2, max_pages: int = 5):
        self.area_type = area_type
        self.days_back = days_back
        self.max_pages = max_pages
        self.keywords = self._load_keywords()
        self.driver = None
        self.results: List[Dict] = []
        self.store = NoticeStore()
        self.cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    def _load_keywords(self) -> List[str]:
        keywords_str = os.getenv("KEYWORDS", "造价，审计，预算，决算，结算")
        return [kw.strip() for kw in re.split(r"[,，]", keywords_str) if kw.strip()]

    def _match_keywords(self, title: str) -> bool:
        return any(keyword in title for keyword in self.keywords)

    def _get_matched_keywords(self, title: str) -> List[str]:
        return [kw for kw in self.keywords if kw in title]

    def crawl(self, max_pages: int = 5) -> List[Dict]:
        if max_pages is None:
            max_pages = self.max_pages
        logger.info("=" * 60)
        logger.info("青岛政府采购爬虫 - 自动运行")
        logger.info("=" * 60)
        logger.info("关键词: %s", ", ".join(self.keywords))
        logger.info("最大页数: %d", max_pages)
        logger.info("区域类型: %s", self.area_type)
        logger.info("日期范围: %s 至今（最近 %d 天）", self.cutoff_date, self.days_back)

        self.driver = setup_driver()
        matched_notices: List[Dict] = []
        duplicate_count = 0

        try:
            tabs_to_crawl = []
            if self.area_type == "all":
                tabs_to_crawl = [("qingdao", "青岛市"), ("districts", "各区市")]
            elif self.area_type == "districts":
                tabs_to_crawl = [("districts", "各区市")]
            else:
                tabs_to_crawl = [("qingdao", "青岛市")]

            try:
                if self.driver is None:
                    raise RuntimeError("浏览器驱动未初始化")
                logger.debug("正在访问 URL: %s", self.BASE_URL)
                self.driver.get(self.BASE_URL)
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                # Wait for SPA Vue.js to render initial content
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "ul.list_right_n, .list-box, .notice-list")
                    )
                )

                click_procurement_notice_tab(self.driver)
            except TimeoutException:
                logger.error("页面加载超时")
                return matched_notices
            except WebDriverException as e:
                logger.error("页面加载失败: %s", e)
                return matched_notices

            for tab_area, tab_name in tabs_to_crawl:
                logger.info(">> 开始爬取【%s】标签页", tab_name)
                current_area = tab_area

                if tab_name != "青岛市":
                    if not switch_to_tab(self.driver, tab_name):
                        logger.warning("  无法切换到 '%s' 标签页，跳过", tab_name)
                        continue

                for page in range(1, max_pages + 1):
                    logger.info("  爬取第 %d 页...", page)

                    html = self.driver.page_source
                    logger.debug("页面 HTML 长度: %d", len(html))

                    soup = BeautifulSoup(html, "html.parser")
                    notices = extract_notices(soup)

                    if not notices:
                        logger.info("  第 %d 页未找到公告，可能已到达最后一页", page)
                        break

                    logger.info("  第 %d 页找到 %d 条公告", page, len(notices))

                    for notice in notices:
                        notice_date = notice.get("publish_date", "")
                        if notice_date and notice_date < self.cutoff_date:
                            logger.debug("  跳过过期公告: %s [%s]", notice["title"][:30], notice_date)
                            continue

                        if self._match_keywords(notice.get("title", "")):
                            if self.store.exists(
                                notice.get("title", ""), notice.get("publish_date", "")
                            ):
                                duplicate_count += 1
                                continue

                            matched = notice.copy()
                            matched["matched_keywords"] = self._get_matched_keywords(
                                notice["title"]
                            )
                            matched["area_type"] = current_area
                            matched_notices.append(matched)
                            self.store.insert(matched)
                            logger.info(
                                "  ✓ 匹配: %s... [%s]",
                                notice["title"][:50],
                                ", ".join(matched["matched_keywords"]),
                            )

                    if not has_next_page(self.driver):
                        logger.info("  没有更多页面了")
                        break

                    logger.info("  点击下一页...")
                    if not go_to_next_page(self.driver):
                        logger.info("  翻页失败，停止当前标签页爬取")
                        break

                logger.info(">> 【%s】标签页爬取完成", tab_name)

        except KeyboardInterrupt:
            logger.info("用户中断爬取")
        except Exception as e:
            logger.error("爬取过程中出错: %s", e, exc_info=True)
        finally:
            close_driver(self.driver)
            self.driver = None

        self.results = matched_notices
        logger.info("=" * 60)
        logger.info("爬取完成！共找到 %d 条新匹配公告（跳过 %d 条重复）",
                     len(matched_notices), duplicate_count)
        logger.info("=" * 60)
        return matched_notices

    # -- 数据保存 ------------------------------------------------------------

    def save_to_file(self, filename: str = "") -> str:
        if not filename:
            filename = f"procurement_notices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)

        logger.info("结果已保存到: %s", filename)
        return filename

    def save_to_csv(self, filename: str = "") -> str:
        if not filename:
            filename = f"procurement_notices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        if not self.results:
            logger.info("没有数据可保存")
            return filename

        fieldnames = [
            "title", "link", "publish_date", "area_type",
            "matched_keywords", "crawl_time",
        ]
        with open(filename, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in self.results:
                out = dict(row)
                out["matched_keywords"] = "、".join(out.get("matched_keywords", []))
                writer.writerow(out)

        logger.info("结果已保存到 CSV: %s", filename)
        return filename
