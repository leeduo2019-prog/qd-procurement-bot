"""
青岛政府采购爬虫
通过后端 API 抓取涉及造价、审计、预算、决算、结算的采购公告,并推送到钉钉群。

v3.0 - 改用直接 API 调用替代 Selenium:
- 不再依赖浏览器,直接 POST /api/siteservice/free/qd/site-info/page(见 api_client)
- 按发布日期降序(-pdate)分页,遇到早于 cutoff 的记录停止翻页
- colCode=0303(采购公告)为默认分类,可在 COL_CODES 扩展

旧版基于 Selenium + BeautifulSoup 解析 DOM 的方案已移除:
网站是 Vue SPA,真实数据来自后端 API,旧选择器(ul.list_right_n 等)已失效。
"""

import os
import re
import csv
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

from store import NoticeStore
from api_client import fetch_page
from notice_parser import parse_api_record

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger("qd_crawler")
logger.setLevel(logging.DEBUG)

_console = logging.StreamHandler()
_console.setLevel(logging.INFO)
_console.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(_console)

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

# 采购公告分类码。如需覆盖中标/更正/废标,在此追加 "0304"/"0305"/"0306"。
COL_CODES: List[str] = ["0303"]

# area_type 配置 -> [(api_area_type, area, 标签名), ...]
#   city   -> 市级公告(areaType="city", area=None)
#   county -> 各区市公告(areaType="county", area=None 拉全部区市)
AREA_TARGETS: Dict[str, List[Tuple[str, Optional[str], str]]] = {
    "all": [("city", None, "青岛市"), ("county", None, "各区市")],
    "qingdao": [("city", None, "青岛市")],
    "districts": [("county", None, "各区市")],
}


class ProcurementCrawler:
    """政府采购爬虫 - 通过后端 API 编排抓取流程。"""

    def __init__(self, area_type: str = "all", days_back: int = 2, max_pages: int = 5):
        self.area_type = area_type
        self.days_back = days_back
        self.max_pages = max_pages
        self.keywords = self._load_keywords()
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

    def crawl(self, max_pages: int = None) -> List[Dict]:
        if max_pages is None:
            max_pages = self.max_pages
        logger.info("=" * 60)
        logger.info("青岛政府采购爬虫 - 自动运行")
        logger.info("=" * 60)
        logger.info("关键词: %s", ", ".join(self.keywords))
        logger.info("分类码: %s", ", ".join(COL_CODES))
        logger.info("最大页数: %d", max_pages)
        logger.info("区域类型: %s", self.area_type)
        logger.info("日期范围: %s 至今（最近 %d 天）", self.cutoff_date, self.days_back)

        targets = AREA_TARGETS.get(self.area_type, AREA_TARGETS["all"])
        matched_notices: List[Dict] = []
        duplicate_count = 0

        try:
            for col_code in COL_CODES:
                for area_type, area, area_name in targets:
                    logger.info(">> 分类 %s 区域【%s】", col_code, area_name)
                    for page in range(1, max_pages + 1):
                        try:
                            result = fetch_page(
                                col_code, area_type, area=area, page=page
                            )
                        except Exception as e:
                            logger.error("  第 %d 页抓取失败,停止该目标: %s", page, e)
                            break

                        records = result["records"]
                        if not records:
                            logger.info("  第 %d 页无更多公告", page)
                            break

                        logger.info("  第 %d 页获取 %d 条", page, len(records))

                        # records 按 -pdate 降序;遇到早于 cutoff 的即停止翻页
                        stop_paging = False
                        for rec in records:
                            notice = parse_api_record(rec)
                            if not notice:
                                continue
                            pdate = notice["publish_date"]
                            if pdate and pdate < self.cutoff_date:
                                logger.debug("  到达日期截止线: %s [%s]", notice["title"][:30], pdate)
                                stop_paging = True
                                break

                            if self._match_keywords(notice["title"]):
                                if self.store.exists(
                                    notice["title"], notice["publish_date"]
                                ):
                                    duplicate_count += 1
                                    continue

                                notice["matched_keywords"] = self._get_matched_keywords(
                                    notice["title"]
                                )
                                notice["area_type"] = area_type
                                matched_notices.append(notice)
                                self.store.insert(notice)
                                logger.info(
                                    "  ✓ 匹配: %s... [%s]",
                                    notice["title"][:50],
                                    ", ".join(notice["matched_keywords"]),
                                )

                        if stop_paging:
                            logger.info("  已达日期截止线,停止翻页")
                            break

                    logger.info(">> 区域【%s】完成", area_name)

        except KeyboardInterrupt:
            logger.info("用户中断爬取")
        except Exception as e:
            logger.error("爬取过程中出错: %s", e, exc_info=True)

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
