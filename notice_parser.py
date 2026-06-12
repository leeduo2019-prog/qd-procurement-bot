"""HTML parsing and notice extraction."""

import re
import logging
from datetime import datetime
from typing import List, Dict, Optional, Set

from bs4 import BeautifulSoup, Tag

logger = logging.getLogger("qd_crawler")

# Target-site DOM selectors, ordered by specificity
NOTICE_SELECTORS = [
    "ul.list_right_n > li",
    "ul.list_right_n li",
    "li > span.datelink1_n",
]


def parse_notice(container: Tag) -> Optional[Dict]:
    """Extract notice info from a single container element."""
    title_elem = container.find("a")
    if not title_elem:
        title_elem = container
        link_elem = container.find("a")
    else:
        link_elem = title_elem

    title = title_elem.get_text(strip=True) if title_elem else ""

    if not title or len(title) < 8:
        return None

    link = link_elem.get("href", "") if link_elem else ""
    if link and isinstance(link, str) and not link.startswith("http"):
        if link.startswith("/qdsite/"):
            link = f"https://zfcg.qingdao.gov.cn{link}"
        else:
            link = f"https://zfcg.qingdao.gov.cn/qdsite/{link.lstrip('/')}"

    publish_date = ""
    date_elem = container.find("span", class_="date_new")
    if date_elem:
        publish_date = date_elem.get_text(strip=True)

    if not publish_date:
        date_pattern = re.compile(r"\d{4}-\d{2}-\d{2}")
        all_text = container.get_text()
        date_match = date_pattern.search(all_text)
        if date_match:
            publish_date = date_match.group()

    return {
        "title": title,
        "link": link,
        "publish_date": publish_date,
        "crawl_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def is_valid_notice(title: str) -> bool:
    """Return True if the title looks like a real procurement notice."""
    exclude_patterns = [
        r"^首页", r"^关于", r"^联系", r"^帮助", r"^登录", r"^注册",
        r"^网站地图", r"^设为首页", r"^加入收藏",
        r"^(上一页|下一页|首页|末页)",
    ]
    for pattern in exclude_patterns:
        if re.search(pattern, title):
            return False
    include_patterns = [
        r"采购|招标|中标|公告|项目|合同|预算|审计|造价|决算|结算",
        r"竞争性|询价|单一来源|邀请|公开",
    ]
    return any(re.search(p, title) for p in include_patterns)


def extract_notices(soup: BeautifulSoup) -> List[Dict]:
    """Extract notices from a parsed HTML page."""
    notices: List[Dict] = []
    seen_titles: Set[str] = set()

    logger.debug("页面 HTML 长度: %d", len(str(soup)))

    for selector in NOTICE_SELECTORS:
        try:
            elements = soup.select(selector)
            if not elements:
                continue

            candidates = elements[:30]
            logger.debug("  选择器 '%s' 匹配到 %d 个元素", selector, len(candidates))

            for container in candidates:
                try:
                    notice = parse_notice(container)
                    if notice and notice["title"] not in seen_titles:
                        logger.debug("  解析到公告: %s...", notice["title"][:30])
                        if is_valid_notice(notice["title"]):
                            notices.append(notice)
                            seen_titles.add(notice["title"])
                            logger.debug("  ✓ 有效公告: %s", notice["title"][:40])
                except Exception as e:
                    logger.debug("  解析单条公告时出错: %s", e)
                    continue

            if notices:
                logger.debug("  使用选择器 '%s' 提取到 %d 条有效公告", selector, len(notices))
                break
        except Exception as e:
            logger.debug("  选择器 '%s' 执行失败: %s", selector, e)
            continue

    if not notices:
        logger.warning("  所有选择器均未提取到公告，尝试 fallback 策略...")
        notices = _fallback_extract(soup)

    return notices


def _fallback_extract(soup: BeautifulSoup) -> List[Dict]:
    """Fallback: extract all links that contain a date pattern."""
    notices: List[Dict] = []
    date_pattern = re.compile(r"\d{4}[-/.]\d{1,2}[-/.]\d{1,2}")

    for a_tag in soup.find_all("a", href=True):
        title = a_tag.get_text(strip=True)
        if len(title) < 10 or not is_valid_notice(title):
            continue

        parent = a_tag.parent
        date_text = ""
        if parent:
            date_match = date_pattern.search(parent.get_text())
            if date_match:
                date_text = date_match.group()

        link = a_tag["href"]
        if isinstance(link, str) and link and not link.startswith("http"):
            link = f"https://zfcg.qingdao.gov.cn{link}"

        notices.append({
            "title": title,
            "link": link,
            "publish_date": date_text,
            "crawl_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })

    logger.debug("  Fallback 策略提取到 %d 条公告", len(notices))
    return notices[:20]
