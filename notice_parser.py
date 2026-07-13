"""API record parsing for procurement notices.

将后端 API 返回的单条公告记录映射为内部 notice dict:
  subject  -> title
  id       -> link (#/read?id=...)
  pdate    -> publish_date (ISO 时间取前 10 位日期)

旧版基于 BeautifulSoup 的 HTML 解析已移除:网站是 Vue SPA,
真实数据来自后端 API(见 api_client),不再解析 DOM。
"""

import logging
from datetime import datetime
from typing import Dict, Optional

logger = logging.getLogger("qd_crawler")

SITE_BASE = "http://zfcg.qingdao.gov.cn"
READ_LINK_TEMPLATE = f"{SITE_BASE}/qdsite/#/read?id={{id}}"


def parse_api_record(record: Dict) -> Optional[Dict]:
    """将一条 API 记录映射为内部 notice dict。

    Returns:
        {title, link, publish_date, crawl_time},或 None(无标题)
    """
    title = (record.get("subject") or "").strip()
    if not title:
        return None

    raw_id = record.get("id") or ""
    link = READ_LINK_TEMPLATE.format(id=raw_id) if raw_id else ""

    pdate = record.get("pdate") or ""
    # pdate 形如 "2026-07-10T21:34:23.000+08:00",取前 10 位日期
    publish_date = pdate[:10] if pdate else ""

    return {
        "title": title,
        "link": link,
        "publish_date": publish_date,
        "crawl_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
