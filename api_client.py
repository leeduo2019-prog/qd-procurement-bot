"""
青岛政府采购网后端 API 客户端。

网站是 Vue SPA,真实数据来自后端 REST API(端口 58060),不在初始 HTML 里。
本模块直接调用列表接口,替代旧的 Selenium + DOM 解析方案。

接口文档(逆向自 app.js):
- POST {API_BASE}{PAGE_ENDPOINT}
- 请求体(siteQData): colCode / areaType / area / page / limit / sort 等
- 响应: {data:{code:100, data:{records, total}}},code=100 成功
"""

import logging
from typing import Dict, Optional

import requests

logger = logging.getLogger("qd_crawler")

API_BASE = "http://zfcg.qingdao.gov.cn:58060"
PAGE_ENDPOINT = "/api/siteservice/free/qd/site-info/page"
MAX_LIMIT = 15  # 服务端硬上限:limit>15 仍只返回 15 条
DEFAULT_TIMEOUT = 20

# 服务端 WAF 会 403 默认的 python-requests UA,必须伪装浏览器。
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Referer": "http://zfcg.qingdao.gov.cn/qdsite/",
    "Content-Type": "application/json",
}


def fetch_page(
    col_code: str,
    area_type: str,
    area: Optional[str] = None,
    page: int = 1,
    limit: int = MAX_LIMIT,
) -> Dict:
    """抓取一页公告列表。

    Args:
        col_code: 分类码,如 "0303"(采购公告)、"0304"(中标公告)
        area_type: "city"(市级) 或 "county"(各区市)
        area: 区域码(如 "370202" 市南区);市级或拉全部区市时传 None
        page: 页码,从 1 开始
        limit: 每页条数,自动截断到 MAX_LIMIT

    Returns:
        {"records": [...], "total": int}

    Raises:
        requests.RequestException: 网络或 HTTP 错误
        RuntimeError: API 返回非成功状态(code != 100)
    """
    payload = {
        "subject": None,
        "page": page,
        "limit": min(limit, MAX_LIMIT),
        "colCode": col_code,
        "colCodes": None,
        "sort": "-pdate",
        "area": area,
        "areaType": area_type,
        "pdate": None,
        "pdates": ["", ""],
        "unitName": None,
        "projectCode": None,
        "agentName": None,
        "pdateType": None,
        "kindOf": None,
        "projectType": None,
    }
    url = f"{API_BASE}{PAGE_ENDPOINT}"
    logger.debug("API POST %s page=%d colCode=%s areaType=%s area=%s",
                 url, page, col_code, area_type, area)

    resp = requests.post(
        url,
        json=payload,
        timeout=DEFAULT_TIMEOUT,
        headers=DEFAULT_HEADERS,
    )
    resp.raise_for_status()
    data = resp.json().get("data", {})
    if data.get("code") != 100:
        raise RuntimeError(f"API 返回失败: code={data.get('code')} message={data.get('message')}")

    page_data = data.get("data", {}) or {}
    return {
        "records": page_data.get("records", []),
        "total": page_data.get("total", 0),
    }
