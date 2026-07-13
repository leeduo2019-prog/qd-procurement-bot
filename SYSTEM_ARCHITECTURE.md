# 青岛政府采购爬虫 - 系统架构详解

## 📊 系统总览

爬虫**不使用浏览器**,直接调用青岛政府采购网后端 REST API 获取公告数据。

```
┌─────────────────────────────────────────────────────────────┐
│           青岛市政府采购网后端 API                            │
│   http://zfcg.qingdao.gov.cn:58060/api/siteservice/...      │
└────────────────────┬────────────────────────────────────────┘
                     │  POST /qd/site-info/page
                     │  (requests + 浏览器 UA,绕过 WAF)
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                    爬虫核心 (crawler.py)                     │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐   │
│  │ api_client  │->│ notice_parser│->│ 关键词匹配      │   │
│  │ (分页抓取)  │  │ (record->dict)│  │ (造价，审计...)  │   │
│  └─────────────┘  └──────────────┘  └─────────────────┘   │
└────────────────────┬────────────────────────────────────────┘
                     │  匹配的公告数据
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  NoticeStore (SQLite 去重) → JSON / CSV 文件                 │
└────────────────────┬────────────────────────────────────────┘
                     │  DingTalkNotifier (Webhook + HMAC 加签)
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                    钉钉群机器人                               │
└─────────────────────────────────────────────────────────────┘
```

---

## 🌐 目标网站与 API

网站 `http://zfcg.qingdao.gov.cn/qdsite/` 是 **Vue 单页应用**,初始 HTML 只有空 `<div id=app>`,真实数据由前端通过后端 API 异步加载。因此**不解析 DOM,直接调 API**。

### API 端点

| 用途 | 方法 | 路径 |
|------|------|------|
| 公告列表 | POST | `/api/siteservice/free/qd/site-info/page` |
| 公告详情 | GET | `/api/siteservice/free/qd/site-info/read-notice-value?id=<加密id>` |
| 区域列表 | GET | `/api/siteservice/free/qd/region/get-son` |
| 栏目树 | GET | `/api/siteservice/free/qd/column/get?colCode=<code>` |

base URL:`http://zfcg.qingdao.gov.cn:58060`(端口 58060)。

### ⚠️ 必须发浏览器 User-Agent

服务端 WAF 拦截默认的 `python-requests` UA,直接返回 403。`api_client.DEFAULT_HEADERS` 固定发送 Chrome UA + Referer。

### 列表请求体(siteQData)

```json
{
  "page": 1, "limit": 15, "colCode": "0303",
  "sort": "-pdate", "areaType": "city", "area": null,
  "pdates": ["", ""], ...
}
```

- 服务端 `limit` 硬上限 **15**(传更大值仍返回 15)。
- 响应:`{data:{code:100, data:{records, total}}}`,`code=100` 成功。

### record 字段

| 字段 | 含义 |
|------|------|
| `id` | 加密 ID,用于拼详情链接 `#/read?id=` |
| `subject` | 公告标题 |
| `pdate` | 发布时间(ISO,如 `2026-07-10T21:34:23.000+08:00`) |
| `area` | 区域码(370200=市级,370202=市南区...) |
| `regionName` | 区域名(市级/市南/李沧...) |
| `projectCode` | 项目编号(如 SDGP370200...) |

---

## 🏷️ 栏目代码(colCode)

**注意**:前端路由的 `colCode=04` 是「政策法规」,**不是采购公告**。早期版本误用 04 导致抓不到数据(已修复)。

`colCode=03` 是「政府采购信息」大类,其下子分类:

| colCode | 名称 |
|---------|------|
| **0303** | **采购公告**(默认) |
| 0304 | 中标公告 |
| 0305 | 更正公告 |
| 0306 | 废标公告 |
| 0307 | 单一来源公示 |

在 `crawler.py` 的 `COL_CODES` 列表中追加即可同时抓取多个分类。

---

## 🏙️ 市级 vs 各区市

通过 `areaType` 参数区分(配合 `area` 区域码):

| 范围 | areaType | area | 数据量(参考) |
|------|----------|------|------------|
| 市级 | `city` | `null` | ~12307 |
| 各区市(全部) | `county` | `null` | ~26577 |
| 单个区市 | `county` | 区域码(如 370202) | 视区而定 |

区域码来自 `/qd/region/get-son`:370202 市南、370203 市北、370213 李沧、370211 黄岛、370212 崂山、370283 平度 等。

爬虫的 `AREA_TYPE` 配置(`all`/`qingdao`/`districts`)映射到上述组合,见 `crawler.AREA_TARGETS`。

---

## 🔍 工作流程

1. **初始化**:读 KEYWORDS / AREA_TYPE / DAYS_BACK,计算 `cutoff_date = now - days_back`。
2. **分页抓取**:对每个 `(colCode, areaType)`:
   - `page=1,2,...` 调 `api_client.fetch_page`(每页 15 条,按 `-pdate` 降序)
   - 每条 record 经 `parse_api_record` 映射为 notice dict
   - 遇到 `publish_date < cutoff_date` 即停止翻页(后续更旧)
3. **关键词匹配**:`title` 含任一关键词即命中,记录命中的关键词。
4. **去重**:`NoticeStore` 以 `(title, publish_date)` 的 md5 为主键,已存在则跳过。
5. **保存与推送**:匹配结果存 JSON/CSV,经 `dingtalk_notifier` 推送钉钉。

---

## 📁 数据流向

```
.env (KEYWORDS / AREA_TYPE / DAYS_BACK / DINGTALK_*)
    ↓
main.py → ProcurementCrawler.crawl()
    ↓
api_client.fetch_page()  ──POST──>  后端 API :58060
    ↓ JSON records
notice_parser.parse_api_record()  →  notice dict
    ↓
关键词匹配 + NoticeStore 去重 (SQLite notices.db)
    ↓
save_to_file / save_to_csv  +  DingTalkNotifier
    ↓
procurement_notices_*.json/csv  +  钉钉群消息
```

---

## 🛡️ 反爬与稳定性

| 策略 | 实现 |
|------|------|
| 浏览器 UA | `DEFAULT_HEADERS` 固定 Chrome UA,绕过 WAF 403 |
| 低频抓取 | 每天 2 次定时(GHA),按日期截止线提前停止翻页 |
| 异常隔离 | 单页/单目标失败不影响其他,记录日志继续 |
| 幂等去重 | SQLite 主键去重,重复运行不重复推送 |

---

## 📊 性能

- 每页 API 耗时 ~0.3-0.5 秒(无浏览器开销)
- `days_back=2` 通常 2-5 页即可覆盖,全程 < 10 秒
- 内存 < 50MB,无 Chrome 依赖

---

## 🧪 测试

```
tests/
├── test_api_client.py      # 请求构造、响应解析、UA、错误处理(mock)
├── test_notice_parser.py   # record→notice 映射
├── test_crawler.py         # 分页/截止/匹配/去重编排(mock fetch_page)
└── test_core.py            # 关键词匹配、Store、DingTalk markdown、文件输出
```

全部用 mock 隔离,不依赖网络。真实 API 由 `tests/fixtures/site_info_page_0303_city.json` 提供固定样本。

运行:`pytest tests/`

---

## 🔧 扩展

- **加关键词**:改 `.env` 的 `KEYWORDS`。
- **加栏目**:在 `crawler.COL_CODES` 追加如 `"0304"`(中标)。
- **改回看天数**:`.env` 的 `DAYS_BACK`。
- **改页数上限**:`main.py` 的 `crawler.crawl(max_pages=...)`。

---

**文档版本**: 3.0
**最后更新**: 2026-07-13
**适用版本**: qd-procurement-bot v3.0(API 直连版)
