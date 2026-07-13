# 配置指南 - 区域、栏目与关键词

`crawler.py` 支持按**区域**、**栏目**、**关键词**三个维度筛选公告。本指南说明如何配置。

> 爬虫通过后端 API 获取数据,无需浏览器。详见 `SYSTEM_ARCHITECTURE.md`。

---

## 🏙️ 区域配置(AREA_TYPE)

青岛政府采购网区分「市级」与「各区市」公告,通过 `areaType` 参数控制:

| AREA_TYPE | 抓取范围 | 对应 API 参数 |
|-----------|----------|---------------|
| `all` | 市级 + 各区市 | `areaType=city` + `areaType=county`(area=null) |
| `qingdao` | 仅市级 | `areaType=city`, area=null |
| `districts` | 仅各区市 | `areaType=county`, area=null(全部区市) |

在 `.env` 中配置:

```ini
AREA_TYPE=all
```

各区市区域码(来自 `/qd/region/get-son`):370202 市南、370203 市北、370213 李沧、370211 黄岛、370212 崂山、370283 平度 等。默认 `districts` 拉全部区市;如需单个区市,可在 `crawler.AREA_TARGETS` 中加 `(area_type, area_code, 区名)`。

---

## 🏷️ 栏目配置(colCode)

默认抓取 **采购公告**(`colCode=0303`)。可在 `crawler.py` 顶部 `COL_CODES` 列表中追加:

```python
COL_CODES = ["0303"]          # 仅采购公告(默认)
# COL_CODES = ["0303", "0304"] # 采购公告 + 中标公告
```

可用栏目码:

| colCode | 名称 |
|---------|------|
| 0303 | 采购公告 |
| 0304 | 中标公告 |
| 0305 | 更正公告 |
| 0306 | 废标公告 |
| 0307 | 单一来源公示 |

> ⚠️ 注意:前端路由里的 `colCode=04` 是「政策法规」,**不是采购公告**。早期版本误用导致抓不到数据,已修复为 0303。

---

## 🔑 关键词配置(KEYWORDS)

标题含任一关键词即匹配。在 `.env` 配置(中英文逗号均可):

```ini
KEYWORDS=造价，审计，预算，决算，结算
```

可追加:

```ini
KEYWORDS=造价，审计，预算，决算，结算，招标，竞争性磋商
```

---

## 📅 回看天数(DAYS_BACK)

只抓取最近 N 天内发布的公告。API 按 `-pdate` 降序返回,遇到早于截止日期的记录即停止翻页,避免无谓请求。

```ini
DAYS_BACK=2
```

调大可抓更早的历史公告(翻页更多)。

---

## 📄 输出

- `procurement_notices_YYYYMMDD_HHMMSS.json` - 结构化结果
- `procurement_notices_YYYYMMDD_HHMMSS.csv` - Excel 可打开
- `notices.db` - SQLite 去重表(跨次运行去重)
- 钉钉群 Markdown 消息(含标题、匹配关键词、发布日期、查看链接)

---

**最后更新**: 2026-07-13
