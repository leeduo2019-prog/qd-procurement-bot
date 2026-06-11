# 增强版爬虫 - 标签页切换功能说明

## 📋 功能概述

增强版爬虫 (`crawler_enhanced.py`) 支持分别爬取青岛政府采购网上的**两个标签页**：

| 标签页 | 说明 | 包含内容 |
|--------|------|----------|
| **青岛市** | 青岛市本级采购公告 | 青岛市级单位（市教育局、市财政局等） |
| **各市区** | 各区市采购公告 | 市南区、市北区、李沧区、崂山区、黄岛区、城阳区、即墨区、胶州市、平度市、莱西市 |

---

## 🚀 快速使用

### 方式 1：爬取全部（青岛市 + 各市区）

```bash
cd /mnt/f/opencode/wlpc-1/qd-procurement-crawler
python3 crawler_enhanced.py
```

**效果**：自动爬取两个标签页，分别保存为：
- `qingdao_notices.json` - 青岛市本级公告
- `districts_notices.json` - 各市区公告
- `all_notices.json` - 全部公告（合并）

---

### 方式 2：在代码中指定爬取范围

```python
from crawler_enhanced import EnhancedProcurementCrawler

# 只爬取"青岛市"标签页
crawler = EnhancedProcurementCrawler(area_type="qingdao")
results = crawler.crawl(max_pages=5)

# 只爬取"各市区"标签页
crawler = EnhancedProcurementCrawler(area_type="districts")
results = crawler.crawl(max_pages=5)

# 爬取全部（两个标签页都爬）
crawler = EnhancedProcurementCrawler(area_type="all")
results = crawler.crawl(max_pages=5)
```

**area_type 参数说明**：

| 值 | 说明 |
|----|------|
| `"all"` | 爬取全部（青岛市 + 各市区） |
| `"qingdao"` | 只爬取青岛市本级 |
| `"districts"` | 只爬取各市区 |

---

## 📊 输出数据说明

### JSON 文件格式

```json
[
  {
    "title": "青岛市教育局教学设备采购公告",
    "link": "http://zfcg.qingdao.gov.cn/xxx",
    "publish_date": "2026-04-05",
    "area_type": "qingdao",
    "matched_keywords": ["预算"],
    "crawl_time": "2026-04-05 09:00:00"
  },
  {
    "title": "市南区财政局审计服务项目招标公告",
    "link": "http://zfcg.qingdao.gov.cn/yyy",
    "publish_date": "2026-04-04",
    "area_type": "districts",
    "matched_keywords": ["审计"],
    "crawl_time": "2026-04-05 09:00:00"
  }
]
```

**新增字段**：
- `area_type`: 标识公告来源
  - `"qingdao"` - 青岛市本级
  - `"districts"` - 各市区

---

### CSV 文件格式

| title | link | publish_date | area_type | matched_keywords | crawl_time |
|-------|------|--------------|-----------|------------------|------------|
| 青岛市教育局... | http://... | 2026-04-05 | qingdao | ["预算"] | 2026-04-05 09:00:00 |
| 市南区财政局... | http://... | 2026-04-04 | districts | ["审计"] | 2026-04-05 09:00:00 |

---

## 🔧 工作原理

### 标签页切换机制

```
1. 访问基础 URL
   http://zfcg.qingdao.gov.cn/qdsite/#/site-list-varied?colCode=04
   ↓
2. 默认显示"青岛市"标签页
   ↓
3. 查找并点击"各市区"标签按钮
   XPath: //*[contains(text(), '各市区')]
   ↓
4. 等待页面刷新（JavaScript 重新加载数据）
   ↓
5. 爬取"各市区"标签页内容
   ↓
6. 重复翻页操作
```

### 关键技术点

**1. 标签页定位**
```python
# XPath 查找包含"各市区"文本的元素
tab_element = driver.find_element(By.XPATH, "//*[contains(text(), '各市区')]")
tab_element.click()
```

**2. 等待页面加载**
```python
# 等待标签页切换后的内容刷新
time.sleep(2)
```

**3. 数据标记**
```python
# 每条公告都标记来源
notice["area_type"] = "qingdao"  # 或 "districts"
```

---

## 📝 实际运行示例

### 示例输出

```
============================================================
青岛政府采购网爬虫 - 增强版（支持标签页切换）
============================================================

【示例 1】只爬取青岛市本级公告

[==================================================]
[2026-04-05 09:00:00] 开始爬取...
区域类型：qingdao
关键词：造价，审计，预算，决算，结算
最大页数：3
[==================================================]

>> 开始爬取【青岛市】标签页

  爬取第 1 页...
  第 1 页找到 15 条公告
    ✓ 匹配：青岛市财政局预算绩效评价项目... [预算]
    ✓ 匹配：青岛市审计局 services 采购... [审计]
    
  爬取第 2 页...
  第 2 页找到 12 条公告
    ✓ 匹配：青岛市教育局决算公开... [决算]
    
  爬取第 3 页...
  第 3 页找到 10 条公告
  已到达最后一页

>> 【青岛市】标签页爬取完成

[==================================================]
[2026-04-05 09:01:30] 爬取完成！共找到 8 条匹配的公告
[==================================================]

结果已保存到：qingdao_notices.json
结果已保存到 CSV: qingdao_notices.csv
```

---

## 🎯 使用场景

### 场景 1：只关心市级单位采购

```python
# 只爬取"青岛市"标签页
crawler = EnhancedProcurementCrawler(area_type="qingdao")
results = crawler.crawl(max_pages=5)
```

**适合用户**：只投标市级单位项目的供应商

---

### 场景 2：只关心区市采购

```python
# 只爬取"各市区"标签页
crawler = EnhancedProcurementCrawler(area_type="districts")
results = crawler.crawl(max_pages=5)
```

**适合用户**：只投标区市单位项目的供应商

---

### 场景 3：全部都要（默认）

```python
# 爬取全部
crawler = EnhancedProcurementCrawler(area_type="all")
results = crawler.crawl(max_pages=5)
```

**适合用户**：全市各级单位项目都感兴趣的供应商

---

## 🔍 按区市进一步筛选

如果需要从"各市区"数据中筛选特定区市，可以使用关键词过滤：

### 方法 1：在关键词中添加区市名称

```python
# .env 文件
KEYWORDS=市南区，市北区，李沧区，崂山区，造价，审计，预算
```

### 方法 2：爬取后过滤

```python
from crawler_enhanced import EnhancedProcurementCrawler

# 爬取各市区
crawler = EnhancedProcurementCrawler(area_type="districts")
results = crawler.crawl(max_pages=5)

# 过滤出市南区公告
shinan_notices = [r for r in results if "市南" in r["title"]]

# 过滤出黄岛区公告
huangdao_notices = [r for r in results if "黄岛" in r["title"] or "西海岸" in r["title"]]
```

---

## 📋 区市名称对照表

| 区市名称 | 常见表述 | 筛选关键词 |
|----------|----------|------------|
| 市南区 | 市南区 | `市南` |
| 市北区 | 市北区 | `市北` |
| 李沧区 | 李沧区 | `李沧` |
| 崂山区 | 崂山区 | `崂山` |
| 西海岸新区/黄岛区 | 黄岛区、西海岸新区 | `黄岛` 或 `西海岸` |
| 城阳区 | 城阳区 | `城阳` |
| 即墨区 | 即墨区 | `即墨` |
| 胶州市 | 胶州市 | `胶州` |
| 平度市 | 平度市 | `平度` |
| 莱西市 | 莱西市 | `莱西` |

---

## ⚙️ 集成到主程序

如果想替换原来的 `main.py` 使用增强版爬虫：

### 修改 `main.py`

```python
#!/usr/bin/env python3
"""
青岛政府采购爬虫 - 主程序（增强版）
支持分别爬取青岛市和各市区公告
"""

import sys
from datetime import datetime
from crawler_enhanced import EnhancedProcurementCrawler
from dingtalk_notifier import send_email


def main():
    """主函数"""
    print("=" * 60)
    print("青岛政府采购爬虫 - 增强版")
    print("=" * 60)
    print(f"启动时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    try:
        # 使用增强版爬虫（爬取全部）
        crawler = EnhancedProcurementCrawler(area_type="all")
        results = crawler.crawl(max_pages=5)

        if not results:
            print("\n未找到匹配的公告")
            print("程序结束")
            return 0

        print(f"\n找到 {len(results)} 条匹配公告")

        # 按 area_type 分组保存
        qingdao_results = [r for r in results if r["area_type"] == "qingdao"]
        districts_results = [r for r in results if r["area_type"] == "districts"]

        if qingdao_results:
            with open(f"qingdao_notices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", "w", encoding="utf-8") as f:
                import json
                json.dump(qingdao_results, f, ensure_ascii=False, indent=2)
            print(f"✓ 青岛市本级公告已保存 ({len(qingdao_results)} 条)")

        if districts_results:
            with open(f"districts_notices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", "w", encoding="utf-8") as f:
                import json
                json.dump(districts_results, f, ensure_ascii=False, indent=2)
            print(f"✓ 各市区公告已保存 ({len(districts_results)} 条)")

        print("\n正在发送钉钉推送...")
        success = send_email(results)

        if success:
            print("\n✓ 任务完成！")
            return 0
        else:
            print("\n× 钉钉推送失败，请检查配置")
            return 1

    except KeyboardInterrupt:
        print("\n\n用户中断程序")
        return 1

    except Exception as e:
        print(f"\n× 程序异常：{e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
```

---

## 🐛 故障排查

### 问题 1：无法切换到"各市区"标签页

**可能原因**：
- 网站结构变化，标签页选择器失效
- 网络加载慢，超时

**解决方法**：
```python
# 增加等待时间
time.sleep(5)  # 原来是 2 秒

# 或修改 XPath 选择器
xpath = "//*[contains(text(), '各市区') or contains(text(), '市区')]"
```

---

### 问题 2：爬取到的数据 area_type 都是"qingdao"

**原因**：标签页切换失败，只爬取了第一个标签页

**检查方法**：
```python
# 查看日志中是否有"已切换到'各市区'标签页"
# 如果没有，说明切换失败
```

**解决方法**：
1. 手动访问网站，确认"各市区"标签是否存在
2. 检查 XPath 选择器是否正确
3. 增加等待时间

---

### 问题 3：两个标签页数据重复

**原因**：某些公告可能同时出现在两个标签页

**解决方法**：
```python
# 去重（根据标题）
seen_titles = set()
unique_results = []
for r in results:
    if r["title"] not in seen_titles:
        seen_titles.add(r["title"])
        unique_results.append(r)
```

---

## 📊 性能对比

| 爬虫版本 | 爬取范围 | 耗时 | 数据量 |
|----------|----------|------|--------|
| 原版 (`crawler.py`) | 仅默认标签页 | ~40 秒 | 较少 |
| 增强版 (`crawler_enhanced.py`) | 青岛市 + 各市区 | ~80 秒 | 较多（约 2 倍） |

---

## ✅ 总结

**增强版爬虫优势**：
- ✅ 支持分别爬取"青岛市"和"各市区"
- ✅ 数据标记来源，便于筛选
- ✅ 灵活配置爬取范围
- ✅ 输出格式兼容原版

**推荐使用场景**：
- 需要区分市级和区级公告
- 只关心特定区域的采购信息
- 需要完整覆盖全市各级单位采购

---

**文档版本**: 1.0  
**最后更新**: 2026-04-05  
**适用文件**: `crawler_enhanced.py`
