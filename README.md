# 青岛政府采购爬虫

自动抓取青岛市政府采购网中涉及**造价、审计、预算、决算、结算**的采购公告，并推送到钉钉群。

## 功能特点

- ✅ 自动抓取政府采购公告
- ✅ 智能关键词匹配（造价、审计、预算、决算、结算）
- ✅ 自动推送钉钉通知
- ✅ 支持定时任务（每天自动运行）
- ✅ 结果保存为 JSON 和 CSV 格式
- ✅ Markdown 格式推送，清晰易读

## 快速开始

### 🚀 一键部署（推荐）

**Windows 系统：**
```bash
# 双击运行或在命令行执行
deploy.bat
```

**Linux 系统：**
```bash
# 赋予执行权限并运行
chmod +x deploy.sh
sudo ./deploy.sh
```

部署脚本会自动完成：
- Python 环境配置
- 依赖安装
- 配置文件创建
- 定时任务设置

### 📖 手动部署

#### 1. 安装依赖

```bash
cd qd-procurement-crawler
pip install -r requirements.txt
```

#### 2. 配置钉钉推送

复制环境变量配置文件：

```bash
cp .env.example .env
```

编辑 `.env` 文件，填写钉钉 Webhook 配置：

```ini
# 钉钉群机器人 Webhook 地址
DINGTALK_WEBHOOK=https://oapi.dingtalk.com/robot/send?access_token=YOUR_TOKEN

# 钉钉加签密钥（推荐配置，增强安全性）
DINGTALK_SECRET=SECxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

# 关键词配置
KEYWORDS=造价，审计，预算，决算，结算

# 运行模式
RUN_MODE=daily
```

### 3. 获取钉钉 Webhook

1. 打开钉钉群 → 群设置 → 智能群助手
2. 点击"添加机器人" → 选择"自定义"
3. 设置机器人名称（如"采购爬虫"）
4. 安全设置选择"加签"，复制 Secret
5. 复制 Webhook 地址，填入 `.env` 文件

### 4. 运行爬虫

**手动运行：**

```bash
python main.py
```

## 📅 定时任务配置

### Windows 系统

#### 方法一：使用一键部署脚本（最简单）

运行 Windows 部署脚本，自动配置定时任务：
```bash
deploy.bat
```

#### 方法二：使用任务计划程序（推荐）

1. 打开"任务计划程序"
2. 点击"创建基本任务"
3. 设置任务名称：`青岛政府采购爬虫`
4. 触发器：选择"每天"，设置时间（如 9:00）
5. 操作：选择"启动程序"
   - 程序/脚本：`python.exe`（填写完整路径）
   - 添加参数：`main.py`
   - 起始于：`F:\opencode\wlpc-1\qd-procurement-crawler`（项目路径）
6. 完成创建

#### 方法二：使用批处理脚本

运行 `setup_windows_task.bat` 自动配置定时任务：

```bash
setup_windows_task.bat
```

### Linux/Mac 系统

#### 方法一：使用一键部署脚本（最简单）

运行 Linux 部署脚本，自动配置 Systemd 定时任务：
```bash
chmod +x deploy.sh
sudo ./deploy.sh
```

#### 方法二：使用 Cron 定时任务

1. 编辑 crontab：

```bash
crontab -e
```

2. 添加以下行（每天早上 9 点运行）：

```bash
0 9 * * * cd /path/to/qd-procurement-crawler && /usr/bin/python3 main.py >> crawler.log 2>&1
```

3. 保存退出

#### 使用 systemd 定时器（Linux）

运行配置脚本：

```bash
sudo bash setup_linux_systemd.sh
```

## 输出说明

### 控制台输出

```
============================================================
青岛政府采购爬虫 - 自动运行
============================================================
启动时间：2024-04-05 09:00:00

[2024-04-05 09:00:01] 开始爬取...
关键词：造价，审计，预算，决算，结算
最大页数：5

[2024-04-05 09:00:05] 正在爬取第 1 页...
第 1 页找到 15 条公告
  ✓ 匹配：青岛市某项目造价咨询服务采购公告... [造价]
  ✓ 匹配：青岛市财政局预算绩效评价项目招标公告... [预算]

找到 2 条匹配公告
结果已保存到：procurement_notices_20240405_090005.json
结果已保存到 CSV: procurement_notices_20240405_090005.csv

正在发送钉钉推送...
✓ 钉钉推送成功！
  标题：青岛政府采购匹配公告 - 2024-04-05
  公告数量：2

✓ 任务完成！
```

### 输出文件

- `procurement_notices_YYYYMMDD_HHMMSS.json` - JSON 格式结果
- `procurement_notices_YYYYMMDD_HHMMSS.csv` - CSV 格式结果

### 钉钉推送内容

推送包含：
- 抓取时间和匹配数量
- 每条公告的标题、匹配关键词、发布日期、链接
- Markdown 格式，清晰易读

## 项目结构

```
qd-procurement-bot/
├── main.py                 # 主程序入口
├── crawler.py              # 爬虫编排(分页/匹配/去重)
├── api_client.py           # 后端 API 客户端(POST site-info/page)
├── notice_parser.py        # API record -> notice dict 映射
├── store.py                # SQLite 去重存储
├── dingtalk_notifier.py     # 钉钉推送模块
├── requirements.txt        # Python 依赖
├── .env.example            # 环境变量模板
├── .env                    # 实际配置（需自行创建）
├── README.md               # 本文件
├── SYSTEM_ARCHITECTURE.md  # 系统架构详解
├── tests/                  # pytest 测试(mock 隔离,不依赖网络)
└── .github/workflows/      # GitHub Actions 定时任务
```

## 自定义配置

### 修改关键词

编辑 `.env` 文件：

```ini
KEYWORDS=造价，审计，预算，决算，结算，招标，采购
```

### 修改爬取页数

编辑 `main.py`，修改 `crawler.crawl(max_pages=5)` 中的数字。

### 修改推送标题

编辑 `dingtalk_notifier.py` 中的 `send()` 方法。

## 常见问题

### 1. 钉钉推送失败

**原因：** Webhook 地址错误或未配置加签密钥

**解决：**
- 检查 `.env` 中的 `DINGTALK_WEBHOOK` 是否正确
- 确认钉钉群机器人已启用
- 如配置了加签，检查 `DINGTALK_SECRET` 是否正确

### 2. 爬虫无法获取数据

**原因：** 后端 API 变更或被 WAF 拦截

**解决：**
- 检查网络连接,确认 `http://zfcg.qingdao.gov.cn:58060` 可访问
- 查看日志是否出现 403(WAF 拦截):若 UA 失效,更新 `api_client.DEFAULT_HEADERS` 的 User-Agent
- 确认分类码 `colCode=0303`(采购公告)仍有效;栏目结构变更可参考 `SYSTEM_ARCHITECTURE.md` 重新核对

### 3. 钉钉推送失败但爬取正常

**原因：** Webhook 地址错误或未配置加签密钥

**解决：**
- 检查 `.env` 中的 `DINGTALK_WEBHOOK` 是否正确
- 确认钉钉群机器人已启用
- 如配置了加签，检查 `DINGTALK_SECRET` 是否正确

### 4. 如何更改钉钉群？

在目标钉钉群中添加新的自定义机器人，获取新的 Webhook 地址，更新 `.env` 文件即可。

## 注意事项

1. **遵守网站爬虫协议**：请合理使用爬虫，不要频繁访问影响网站正常运行
2. **Webhook 安全**：妥善保管 `.env` 文件，不要上传到公开代码仓库
3. **定时任务**：建议设置在非工作时间运行（如早上 9 点前）
4. **数据使用**：抓取的数据仅供个人使用，请勿用于商业用途

## 技术栈

- **Python 3.8+**
- **requests** - 直接调用后端 REST API 获取公告数据
- **python-dotenv** - 环境变量管理
- **SQLite** - 公告去重存储

> 爬虫不使用浏览器。青岛政府采购网是 Vue 单页应用,数据来自后端 API(`http://zfcg.qingdao.gov.cn:58060`),爬虫直接调用该 API,无需 Chrome / ChromeDriver。

## 许可证

MIT License

## 支持

如有问题，请检查：
1. `.env` 配置是否正确
2. 网络连接是否正常
3. Python 依赖是否安装完整
4. 查看控制台错误信息

---

**最后更新**: 2024-04-05
