# 快速开始 - 5 分钟部署爬虫

## 🎯 部署到另一台电脑

### 步骤 1：准备目标电脑

找一台可以**长期开机**的电脑：
- 旧笔记本、台式机都可以
- 连接稳定电源和网络
- 关闭睡眠模式（控制面板 → 电源选项）

### 步骤 2：复制项目

将整个 `qd-procurement-crawler` 文件夹复制到目标电脑，例如：
- Windows: `D:\projects\qd-procurement-crawler`
- Linux: `/opt/qd-crawler`

### 步骤 3：一键部署

**Windows 系统：**
```bash
# 双击运行此文件
deploy.bat
```

**Linux 系统：**
```bash
chmod +x deploy.sh
sudo ./deploy.sh
```

### 步骤 4：配置钉钉推送

编辑 `.env` 文件（用记事本或 nano）：

```ini
DINGTALK_WEBHOOK=https://oapi.dingtalk.com/robot/send?access_token=YOUR_TOKEN
DINGTALK_SECRET=SECxxx  # 加签密钥，推荐配置
```

> 📌 **获取钉钉 Webhook**：
> 1. 打开钉钉群 → 群设置 → 智能群助手
> 2. 添加机器人 → 自定义
> 3. 安全设置选择"加签"，复制 Secret 和 Webhook

### 步骤 5：测试运行

```bash
# 进入项目目录
cd D:\projects\qd-procurement-crawler  # Windows 示例

# 激活虚拟环境（如果部署脚本已创建）
venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux

# 运行爬虫
python main.py
```

如果看到"✓ 任务完成！"，说明配置正确。

### 步骤 6：确认钉钉推送

检查钉钉群，应该收到一条推送消息，包含：
- 抓取的公告数量
- 每条公告的标题和链接

### 步骤 7：设置每天自动运行

部署脚本会自动配置定时任务（每天早上 9 点运行）。

**验证定时任务：**

Windows:
```bash
# 打开任务计划程序
taskschd.msc
```

Linux:
```bash
# 查看定时任务状态
systemctl status qd-crawler.timer
```

---

## ✅ 完成！

现在爬虫会：
- ✅ 每天早上 9 点自动运行
- ✅ 抓取匹配的公告
- ✅ 推送到钉钉群
- ✅ 保存数据到本地

---

## 🔧 日常管理

### 查看日志

Windows:
```bash
type crawler.log
```

Linux:
```bash
tail -f /var/log/qd-crawler.log
```

### 手动运行

```bash
python main.py
```

### 修改运行时间

**Windows:** 任务计划程序 → 找到"青岛政府采购爬虫" → 属性 → 触发器 → 编辑

**Linux:** 编辑 `/etc/systemd/system/qd-crawler.timer`，修改 `OnCalendar` 行

---

## ❓ 常见问题

**Q: 电脑关机了还能运行吗？**  
A: 不能，需要保持开机状态。

**Q: 可以合上笔记本盖子吗？**  
A: 需要设置"合盖不睡眠"：
- Windows: 控制面板 → 电源选项 → 选择关闭笔记本盖的功能 → 不采取任何操作

**Q: 如何确认爬虫正常工作？**  
A: 每天检查钉钉群，收到推送即表示正常运行。

**Q: 部署脚本报错怎么办？**  
A: 按步骤手动部署：
1. 安装 Python 3.10+
2. `pip install -r requirements.txt`
3. 配置 `.env` 文件
4. 手动配置定时任务

---

**详细文档**: 查看 `DEPLOYMENT.md` 获取完整部署指南
