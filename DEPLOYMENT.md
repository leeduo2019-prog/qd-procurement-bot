# 青岛政府采购爬虫 - 服务器部署指南

## 📋 部署方案概述

本指南帮助你在一台**独立的电脑**上部署爬虫，实现 24 小时自动运行。

### 推荐配置

| 配置项 | 最低要求 | 推荐配置 |
|--------|----------|----------|
| 操作系统 | Windows 10 / Linux | Windows 10+ / Ubuntu 20.04+ |
| CPU | 双核 | 四核及以上 |
| 内存 | 4GB | 8GB 及以上 |
| 硬盘 | 10GB 可用空间 | 20GB 及以上 |
| 网络 | 稳定互联网连接 | 有线连接（更稳定） |

---

## 🖥️ 方案一：Windows 电脑部署（推荐新手）

### 步骤 1：准备部署机器

1. **选择一台电脑**作为爬虫服务器
   - 可以是旧电脑、闲置笔记本
   - 保持开机状态，连接电源
   - 连接稳定的网络（推荐有线网络）

2. **系统设置**
   - 关闭自动睡眠：控制面板 → 电源选项 → 从不睡眠
   - 关闭自动更新重启：设置 → 更新 → 暂停更新
   - 设置固定 IP（可选）：避免 DHCP 变化影响

### 步骤 2：安装 Python 环境

1. 下载 Python 3.10+：https://www.python.org/downloads/
2. 安装时勾选 **"Add Python to PATH"**
3. 验证安装：
   ```cmd
   python --version
   pip --version
   ```

### 步骤 3：部署爬虫代码

1. 将项目文件夹复制到目标电脑，例如：
   ```
   D:\projects\qd-procurement-crawler
   ```

2. 或者使用 Git 克隆（如果目标电脑有 Git）：
   ```cmd
   cd D:\projects
   git clone <你的仓库地址> qd-procurement-crawler
   ```

### 步骤 4：安装依赖

```cmd
cd D:\projects\qd-procurement-crawler
pip install -r requirements.txt
```

如果遇到网络慢的问题，使用国内镜像：
```cmd
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
```

### 步骤 5：配置钉钉推送

```cmd
copy .env.example .env
```

用记事本编辑 `.env` 文件：
```ini
DINGTALK_WEBHOOK=https://oapi.dingtalk.com/robot/send?access_token=YOUR_TOKEN
DINGTALK_SECRET=SECxxx
KEYWORDS=造价，审计，预算，决算，结算
RUN_MODE=daily
```

### 步骤 6：首次测试运行

```cmd
python main.py
```

观察输出，确认：
- ✅ 能正常访问网站
- ✅ 能抓取到数据
- ✅ 钉钉推送能正常发送

### 步骤 7：配置开机自启动和定时任务

#### 方法 A：使用任务计划程序（推荐）

1. 按 `Win + R`，输入 `taskschd.msc`，回车

2. 点击右侧 **"创建基本任务"**

3. 填写任务信息：
   - 名称：`青岛政府采购爬虫`
   - 描述：`每天自动抓取政府采购公告并推送钉钉`

4. 触发器选择：**每天**
   - 设置时间：`09:00`（或其他时间）

5. 操作选择：**启动程序**
   - 程序/脚本：`python.exe`（填写完整路径，如 `C:\Python310\python.exe`）
   - 添加参数：`main.py`
   - 起始于：`D:\projects\qd-procurement-crawler`

6. 完成创建后，右键任务 → **属性**：
   - ✅ 勾选"不管用户是否登录都要运行"
   - ✅ 勾选"使用最高权限运行"
   - 设置 → ✅ "如果任务失败，重新启动每隔：1 分钟"
   - 设置 → ✅ "尝试重新启动次数：3"

#### 方法 B：使用批处理 + 开机启动

1. 创建启动脚本 `start_crawler.bat`：
   ```batch
   @echo off
   cd /d D:\projects\qd-procurement-crawler
   python main.py >> crawler.log 2>&1
   ```

2. 按 `Win + R`，输入 `shell:startup`，回车

3. 将 `start_crawler.bat` 的快捷方式放到打开的文件夹中

> ⚠️ 此方法需要电脑保持登录状态，推荐使用方法 A

---

## 🐧 方案二：Linux 服务器部署（推荐进阶）

### 步骤 1：系统准备

推荐使用 **Ubuntu 20.04 LTS** 或 **CentOS 7+**

```bash
# 更新系统
sudo apt update && sudo apt upgrade -y  # Ubuntu/Debian
# 或
sudo yum update -y  # CentOS/RHEL
```

### 步骤 2：安装 Python 环境

**Ubuntu/Debian:**
```bash
sudo apt install -y python3 python3-pip python3-venv
```

**CentOS/RHEL:**
```bash
sudo yum install -y python3 python3-pip
```

### 步骤 3：安装 Chrome 浏览器和驱动

```bash
# 安装 Chrome
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo dpkg -i google-chrome-stable_current_amd64.deb
sudo apt-get install -f -y

# 安装 ChromeDriver
sudo apt install -y chromium-chromedriver
```

### 步骤 4：部署代码

```bash
# 创建目录
sudo mkdir -p /opt/qd-crawler
sudo chown $USER:$USER /opt/qd-crawler

# 复制代码
cd /opt/qd-crawler
# 方式 1: 使用 Git
git clone <你的仓库地址> .
# 方式 2: 使用 SCP 从本机复制
# scp -r /本地路径/* user@服务器 IP:/opt/qd-crawler/
```

### 步骤 5：创建虚拟环境

```bash
cd /opt/qd-crawler
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
```

### 步骤 6：配置环境变量

```bash
cp .env.example .env
nano .env  # 编辑配置
```

### 步骤 7：创建系统服务（Systemd）

创建服务文件：
```bash
sudo nano /etc/systemd/system/qd-crawler.service
```

内容如下：
```ini
[Unit]
Description=青岛政府采购爬虫
After=network.target

[Service]
Type=oneshot
User=your_username
WorkingDirectory=/opt/qd-crawler
Environment="PATH=/opt/qd-crawler/venv/bin"
ExecStart=/opt/qd-crawler/venv/bin/python main.py
StandardOutput=append:/var/log/qd-crawler.log
StandardError=append:/var/log/qd-crawler.error

[Install]
WantedBy=multi-user.target
```

创建定时器文件：
```bash
sudo nano /etc/systemd/system/qd-crawler.timer
```

内容如下：
```ini
[Unit]
Description=每天运行青岛政府采购爬虫
Requires=qd-crawler.service

[Timer]
OnCalendar=*-*-* 09:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

启用服务：
```bash
sudo systemctl daemon-reload
sudo systemctl enable qd-crawler.timer
sudo systemctl start qd-crawler.timer

# 查看状态
sudo systemctl status qd-crawler.timer
# 查看日志
sudo journalctl -u qd-crawler.service
```

### 步骤 8：配置 Cron（替代方案）

```bash
crontab -e
```

添加：
```bash
0 9 * * * cd /opt/qd-crawler && /opt/qd-crawler/venv/bin/python main.py >> /var/log/qd-crawler.log 2>&1
```

---

## 📊 监控和维护

### 日志查看

**Windows:**
```cmd
# 查看最新日志
type crawler.log

# 实时监控（需要 PowerShell）
Get-Content crawler.log -Wait -Tail 50
```

**Linux:**
```bash
# 实时查看日志
tail -f /var/log/qd-crawler.log

# 查看最近 100 行
tail -n 100 /var/log/qd-crawler.log
```

### 推送监控

- 每天检查钉钉群，确认收到推送
- 如果某天没收到，检查服务器状态和日志

### 远程管理

**Windows 远程桌面:**
1. 设置 → 系统 → 远程桌面 → 启用
2. 使用"远程桌面连接"从本机连接

**Linux SSH:**
```bash
# 从本机连接
ssh username@服务器 IP

# 配置 SSH 密钥免密码登录（推荐）
ssh-keygen -t rsa
ssh-copy-id username@服务器 IP
```

### 异常处理

| 问题 | 可能原因 | 解决方案 |
|------|----------|----------|
| 钉钉推送没收到 | Webhook 配置错误 | 检查 `.env` 配置，测试手动运行 |
| 爬虫报错 | 网站结构变化 | 查看日志，调整选择器 |
| 电脑自动关机 | 电源/睡眠设置 | 检查电源选项，关闭睡眠 |
| 网络断开 | 网络不稳定 | 使用有线连接，检查路由器 |

---

## 🔒 安全建议

### 1. 服务器安全

- 设置强密码
- 开启防火墙
- 定期更新系统
- 安装杀毒软件（Windows）

### 2. Webhook 安全

- `.env` 文件不要上传到公开仓库
- 钉钉 Webhook 和加签密钥定期更换
- 不要在公开场合泄露 Webhook 地址

### 3. 数据安全

- 定期备份 `.env` 配置文件
- 重要数据导出到云存储
- 设置日志轮转，避免磁盘占满

---

## 📝 部署检查清单

部署完成后，逐项检查：

- [ ] Python 环境安装成功
- [ ] 依赖包安装完成
- [ ] `.env` 配置正确
- [ ] 手动运行 `python main.py` 成功
- [ ] 收到钉钉测试推送
- [ ] 定时任务配置完成
- [ ] 电脑睡眠已关闭
- [ ] 网络连接稳定
- [ ] 日志文件正常生成
- [ ] 远程管理已配置（可选）

---

## 🆘 常见问题

### Q1: 电脑关机后还能运行吗？
**A:** 不能。爬虫需要电脑处于开机状态。建议：
- 保持电脑常开
- 设置 BIOS 通电自启（部分主板支持）
- 考虑使用云服务器替代

### Q2: 可以部署到笔记本电脑吗？
**A:** 可以，但注意：
- 合盖后可能进入睡眠，需要修改电源设置
- 长期插电使用，注意电池保养

### Q3: 多台电脑可以运行同一个爬虫吗？
**A:** 可以，但没必要。一台足够，多台会导致重复数据。

### Q4: 如何确认爬虫在正常运行？
**A:**
- 每天检查钉钉群是否收到推送
- 定期查看日志文件
- 设置异常告警（如连续 3 天没推送，发送告警）

### Q5: 服务器重启后需要手动启动吗？
**A:** 
- Windows 任务计划程序：配置"不管用户是否登录都要运行"可自动执行
- Linux Systemd Timer：会自动运行
- Linux Cron：会自动运行

---

## 📞 技术支持

如遇到问题：
1. 查看日志文件定位错误
2. 检查 `.env` 配置是否正确
3. 确认网络连接正常
4. 搜索错误信息

---

**文档版本**: v1.0  
**最后更新**: 2024-04-05
