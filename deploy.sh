#!/bin/bash
# 青岛政府采购爬虫 - Linux 快速部署脚本
# 适用于 Ubuntu/Debian/CentOS

set -e

echo "============================================================"
echo "青岛政府采购爬虫 - 一键部署脚本"
echo "============================================================"
echo ""

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 检查是否以 root 运行
if [ "$EUID" -ne 0 ]; then 
    echo -e "${YELLOW}[警告] 建议使用 sudo 运行此脚本${NC}"
    echo "是否继续？(y/n)"
    read -r response
    if [[ ! "$response" =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# 检测系统类型
if [ -f /etc/debian_version ]; then
    SYSTEM="debian"
    PM="apt"
elif [ -f /etc/redhat-release ]; then
    SYSTEM="redhat"
    PM="yum"
else
    echo -e "${RED}[错误] 不支持的系统，仅支持 Debian/Ubuntu/CentOS${NC}"
    exit 1
fi

echo -e "${GREEN}[信息] 检测到系统：$SYSTEM${NC}"

# 1. 安装系统依赖
echo ""
echo "============================================================"
echo "步骤 1: 安装系统依赖"
echo "============================================================"

if [ "$SYSTEM" = "debian" ]; then
    apt update
    apt install -y python3 python3-pip python3-venv wget curl
elif [ "$SYSTEM" = "redhat" ]; then
    yum install -y python3 python3-pip wget curl
fi

# 注:爬虫通过后端 API 获取数据(见 api_client.py),无需 Chrome/ChromeDriver。

echo -e "${GREEN}[完成] 系统依赖安装完成${NC}"

# 2. 创建安装目录
echo ""
echo "============================================================"
echo "步骤 2: 创建安装目录"
echo "============================================================"

INSTALL_DIR="/opt/qd-crawler"
mkdir -p "$INSTALL_DIR"
chown "$SUDO_USER:$SUDO_USER" "$INSTALL_DIR" 2>/dev/null || chown "$(whoami):$(whoami)" "$INSTALL_DIR"

echo -e "${GREEN}[完成] 安装目录：$INSTALL_DIR${NC}"

# 3. 复制项目文件
echo ""
echo "============================================================"
echo "步骤 3: 复制项目文件"
echo "============================================================"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cp -r "$SCRIPT_DIR"/* "$INSTALL_DIR/"

echo -e "${GREEN}[完成] 项目文件复制完成${NC}"

# 4. 创建虚拟环境
echo ""
echo "============================================================"
echo "步骤 4: 创建 Python 虚拟环境"
echo "============================================================"

cd "$INSTALL_DIR"
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip -i https://pypi.tuna.tsinghua.edu.cn/simple
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

echo -e "${GREEN}[完成] Python 环境配置完成${NC}"

# 5. 配置环境变量
echo ""
echo "============================================================"
echo "步骤 5: 配置环境变量"
echo "============================================================"

if [ ! -f .env ]; then
    cp .env.example .env
    echo -e "${GREEN}[完成] 已创建 .env 文件${NC}"
    echo -e "${YELLOW}[注意] 请编辑 $INSTALL_DIR/.env 配置钉钉 Webhook 信息${NC}"
else
    echo -e "${GREEN}[信息] .env 文件已存在${NC}"
fi

# 6. 创建系统服务
echo ""
echo "============================================================"
echo "步骤 6: 配置 Systemd 定时任务"
echo "============================================================"

USERNAME="${SUDO_USER:-$(whoami)}"

# 创建 service 文件
cat > /etc/systemd/system/qd-crawler.service << EOF
[Unit]
Description=青岛政府采购爬虫
After=network.target

[Service]
Type=oneshot
User=$USERNAME
WorkingDirectory=$INSTALL_DIR
Environment="PATH=$INSTALL_DIR/venv/bin"
ExecStart=$INSTALL_DIR/venv/bin/python main.py
StandardOutput=append:/var/log/qd-crawler.log
StandardError=append:/var/log/qd-crawler.error

[Install]
WantedBy=multi-user.target
EOF

# 创建 timer 文件
cat > /etc/systemd/system/qd-crawler.timer << EOF
[Unit]
Description=每天运行青岛政府采购爬虫
Requires=qd-crawler.service

[Timer]
OnCalendar=*-*-* 09:00:00
Persistent=true

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
systemctl enable qd-crawler.timer
systemctl start qd-crawler.timer

echo -e "${GREEN}[完成] Systemd 服务配置完成${NC}"

# 7. 测试运行
echo ""
echo "============================================================"
echo "步骤 7: 测试运行"
echo "============================================================"

echo "是否现在测试运行爬虫？(y/n)"
read -r response
if [[ "$response" =~ ^[Yy]$ ]]; then
    cd "$INSTALL_DIR"
    python main.py
fi

# 完成
echo ""
echo "============================================================"
echo -e "${GREEN}✓ 部署完成！${NC}"
echo "============================================================"
echo ""
echo "安装目录：$INSTALL_DIR"
echo "配置文件：$INSTALL_DIR/.env"
echo "日志文件：/var/log/qd-crawler.log"
echo ""
echo "下一步:"
echo "1. 编辑配置文件：nano $INSTALL_DIR/.env"
echo "2. 配置钉钉 Webhook 信息"
echo "3. 测试运行：cd $INSTALL_DIR && python main.py"
echo ""
echo "管理命令:"
echo "  查看状态：systemctl status qd-crawler.timer"
echo "  查看日志：journalctl -u qd-crawler.service"
echo "  手动运行：cd $INSTALL_DIR && python main.py"
echo "  重启服务：systemctl restart qd-crawler.timer"
echo ""
