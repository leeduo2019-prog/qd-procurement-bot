#!/bin/bash
# Linux Cron 定时任务配置脚本
# 自动添加每天运行爬虫的 Cron 任务

echo "============================================================"
echo "青岛政府采购爬虫 - Linux Cron 配置"
echo "============================================================"
echo ""

# 获取当前目录
CURRENT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 获取 Python3 路径
PYTHON_PATH=$(which python3)
if [ -z "$PYTHON_PATH" ]; then
    PYTHON_PATH=$(which python)
fi

if [ -z "$PYTHON_PATH" ]; then
    echo "[错误] 未找到 Python3，请先安装 Python3"
    exit 1
fi

echo "[信息] Python 路径：$PYTHON_PATH"
echo "[信息] 项目路径：$CURRENT_DIR"
echo ""

# Cron 任务配置（每天早上 9:00 运行）
CRON_JOB="0 9 * * * cd $CURRENT_DIR && $PYTHON_PATH main.py >> $CURRENT_DIR/crawler.log 2>&1"

echo "[信息] Cron 任务配置:"
echo "$CRON_JOB"
echo ""

# 添加到 crontab
(crontab -l 2>/dev/null | grep -v "青岛政府采购爬虫"; echo "$CRON_JOB") | crontab -

if [ $? -eq 0 ]; then
    echo "============================================================"
    echo "[成功] Cron 任务添加完成！"
    echo "============================================================"
    echo ""
    echo "运行时间：每天早上 9:00"
    echo "日志文件：$CURRENT_DIR/crawler.log"
    echo ""
    echo "查看任务：crontab -l"
    echo "删除任务：crontab -e (手动删除对应行)"
    echo "查看日志：tail -f $CURRENT_DIR/crawler.log"
    echo ""
else
    echo "[错误] Cron 任务添加失败"
    exit 1
fi
