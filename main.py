#!/usr/bin/env python3
"""
青岛政府采购爬虫 - 主程序
每天自动运行，抓取匹配的公告并推送到钉钉群

v2.0 - 改进版：
- 使用结构化日志
- 支持去重（无新公告时跳过推送）
- 支持区域类型配置
"""

import sys
import os
from datetime import datetime
from crawler import ProcurementCrawler, logger
from dingtalk_notifier import send_email


def main():
    """主函数"""
    logger.info("=" * 60)
    logger.info("青岛政府采购爬虫 - 自动运行")
    logger.info("=" * 60)
    logger.info("启动时间: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # 从环境变量读取区域类型（默认 all）
    area_type = os.getenv("AREA_TYPE", "all")

    try:
        crawler = ProcurementCrawler(area_type=area_type)
        results = crawler.crawl(max_pages=5)

        logger.info("找到 %d 条新匹配公告", len(results))

        # 保存结果（仅当有结果时）
        if results:
            crawler.save_to_file()
            crawler.save_to_csv()

        # 无论是否有结果都发送钉钉推送
        logger.info("正在发送钉钉推送...")
        success = send_email(results)

        if success:
            logger.info("✓ 任务完成！")
            return 0
        else:
            logger.error("× 钉钉推送失败，请检查配置")
            return 1

    except KeyboardInterrupt:
        logger.info("\n用户中断程序")
        return 1

    except Exception as e:
        logger.error("× 程序异常: %s", e, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
