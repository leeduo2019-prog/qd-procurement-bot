"""
钉钉群机器人 Webhook 推送模块
将爬取结果推送到钉钉群

v1.0:
- 使用 Markdown 格式消息
- 支持 timestamp + sign 加签验证
- 结构化日志 + 异常捕获
- 超时设置
"""

import os
import time
import hmac
import hashlib
import base64
import urllib.parse
import logging
import requests
from datetime import datetime
from typing import List, Dict, Optional
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("qd_crawler.dingtalk")


class DingTalkNotifier:
    """钉钉群机器人推送器"""

    def __init__(self):
        self.webhook = os.getenv("DINGTALK_WEBHOOK", "")
        self.secret = os.getenv("DINGTALK_SECRET", "")
        self.timeout = int(os.getenv("DINGTALK_TIMEOUT", "10"))

    def _validate_config(self) -> bool:
        """验证配置是否完整"""
        if not self.webhook:
            logger.error("钉钉 Webhook 未配置")
            logger.error("请在 .env 文件中配置 DINGTALK_WEBHOOK")
            logger.info(
                "\n配置说明:\n"
                "  DINGTALK_WEBHOOK=https://oapi.dingtalk.com/robot/send?access_token=xxx\n"
                "  DINGTALK_SECRET=SECxxx (加签密钥，可选但推荐)\n"
                "  DINGTALK_TIMEOUT=10 (请求超时秒数，可选)"
            )
            return False
        return True

    def _generate_sign(self) -> dict:
        """生成钉钉加签参数"""
        if not self.secret:
            return {}

        timestamp = str(round(time.time() * 1000))
        string_to_sign = f"{timestamp}\n{self.secret}"
        hmac_code = hmac.new(
            self.secret.encode("utf-8"),
            string_to_sign.encode("utf-8"),
            digestmod=hashlib.sha256,
        ).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))

        return {"timestamp": timestamp, "sign": sign}

    def send(self, notices: List[Dict], subject: Optional[str] = None) -> bool:
        """
        发送公告到钉钉群

        Args:
            notices: 公告列表
            subject: 消息标题

        Returns:
            是否发送成功
        """
        if not self._validate_config():
            return False

        date_str = datetime.now().strftime("%Y-%m-%d")
        if not subject:
            if notices:
                subject = f"青岛政府采购匹配公告 - {date_str}"
            else:
                subject = f"青岛政府采购每日报告 - {date_str}"

        try:
            # 构建 markdown 内容
            markdown_content = self._generate_markdown(notices, subject)

            # 构建请求 URL（含加签参数）
            url = self.webhook
            sign_params = self._generate_sign()
            if sign_params:
                url += f"&timestamp={sign_params['timestamp']}&sign={sign_params['sign']}"

            # 构建请求体
            payload = {
                "msgtype": "markdown",
                "markdown": {
                    "title": subject,
                    "text": markdown_content,
                },
            }

            # 发送请求
            response = requests.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=self.timeout,
            )
            response.raise_for_status()

            result = response.json()
            if result.get("errcode") == 0:
                logger.info("✓ 钉钉推送成功！")
                logger.info("  标题: %s", subject)
                logger.info("  公告数量: %d", len(notices))
                return True
            else:
                logger.error("钉钉推送失败: %s", result)
                return False

        except requests.exceptions.Timeout:
            logger.error("钉钉推送超时 (超时设置: %ds)", self.timeout)
            return False
        except requests.exceptions.RequestException as e:
            logger.error("钉钉推送请求异常: %s", e)
            return False
        except Exception as e:
            logger.error("钉钉推送失败: %s", e, exc_info=True)
            return False

    def _generate_markdown(self, notices: List[Dict], subject: str) -> str:
        """生成钉钉 Markdown 消息内容"""
        date_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        keywords = "造价，审计，预算，决算，结算"

        if not notices:
            # 无公告时的简洁模板
            content = f"""## {subject}

> 今日未有目标公告

---

**关键词**: {keywords}
**抓取时间**: {date_str}

---
*此消息由青岛政府采购爬虫自动生成*"""
        else:
            # 有公告时的详细模板
            content = f"""## {subject}

> 匹配数量：**{len(notices)}** 条

---

**关键词**: {keywords}
**抓取时间**: {date_str}

---

"""
            for i, notice in enumerate(notices, 1):
                title = notice.get("title", "未知项目")
                link = notice.get("link", "#")
                publish_date = notice.get("publish_date", "未知")
                matched_keywords = notice.get("matched_keywords", [])
                keyword_str = "、".join(matched_keywords) if matched_keywords else "无"

                content += f"""### {i}. {title}

- **匹配关键词**: {keyword_str}
- **发布日期**: {publish_date}
- [查看公告 →]({link})

---

"""
            content += "*此消息由青岛政府采购爬虫自动生成*"

        return content


def send_email(notices: List[Dict]) -> bool:
    """
    便捷函数：发送公告到钉钉群
    保持与原有 send_email 函数签名一致，方便直接替换
    """
    notifier = DingTalkNotifier()
    return notifier.send(notices)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_notices = [
        {
            "title": "青岛市某项目造价咨询服务采购公告",
            "link": "http://example.com/notice/1",
            "publish_date": "2024-04-01",
            "matched_keywords": ["造价"],
            "area_type": "qingdao",
        },
        {
            "title": "青岛市财政局预算绩效评价项目招标公告",
            "link": "http://example.com/notice/2",
            "publish_date": "2024-04-02",
            "matched_keywords": ["预算"],
            "area_type": "qingdao",
        },
    ]

    logger.info("测试钉钉推送...")
    send_email(test_notices)
