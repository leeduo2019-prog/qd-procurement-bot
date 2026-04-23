"""
青岛政府采购网爬虫
抓取涉及造价、审计、预算、决算、结算的采购公告

v2.0 - 改进版：
- 精准选择器策略 + 内容验证
- SQLite 去重机制
- 结构化日志
- HTTPS 升级
- 细化异常处理
- 标签页切换支持（青岛市/各市区）
"""

import os
import re
import csv
import json
import time
import logging
import hashlib
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set
from contextlib import contextmanager

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    ElementClickInterceptedException,
    StaleElementReferenceException,
    WebDriverException,
)
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# 日志配置
# ---------------------------------------------------------------------------

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger("qd_crawler")
logger.setLevel(logging.DEBUG)

# 控制台 handler
_console = logging.StreamHandler()
_console.setLevel(logging.DEBUG)
_console.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(_console)

# 文件 handler（按天轮转，保留 30 天）
from logging.handlers import TimedRotatingFileHandler

_file = TimedRotatingFileHandler(
    os.path.join(LOG_DIR, "crawler.log"),
    when="midnight",
    backupCount=30,
    encoding="utf-8",
)
_file.setLevel(logging.DEBUG)
_file.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(_file)


# ---------------------------------------------------------------------------
# SQLite 去重 & 持久化
# ---------------------------------------------------------------------------

class NoticeStore:
    """基于 SQLite 的公告存储，支持去重和历史查询。"""

    def __init__(self, db_path: Optional[str] = None):
        if db_path is None:
            db_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "notices.db",
            )
        self.db_path = db_path
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    @contextmanager
    def _connect(self):
        conn = self._get_conn()
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_db(self):
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS notices (
                    id          TEXT PRIMARY KEY,
                    title       TEXT NOT NULL,
                    link        TEXT,
                    publish_date TEXT,
                    area_type   TEXT DEFAULT 'unknown',
                    matched_keywords TEXT,
                    crawl_time  TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_crawl_time ON notices(crawl_time)
            """)

    @staticmethod
    def _make_id(title: str, publish_date: str) -> str:
        raw = f"{title.strip()}|{publish_date.strip()}"
        return hashlib.md5(raw.encode("utf-8")).hexdigest()

    def exists(self, title: str, publish_date: str) -> bool:
        nid = self._make_id(title, publish_date)
        with self._connect() as conn:
            row = conn.execute("SELECT 1 FROM notices WHERE id = ?", (nid,)).fetchone()
            return row is not None

    def insert(self, notice: Dict) -> bool:
        """插入一条公告，若已存在则返回 False。"""
        nid = self._make_id(notice.get("title", ""), notice.get("publish_date", ""))
        if self.exists(notice.get("title", ""), notice.get("publish_date", "")):
            return False
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO notices (id, title, link, publish_date, area_type, matched_keywords, crawl_time) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    nid,
                    notice.get("title", ""),
                    notice.get("link", ""),
                    notice.get("publish_date", ""),
                    notice.get("area_type", "unknown"),
                    json.dumps(notice.get("matched_keywords", []), ensure_ascii=False),
                    notice.get("crawl_time", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                ),
            )
        return True

    def get_recent_ids(self, days: int = 7) -> Set[str]:
        """返回最近 N 天内已存在的公告 ID 集合。"""
        cutoff = (datetime.now().timestamp() - days * 86400) * 1000
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id FROM notices WHERE crawl_time > ?",
                (datetime.fromtimestamp(cutoff / 1000).strftime("%Y-%m-%d %H:%M:%S"),),
            ).fetchall()
            return {row["id"] for row in rows}


# ---------------------------------------------------------------------------
# 爬虫核心
# ---------------------------------------------------------------------------

class ProcurementCrawler:
    """政府采购爬虫类"""

    # 基础 URL（不带 title 参数，避免重定向问题）
    BASE_URL = "http://zfcg.qingdao.gov.cn/qdsite/#/site-list-varied?colCode=04"

    # 精准选择器 - 针对青岛政府采购网 2026 版 DOM 结构
    # 实际DOM: ul.list_right_n > li > span.datelink1_n > a[标题] + span.date_new[日期]
    NOTICE_SELECTORS = [
        # 1. 最精确选择器 - 青岛政府采购网实际结构
        "ul.list_right_n > li",
        "ul.list_right_n li",
        "li > span.datelink1_n",
        "span.datelink1_n",
        # 2. 通用 li，配合 _is_valid_notice 过滤
        "li",
        # 3. 常见公告列表容器
        ".list-box li",
        ".notice-list li",
        ".article-list li",
        ".data-list li",
        ".content-list li",
        # 4. 表格形式
        ".list-table tbody tr",
        "table.dataTable tbody tr",
        # 5. div 卡片形式
        ".list-item",
        ".notice-item",
        ".item-box",
        # 6. 通用但有限制
        ".list ul li",
        ".main-content ul li",
    ]

    # 翻页选择器
    NEXT_PAGE_SELECTORS = [
        "a.next",
        "li.next a",
        "button.next",
        ".pagination .next a",
        ".pager .next a",
        "a[title*='下一页']",
        "a[aria-label='Next']",
        "li[class*='next'] a",
        ".el-pager li:last-child",  # Element UI
        ".ant-pagination-next a",    # Ant Design
    ]

    def __init__(self, area_type: str = "all", days_back: int = 2):
        """
        Args:
            area_type: "all" | "qingdao" | "districts"
            days_back: 只爬取最近 N 天的公告（默认 2 天）
        """
        self.base_url = self.BASE_URL
        self.area_type = area_type
        self.days_back = days_back
        self.keywords = self._load_keywords()
        self.driver = None
        self.results: List[Dict] = []
        self.store = NoticeStore()
        # 计算截止日期
        self.cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    # -- 配置加载 -----------------------------------------------------------

    def _load_keywords(self) -> List[str]:
        keywords_str = os.getenv("KEYWORDS", "造价，审计，预算，决算，结算")
        return [kw.strip() for kw in keywords_str.split(",") if kw.strip()]

    # -- 浏览器驱动 ----------------------------------------------------------

    def _setup_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")  # 新版无头模式
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        )
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)

        try:
            # GitHub Actions / Linux: use system Chrome directly
            chrome_options.binary_location = "/usr/bin/google-chrome"
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(30)
            
            # 执行 CDP 命令来隐藏 webdriver 属性
            self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            })
            logger.debug("Chrome 驱动初始化成功")
        except WebDriverException as e:
            logger.error("Chrome 驱动初始化失败: %s", e)
            raise

    def _close_driver(self):
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                logger.warning("关闭浏览器驱动时出错: %s", e)
            finally:
                self.driver = None

    # -- 标签页切换 ----------------------------------------------------------

    def _switch_to_tab(self, tab_name: str) -> bool:
        if self.driver is None:
            return False
        try:
            logger.info("  正在切换到 '%s' 标签页...", tab_name)
            
            # 查找所有包含标签页文本的元素
            all_elements = self.driver.find_elements(
                By.XPATH, f"//*[contains(text(), '{tab_name}')]"
            )
            
            if not all_elements:
                logger.warning("  未找到 '%s' 标签页", tab_name)
                return False
            
            # 点击第一个匹配的元素（通常是<a>标签）
            logger.debug("  找到 %d 个匹配元素，点击第一个", len(all_elements))
            self.driver.execute_script("arguments[0].click();", all_elements[0])
            logger.info("  已点击 '%s' 标签页，等待内容加载...", tab_name)
            
            # 等待更长时间确保Vue.js重新渲染和内容加载
            time.sleep(5)
            
            # 验证内容是否加载 - 检查是否有列表项
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "ul.list_right_n li")
                    )
                )
                logger.info("  ✓ 已切换到 '%s' 标签页", tab_name)
                return True
            except TimeoutException:
                logger.warning("  切换后未检测到内容更新，但继续执行")
                return True
                
        except TimeoutException:
            logger.warning("  等待 '%s' 标签页超时", tab_name)
            return False
        except WebDriverException as e:
            logger.error("  切换标签页失败: %s", e)
            return False

    # -- 关键词匹配 ----------------------------------------------------------

    def _match_keywords(self, title: str) -> bool:
        return any(keyword in title for keyword in self.keywords)

    def _get_matched_keywords(self, title: str) -> List[str]:
        return [kw for kw in self.keywords if kw in title]

    # -- 页面提取 ------------------------------------------------------------

    def _extract_notices(self, soup: BeautifulSoup) -> List[Dict]:
        """从页面提取公告信息，使用精准选择器 + 内容验证。"""
        notices: List[Dict] = []
        seen_titles: Set[str] = set()

        logger.debug("页面 HTML 长度: %d", len(str(soup)))

        for selector in self.NOTICE_SELECTORS:
            try:
                elements = soup.select(selector)
                if not elements:
                    continue

                candidates = elements[:30]  # 每页最多 30 条
                logger.debug("  选择器 '%s' 匹配到 %d 个元素", selector, len(candidates))

                for container in candidates:
                    try:
                        notice = self._parse_notice(container)
                        if notice and notice["title"] not in seen_titles:
                            logger.debug("  解析到公告: %s...", notice["title"][:30])
                            # 内容验证：标题应包含采购相关关键词
                            if self._is_valid_notice(notice["title"]):
                                notices.append(notice)
                                seen_titles.add(notice["title"])
                                logger.debug("  ✓ 有效公告: %s", notice["title"][:40])
                    except Exception as e:
                        logger.debug("  解析单条公告时出错: %s", e)
                        continue

                if notices:
                    logger.debug("  使用选择器 '%s' 提取到 %d 条有效公告", selector, len(notices))
                    break
            except Exception as e:
                logger.debug("  选择器 '%s' 执行失败: %s", selector, e)
                continue

        if not notices:
            logger.warning("  所有选择器均未提取到公告，尝试 fallback 策略...")
            notices = self._fallback_extract(soup)

        return notices

    @staticmethod
    def _parse_notice(container) -> Optional[Dict]:
        """从单个容器元素解析公告信息。"""
        # 新结构：li > span.datelink1_n > a[标题] + span.date_new[日期]
        title_elem = container.find("a")
        if not title_elem:
            # 如果 container 本身就是 span.datelink1_n
            title_elem = container
            link_elem = container.find("a")
        else:
            link_elem = title_elem

        title = title_elem.get_text(strip=True) if title_elem else ""

        if not title or len(title) < 8:
            return None

        link = link_elem.get("href", "") if link_elem else ""
        if link and isinstance(link, str) and not link.startswith("http"):
            link = f"http://zfcg.qingdao.gov.cn/qdsite/{link}"

        # 提取日期 - 从兄弟元素 span.date_new 中查找
        publish_date = ""
        date_elem = container.find("span", class_="date_new")
        if date_elem:
            publish_date = date_elem.get_text(strip=True)
        
        # 备用：如果没有找到 date_new，尝试从所有文本中提取日期
        if not publish_date:
            date_pattern = re.compile(r"\d{4}-\d{2}-\d{2}")
            all_text = container.get_text()
            date_match = date_pattern.search(all_text)
            if date_match:
                publish_date = date_match.group()

        return {
            "title": title,
            "link": link,
            "publish_date": publish_date,
            "crawl_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    @staticmethod
    def _is_valid_notice(title: str) -> bool:
        """验证标题是否为有效公告（过滤导航、菜单等无关元素）。"""
        # 排除导航/菜单类关键词
        exclude_patterns = [
            r"^首页", r"^关于", r"^联系", r"^帮助", r"^登录", r"^注册",
            r"^网站地图", r"^设为首页", r"^加入收藏",
            r"^(上一页|下一页|首页|末页)",
        ]
        for pattern in exclude_patterns:
            if re.search(pattern, title):
                return False
        # 公告标题通常包含采购相关词汇
        include_patterns = [
            r"采购|招标|中标|公告|项目|合同|预算|审计|造价|决算|结算",
            r"竞争性|询价|询价|单一来源|邀请|公开",
        ]
        return any(re.search(p, title) for p in include_patterns)

    def _fallback_extract(self, soup: BeautifulSoup) -> List[Dict]:
        """Fallback：尝试提取所有包含日期模式的链接。"""
        notices: List[Dict] = []
        date_pattern = re.compile(r"\d{4}[-/.]\d{1,2}[-/.]\d{1,2}")

        for a_tag in soup.find_all("a", href=True):
            title = a_tag.get_text(strip=True)
            if len(title) < 10 or not self._is_valid_notice(title):
                continue

            # 在同级或父级中查找日期
            parent = a_tag.parent
            date_text = ""
            if parent:
                date_match = date_pattern.search(parent.get_text())
                if date_match:
                    date_text = date_match.group()

            link = a_tag["href"]
            if isinstance(link, str) and link and not link.startswith("http"):
                link = f"https://zfcg.qingdao.gov.cn{link}"

            notices.append({
                "title": title,
                "link": link,
                "publish_date": date_text,
                "crawl_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })

        logger.debug("  Fallback 策略提取到 %d 条公告", len(notices))
        return notices[:20]

    # -- 翻页逻辑 ------------------------------------------------------------

    def _has_next_page(self) -> bool:
        if self.driver is None:
            return False
        try:
            for selector in self.NEXT_PAGE_SELECTORS:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if element and element.is_enabled():
                        return True
                except NoSuchElementException:
                    continue

            # 检查页码数字
            page_elements = self.driver.find_elements(
                By.CSS_SELECTOR, ".pagination li a, .pager li a, .el-pager li.number"
            )
            if page_elements:
                return True
        except WebDriverException:
            pass
        return False

    def _go_to_next_page(self):
        if self.driver is None:
            return
        try:
            for selector in self.NEXT_PAGE_SELECTORS:
                try:
                    element = WebDriverWait(self.driver, 5).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                    element.click()
                    time.sleep(2)
                    return
                except (TimeoutException, ElementClickInterceptedException):
                    continue

            # 尝试点击下一个页码数字
            page_elements = self.driver.find_elements(
                By.CSS_SELECTOR, ".el-pager li.number, .ant-pagination-item a"
            )
            if page_elements:
                for i, elem in enumerate(page_elements):
                    try:
                        if elem.is_enabled() and i + 1 < len(page_elements):
                            page_elements[i + 1].click()
                            time.sleep(2)
                            return
                    except StaleElementReferenceException:
                        continue
        except WebDriverException as e:
            logger.warning("点击下一页失败: %s", e)

    def _click_procurement_notice_tab(self) -> bool:
        """点击左侧菜单的'采购公告'选项。"""
        if self.driver is None:
            return False
        try:
            logger.info("  正在点击'采购公告'菜单...")
            # 使用更精确的选择器 - 匹配实际DOM结构
            # <li class="wp_column"><a class="v1"><span class="column-name">采购公告</span></a></li>
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located(
                    (By.XPATH, "//ul[@class='ul-snav']//span[contains(text(), '采购公告')]")
                )
            )
            time.sleep(1)
            
            # 查找并点击"采购公告"菜单项
            menu_items = self.driver.find_elements(
                By.XPATH, "//ul[@class='ul-snav']//span[contains(text(), '采购公告')]/ancestor::a"
            )
            
            if not menu_items:
                logger.warning("  未找到'采购公告'菜单项，尝试备用选择器")
                # 备用：直接查找包含文本的元素
                menu_items = self.driver.find_elements(
                    By.XPATH, "//*[contains(text(), '采购公告') and @class='v1']"
                )
            
            if not menu_items:
                logger.warning("  仍未找到'采购公告'菜单项")
                return False
            
            # 点击菜单项
            menu_items[0].click()
            logger.info("  已点击'采购公告'菜单，等待内容加载...")
            
            # 等待Vue.js重新渲染 - 等待公告列表出现
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "ul.list_right_n")
                    )
                )
                time.sleep(2)  # 额外等待确保渲染完成
                logger.info("  ✓ '采购公告'内容加载完成")
            except TimeoutException:
                logger.warning("  等待'采购公告'内容加载超时，但继续执行")
                time.sleep(3)
            
            return True
        except TimeoutException:
            logger.warning("  等待'采购公告'菜单超时")
            return False
        except WebDriverException as e:
            logger.error("  点击'采购公告'菜单失败: %s", e)
            return False

    # -- 主爬取流程 ----------------------------------------------------------

    def crawl(self, max_pages: int = 5) -> List[Dict]:
        logger.info("=" * 60)
        logger.info("青岛政府采购爬虫 - 自动运行")
        logger.info("=" * 60)
        logger.info("关键词: %s", ", ".join(self.keywords))
        logger.info("最大页数: %d", max_pages)
        logger.info("区域类型: %s", self.area_type)
        logger.info("日期范围: %s 至今（最近 %d 天）", self.cutoff_date, self.days_back)

        self._setup_driver()
        matched_notices: List[Dict] = []
        duplicate_count = 0

        try:
            tabs_to_crawl = []
            if self.area_type == "all":
                tabs_to_crawl = [("qingdao", "青岛市"), ("districts", "各区市")]
            elif self.area_type == "districts":
                tabs_to_crawl = [("districts", "各区市")]
            else:
                tabs_to_crawl = [("qingdao", "青岛市")]

            # 先访问URL并点击采购公告菜单（只需要做一次）
            try:
                assert self.driver is not None, "浏览器驱动未初始化"
                logger.debug("正在访问 URL: %s", self.base_url)
                self.driver.get(self.base_url)
                logger.debug("当前 URL: %s", self.driver.current_url)
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                time.sleep(10)
                
                # 点击"采购公告"菜单
                self._click_procurement_notice_tab()
            except TimeoutException:
                logger.error("页面加载超时")
                return matched_notices
            except WebDriverException as e:
                logger.error("页面加载失败: %s", e)
                return matched_notices

            for tab_area, tab_name in tabs_to_crawl:
                logger.info(">> 开始爬取【%s】标签页", tab_name)
                self.area_type = tab_area

                # 切换到对应标签页（如果不是第一个标签页）
                if tab_name != "青岛市":
                    if not self._switch_to_tab(tab_name):
                        logger.warning("  无法切换到 '%s' 标签页，跳过", tab_name)
                        continue

                for page in range(1, max_pages + 1):
                    if page > 1:
                        self._go_to_next_page()

                    logger.info("  爬取第 %d 页...", page)

                    # 翻页时需要重新访问URL
                    if page > 1:
                        url = f"{self.base_url}&page={page}"
                        try:
                            assert self.driver is not None, "浏览器驱动未初始化"
                            logger.debug("正在访问 URL: %s", url)
                            self.driver.get(url)
                            logger.debug("当前 URL: %s", self.driver.current_url)
                            WebDriverWait(self.driver, 15).until(
                                EC.presence_of_element_located((By.TAG_NAME, "body"))
                            )
                            time.sleep(10)
                            
                            # 重新点击采购公告和切换标签页
                            self._click_procurement_notice_tab()
                            if tab_name != "青岛市":
                                self._switch_to_tab(tab_name)
                        except TimeoutException:
                            logger.warning("  第 %d 页加载超时，跳过", page)
                            break
                        except WebDriverException as e:
                            logger.error("  第 %d 页加载失败: %s", page, e)
                            break

                    html = self.driver.page_source
                    logger.debug("页面 HTML 长度: %d", len(html))
                    if len(html) < 1000:
                        logger.debug("页面 HTML 内容: %s", html)
                    
                    # 保存调试HTML（仅第一页）
                    if page == 1 and tab_area == "qingdao":
                        debug_html_path = os.path.join(
                            os.path.dirname(os.path.abspath(__file__)),
                            "debug_page.html"
                        )
                        try:
                            with open(debug_html_path, "w", encoding="utf-8") as f:
                                f.write(html)
                            logger.debug("调试HTML已保存到: %s", debug_html_path)
                        except Exception as e:
                            logger.debug("保存调试HTML失败: %s", e)
                    
                    soup = BeautifulSoup(html, "html.parser")
                    notices = self._extract_notices(soup)

                    if not notices:
                        logger.info("  第 %d 页未找到公告，可能已到达最后一页", page)
                        break

                    logger.info("  第 %d 页找到 %d 条公告", page, len(notices))

                    for notice in notices:
                        # 日期过滤：只保留近 N 天的公告
                        notice_date = notice.get("publish_date", "")
                        if notice_date and notice_date < self.cutoff_date:
                            logger.debug("  跳过过期公告: %s [%s]", notice["title"][:30], notice_date)
                            continue
                        
                        if self._match_keywords(notice.get("title", "")):
                            # 去重检查
                            if self.store.exists(
                                notice.get("title", ""), notice.get("publish_date", "")
                            ):
                                duplicate_count += 1
                                continue

                            matched = notice.copy()
                            matched["matched_keywords"] = self._get_matched_keywords(
                                notice["title"]
                            )
                            matched["area_type"] = self.area_type
                            matched_notices.append(matched)
                            self.store.insert(matched)
                            logger.info(
                                "  ✓ 匹配: %s... [%s]",
                                notice["title"][:50],
                                ", ".join(matched["matched_keywords"]),
                            )

                    if not self._has_next_page():
                        logger.info("  没有更多页面了")
                        break

                logger.info(">> 【%s】标签页爬取完成", tab_name)

        except KeyboardInterrupt:
            logger.info("用户中断爬取")
        except Exception as e:
            logger.error("爬取过程中出错: %s", e, exc_info=True)
        finally:
            self._close_driver()

        self.results = matched_notices
        logger.info("=" * 60)
        logger.info("爬取完成！共找到 %d 条新匹配公告（跳过 %d 条重复）",
                     len(matched_notices), duplicate_count)
        logger.info("=" * 60)
        return matched_notices

    # -- 数据保存 ------------------------------------------------------------

    def save_to_file(self, filename: str = "") -> str:
        if not filename:
            filename = f"procurement_notices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)

        logger.info("结果已保存到: %s", filename)
        return filename

    def save_to_csv(self, filename: str = "") -> str:
        if not filename:
            filename = f"procurement_notices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        if not self.results:
            logger.info("没有数据可保存")
            return filename

        fieldnames = [
            "title", "link", "publish_date", "area_type",
            "matched_keywords", "crawl_time",
        ]
        with open(filename, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.results)

        logger.info("结果已保存到 CSV: %s", filename)
        return filename
