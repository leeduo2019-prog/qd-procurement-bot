"""Browser driver management for Selenium Chrome."""

import time
import logging

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
from webdriver_manager.chrome import ChromeDriverManager

logger = logging.getLogger("qd_crawler")

# Pagination selectors for the target site
NEXT_PAGE_SELECTORS = [
    "a.next",
    "li.next a",
    ".pagination .next a",
    ".pager .next a",
    "a[title*='下一页']",
    ".el-pager li:last-child",
    ".ant-pagination-next a",
]


def setup_driver():
    """Create and return a headless Chrome WebDriver."""
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
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
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(30)
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        })
        logger.debug("Chrome 驱动初始化成功")
        return driver
    except WebDriverException as e:
        logger.error("Chrome 驱动初始化失败: %s", e)
        raise


def close_driver(driver):
    """Safely quit the WebDriver."""
    if driver:
        try:
            driver.quit()
        except Exception as e:
            logger.warning("关闭浏览器驱动时出错: %s", e)


def switch_to_tab(driver, tab_name: str) -> bool:
    """Click a tab element to switch content area."""
    if driver is None:
        return False
    try:
        logger.info("  正在切换到 '%s' 标签页...", tab_name)
        all_elements = driver.find_elements(
            By.XPATH, f"//*[contains(text(), '{tab_name}')]"
        )
        if not all_elements:
            logger.warning("  未找到 '%s' 标签页", tab_name)
            return False

        logger.debug("  找到 %d 个匹配元素，点击第一个", len(all_elements))
        driver.execute_script("arguments[0].click();", all_elements[0])
        logger.info("  已点击 '%s' 标签页，等待内容加载...", tab_name)

        try:
            WebDriverWait(driver, 10).until(
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


def has_next_page(driver) -> bool:
    """Check whether a next-page control is available."""
    if driver is None:
        return False
    try:
        for selector in NEXT_PAGE_SELECTORS:
            try:
                element = driver.find_element(By.CSS_SELECTOR, selector)
                if element:
                    # Check if element is enabled and not disabled
                    if not element.is_enabled():
                        continue
                    classes = element.get_attribute("class") or ""
                    if "disabled" in classes:
                        continue
                    return True
            except NoSuchElementException:
                continue

        page_elements = driver.find_elements(
            By.CSS_SELECTOR, ".pagination li a, .pager li a, .el-pager li.number"
        )
        if page_elements:
            return True
    except WebDriverException:
        pass
    return False


def go_to_next_page(driver) -> bool:
    """Click the next-page control. Returns True on success."""
    if driver is None:
        return False
    try:
        for selector in NEXT_PAGE_SELECTORS:
            try:
                element = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                )
                element.click()
                # Wait for SPA content to re-render using WebDriverWait
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "ul.list_right_n")
                    )
                )
                return True
            except (TimeoutException, ElementClickInterceptedException):
                continue

        page_elements = driver.find_elements(
            By.CSS_SELECTOR, ".el-pager li.number, .ant-pagination-item a"
        )
        if page_elements:
            for i, elem in enumerate(page_elements):
                try:
                    if elem.is_enabled() and i + 1 < len(page_elements):
                        page_elements[i + 1].click()
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located(
                                (By.CSS_SELECTOR, "ul.list_right_n")
                            )
                        )
                        return True
                except StaleElementReferenceException:
                    continue
    except WebDriverException as e:
        logger.warning("点击下一页失败: %s", e)
    return False


def click_procurement_notice_tab(driver) -> bool:
    """Click the '采购公告' menu item in the left sidebar."""
    if driver is None:
        return False
    try:
        logger.info("  正在点击'采购公告'菜单...")
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located(
                (By.XPATH, "//ul[@class='ul-snav']//span[contains(text(), '采购公告')]")
            )
        )

        menu_items = driver.find_elements(
            By.XPATH,
            "//ul[@class='ul-snav']//span[contains(text(), '采购公告')]/ancestor::a",
        )

        if not menu_items:
            logger.warning("  未找到'采购公告'菜单项，尝试备用选择器")
            menu_items = driver.find_elements(
                By.XPATH, "//*[contains(text(), '采购公告') and @class='v1']"
            )

        if not menu_items:
            logger.warning("  仍未找到'采购公告'菜单项")
            return False

        menu_items[0].click()
        logger.info("  已点击'采购公告'菜单，等待内容加载...")

        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "ul.list_right_n")
                )
            )
            logger.info("  ✓ '采购公告'内容加载完成")
        except TimeoutException:
            logger.warning("  等待'采购公告'内容加载超时，但继续执行")

        return True
    except TimeoutException:
        logger.warning("  等待'采购公告'菜单超时")
        return False
    except WebDriverException as e:
        logger.error("  点击'采购公告'菜单失败: %s", e)
        return False
