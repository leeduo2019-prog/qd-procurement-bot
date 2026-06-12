"""
分析青岛政府采购网的标签页结构
目标：找出"青岛市"和"各市区"标签页的 URL 差异或选择器
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import json

def analyze_tabs():
    # 配置 Chrome
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    
    driver = None
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        
        # 访问基础 URL
        base_url = "http://zfcg.qingdao.gov.cn/qdsite/#/site-list-varied?colCode=04"
        print(f"访问：{base_url}")
        driver.get(base_url)
        time.sleep(5)
        
        # 获取页面 HTML
        html = driver.page_source
        
        # 保存 HTML 用于分析
        with open('tabs_analysis.html', 'w', encoding='utf-8') as f:
            f.write(html)
        print("✓ HTML 已保存到 tabs_analysis.html")
        
        # 尝试查找标签页元素
        print("\n=== 查找标签页元素 ===")
        
        # 可能的标签页选择器
        tab_selectors = [
            "li",
            "button",
            "a",
            "[class*='tab']",
            "[class*='menu']",
            "[role='tab']",
            ".el-tabs__item",  # Element UI 标签
            ".ant-tabs-tab",   # Ant Design 标签
        ]
        
        for selector in tab_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    texts = [elem.text.strip() for elem in elements[:10] if elem.text.strip()]
                    if texts:
                        print(f"\n选择器 '{selector}':")
                        for i, text in enumerate(texts, 1):
                            if '青岛' in text or '市区' in text or '市南' in text or '市北' in text:
                                print(f"  [{i}] {text} ← 可能相关")
                            else:
                                print(f"  [{i}] {text}")
            except Exception as e:
                pass
        
        # 查找包含"青岛"或"市区"的文本
        print("\n=== 查找包含关键词的元素 ===")
        all_elements = driver.find_elements(By.XPATH, "//*[contains(text(), '青岛') or contains(text(), '市区')]")
        for elem in all_elements[:20]:
            text = elem.text.strip()
            if text:
                print(f"  - {text[:100]}")
        
        # 尝试点击"各市区"标签（如果存在）
        print("\n=== 尝试查找并点击'各市区'标签 ===")
        
        # 可能的"各市区"按钮文本
        tab_texts = ['各市区', '市区', '区市', '各区市', '县级']
        
        for tab_text in tab_texts:
            try:
                # XPath 查找包含特定文本的元素
                xpath = f"//*[contains(text(), '{tab_text}')]"
                elements = driver.find_elements(By.XPATH, xpath)
                if elements:
                    print(f"找到包含'{tab_text}'的元素，数量：{len(elements)}")
                    for i, elem in enumerate(elements[:3]):
                        print(f"  [{i}] 标签名：{elem.tag_name}, 类名：{elem.get_attribute('class')}, 文本：{elem.text[:50]}")
            except Exception as e:
                print(f"查找'{tab_text}'失败：{e}")
        
        # 获取当前 URL
        print(f"\n当前 URL: {driver.current_url}")
        
        # 获取所有可能的筛选条件
        print("\n=== 查找所有可能的筛选下拉框 ===")
        selects = driver.find_elements(By.TAG_NAME, 'select')
        print(f"找到 {len(selects)} 个 select 元素")
        
        # 查找可能的按钮
        buttons = driver.find_elements(By.TAG_NAME, 'button')
        print(f"找到 {len(buttons)} 个 button 元素")
        
        # 查找带点击事件的元素
        clickable = driver.find_elements(By.CSS_SELECTOR, "[onclick], [click], [role='button']")
        if clickable:
            print(f"找到 {len(clickable)} 个可点击元素")
        
        print("\n✓ 分析完成")
        
    except Exception as e:
        print(f"错误：{e}")
        import traceback
        traceback.print_exc()
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    analyze_tabs()
