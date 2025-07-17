import time
import json
import html
import re
import logging
import pandas as pd
from dataclasses import dataclass
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class Product:
    product_id: str
    product_name: str
    brand_name: str
    category_id: str
    category_name: str
    price_origin: str
    price_discount: str
    quantity_sold: str
    rating_average: str
    review_count: str
    crawl_date: str
    in_stock: bool

def initialize_driver(url=None):
    try:
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-blink-features=AutomationControlled")

        driver = webdriver.Remote(
            command_executor='http://selenium:4444/wd/hub',
            options=options,
        )
        if url:
            driver.get(url)
        return driver
    except Exception as e:
        logger.error(f"Failed to initialize WebDriver. Error: {str(e)}")
        raise  # Cho phép fail nhanh

def get_category_links(url):
    """Lấy danh sách URL và tên danh mục"""
    driver = initialize_driver(url)
    category_urls = []
    category_names = []
    for tag in driver.find_elements(By.CSS_SELECTOR, 'div.sc-602cd749-1.hagwli'):
        try:
            temp_tag = tag.find_element(By.TAG_NAME, 'a')
            final_link_category = temp_tag.get_attribute('href')
            category_name = tag.find_element(By.CLASS_NAME, 'sc-602cd749-3').text
            logger.info(f"Found category: {category_name} - {final_link_category}")
                
            category_names.append(category_name)
            category_urls.append(final_link_category)
        except Exception as e:
            logger.warning(f"Error extracting category link: {str(e)}")
            continue
    return category_urls[:10], category_names[:10]

def extract_category(get_category_urls, category_names):
    """Trích xuất category_id từ URL và tạo dict với category_name"""
    try:
        format_category_url = [re.sub('https://tiki.vn/', '', i) for i in get_category_urls]
        res = [i.split('/') for i in format_category_url]
        # print(res)
        dict_res = dict(res)
        final_res = dict(zip(category_names, dict_res.values()))
        return final_res
    except Exception as e:
        logger.error(f"Error extracting categories: {str(e)}")

def get_product_id(product_url):
    """Lấy product_id từ data-view-content"""
    try:
        data_view_content = product_url.get_attribute('data-view-content')
        # Giải mã HTML entity (&quot; → ")
        clean_json = html.unescape(data_view_content)
        data = json.loads(clean_json)
        product_id = data['click_data']['id']
        return product_id
    except Exception as e:
        logger.warning(f"Error getting product ID: {str(e)}")
        return "N/A"

def get_text_element(driver, by, selector, default="N/A"):
    """Lấy text từ phần tử, trả về default nếu lỗi"""
    try:
        return driver.find_element(by, selector).text
    except:
        return default
 
def get_information_products(get_category_urls, get_category_names):
    products = []
    categories = extract_category(get_category_urls, get_category_names)
    for url_category, cat_id in zip(get_category_urls[:8], categories.values()):
        try:
            driver = initialize_driver(url_category)
            WebDriverWait(driver, 5).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'a.sc-68e86366-2.bPchof.product-item'))
            )            
            get_url_products = driver.find_elements(By.CSS_SELECTOR, 'a.sc-68e86366-2.bPchof.product-item')            
            for idx in range(min(30, len(get_url_products))):  
                try:
                    product_link = get_url_products[idx]
                    product_id = get_product_id(product_link)
                    final_data_link = product_link.get_attribute('href')

                    logger.info(f"Access to: {final_data_link}")
                    driver.get(final_data_link)

                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CLASS_NAME, 'sc-c0f8c612-0'))
                    )

                    product_name = get_text_element(driver, By.CLASS_NAME, 'sc-c0f8c612-0')
                    brand_name = get_text_element(driver, By.CLASS_NAME, 'brand-and-author')
                    price_origin = get_text_element(driver, By.CLASS_NAME, 'product-price__original-price')
                    price_discount = get_text_element(driver, By.CLASS_NAME, 'product-price__current-price')
                    quantity_sold = get_text_element(driver, By.CLASS_NAME, 'sc-1a46a934-3')
                    category_id = cat_id if cat_id else "N/A"
                    rating_average = get_text_element(driver, By.CLASS_NAME, 'sc-1a46a934-1')
                    review_count = get_text_element(driver, By.CLASS_NAME, 'number')
                    crawl_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    in_stock = True if product_id else False

                    product = Product(product_id, product_name, brand_name, category_id, "", price_origin, price_discount, quantity_sold, rating_average, review_count, crawl_date, in_stock)
                    products.append(product)

                    driver.back()  # Quay lại danh sách
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'a.sc-68e86366-2.bPchof.product-item'))
                    )
                    get_url_products = driver.find_elements(By.CSS_SELECTOR, 'a.sc-68e86366-2.bPchof.product-item')  # refresh list

                except Exception as e:
                    logger.warning(f"Error processing product {final_data_link}: {str(e)}")
                    continue
        except Exception as e:
            logger.error(f"Error processing category {url_category}: {str(e)}")
            continue
        finally:
            if driver:
                driver.quit()
    
    logger.info(f"Crawling completed {len(products)}")
    return products

if __name__ == '__main__':
    url = r'https://tiki.vn/'
    try:
        get_category_urls, get_category_names = get_category_links(url)
        products = get_information_products(get_category_urls, get_category_names)
        
        df = pd.DataFrame(products)
        a = extract_category(get_category_urls, get_category_names)
        df['category_name'] = df['category_id'].apply(lambda x: next((k for k, v in a.items() if v == x), "Undefined"))
        
        #Storage raw data to folder data
        df.to_csv('/var/tmp/data/raw_data_tiki.csv', index=False)
        
    except Exception as e:
        logger.error(f"Main execution failed: {str(e)}")