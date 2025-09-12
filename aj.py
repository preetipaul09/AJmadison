import re
import time
import json
import os
import csv
import logging
import requests
import random
import traceback
from random import randint, uniform
import mysql.connector
from datetime import datetime
from bs4 import BeautifulSoup
from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver import FirefoxOptions
from datetime import datetime
from random import randint
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from undetected_chromedriver import ChromeOptions
from selenium.webdriver.common.action_chains import ActionChains
from decimal import Decimal, InvalidOperation
import undetected_chromedriver as uc
from modules.runTimeSecrets import HOST, DB, USER, PASS, HOST2, DB2, USER2, PASS2, HOST3, DB3, USER3, PASS3
from modules.saveRanks import commence as evalRanking

def loggerInit(logFileName):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
    file_handler = logging.FileHandler(f'logs/{logFileName}')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.DEBUG)
    logger.addHandler(stream_handler)
    return logger
logger = loggerInit(logFileName="ajScraper.log")
# ---------------------------------------------------------------

# ----------------------------------------------------------------------

# ---------------------- WEBDRIVER SETUP ----------------------
def triggerSelenium(useVPN=True, checkIP=False):
    logger.debug(f"Selenium triggered")
    
    with open("vpn.config.json") as json_data_file:
        configs = json.load(json_data_file)

    attempts = 0
    while attempts < 3:
        try:
            VPN_IP_PORT = configs['VPN_IP_PORT'][random.randint(0, len(configs['VPN_IP_PORT']) - 1)]

            seleniumwire_options = {
                'proxy': {
                    "http": f"http://{VPN_IP_PORT}",
                    "https": f"http://{VPN_IP_PORT}",
                    'no_proxy': 'localhost,127.0.0.1'
                }
            }

            options = FirefoxOptions()
            # options.add_argument('-headless')
            # options.add_argument('-private')
            if useVPN:
                driver = webdriver.Firefox(options=options,seleniumwire_options=seleniumwire_options)
            else:
                driver = webdriver.Firefox(options=options)
            try:
                if checkIP:
                    time.sleep(2)
                    driver.get("https://api.ipify.org?format=json")
                    time.sleep(2)
                    for request in driver.requests:
                        if request.response:
                            if 'ipify' in request.url:
                                ip_info = json.loads(request.response.body.decode('utf-8'))
                                ip_address = ip_info.get('ip', 'IP not found')
                                logger.debug(f"Current IP: {ip_address}")
                                break
                time.sleep(2)
                try:
                    close_btn = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[aria-label="Close"]'))
                    )
                    close_btn.click()
                except:
                    pass  # No modal appeared
                return driver
            except:
                driver.get_screenshot_as_file(f"logs/IpException({VPN_IP_PORT}).png")
                driver.quit()
                raise Exception("BadSession")
        except Exception as e:
            logger.debug(f"Attempt {attempts+1}/10 failed with error: {e}")
            print(f"Attempt {attempts+1}/10 failed with error: {e}")
            attempts += 1
            if attempts == 3:
                logger.debug(f"triggerSelenium() failed after 10 attempts")
                print(f"triggerSelenium() failed after 10 attempts")
                raise e

def create_stealth_driver():
    """Create a stealth Chrome driver with anti-detection measures"""
    options = uc.ChromeOptions()
    
    # Random user agents
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ]
    
    # Basic anti-detection arguments (compatible with undetected_chromedriver)
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-plugins')
    options.add_argument('--disable-images')
    options.add_argument('--disable-javascript')
    options.add_argument('--disable-web-security')
    options.add_argument('--disable-features=VizDisplayCompositor')
    options.add_argument('--disable-ipc-flooding-protection')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--disable-background-timer-throttling')
    options.add_argument('--disable-backgrounding-occluded-windows')
    options.add_argument('--disable-client-side-phishing-detection')
    options.add_argument('--disable-component-extensions-with-background-pages')
    options.add_argument('--disable-default-apps')
    options.add_argument('--disable-domain-reliability')
    options.add_argument('--disable-features=TranslateUI')
    options.add_argument('--disable-hang-monitor')
    options.add_argument('--disable-prompt-on-repost')
    options.add_argument('--disable-sync')
    options.add_argument('--disable-translate')
    options.add_argument('--metrics-recording-only')
    options.add_argument('--no-first-run')
    options.add_argument('--safebrowsing-disable-auto-update')
    options.add_argument('--enable-automation')
    options.add_argument('--password-store=basic')
    options.add_argument('--use-mock-keychain')
    options.add_argument('--force-device-scale-factor=1')
    options.add_argument('--high-dpi-support=1')
    options.add_argument('--force-color-profile=srgb')
    options.add_argument('--disable-background-networking')
    
    # Set random user agent
    options.add_argument(f'--user-agent={random.choice(user_agents)}')
    
    # Window size
    options.add_argument('--window-size=1920,1080')
    
    # Use compatible experimental options
    try:
        options.add_experimental_option("prefs", {
            "profile.default_content_setting_values.notifications": 2,
            "profile.default_content_settings.popups": 0,
            "profile.managed_default_content_settings.images": 2,
            "profile.default_content_setting_values.media_stream": 2,
        })
    except Exception as e:
        logger.debug(f"Could not set experimental options: {e}")
    
    try:
        # Try with version_main parameter first
        driver = uc.Chrome(options=options, version_main=139)
    except Exception as e:
        logger.debug(f"Failed with version_main=139, trying without: {e}")
        try:
            # Try without version_main parameter
            driver = uc.Chrome(options=options)
        except Exception as e2:
            logger.debug(f"Failed without version_main, trying with basic options: {e2}")
            # Try with minimal options
            basic_options = uc.ChromeOptions()
            basic_options.add_argument('--no-sandbox')
            basic_options.add_argument('--disable-dev-shm-usage')
            basic_options.add_argument('--disable-blink-features=AutomationControlled')
            basic_options.add_argument(f'--user-agent={random.choice(user_agents)}')
            driver = uc.Chrome(options=basic_options)
    
    # Execute stealth scripts
    try:
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
        driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']})")
        driver.execute_script("Object.defineProperty(navigator, 'permissions', {get: () => {query: () => Promise.resolve({state: 'granted'})}})")
        
        # Remove webdriver property
        driver.execute_script("delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;")
        driver.execute_script("delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;")
        driver.execute_script("delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;")
    except Exception as e:
        logger.debug(f"Could not execute stealth scripts: {e}")
    
    return driver

def handle_press_and_hold(driver):
    """Handle press and hold verification if it appears"""
    try:
        logger.debug("Checking for press and hold verification...")
        
        # Wait a moment for any verification elements to load
        time.sleep(3)
        
        # Log current page info for debugging
        logger.debug(f"Current URL: {driver.current_url}")
        logger.debug(f"Page title: {driver.title}")
        
        # Check if we're on an access denied or blocked page
        page_source = driver.page_source.lower()
        if "access denied" in page_source or "blocked" in page_source or "security check" in page_source:
            logger.warning("Detected access denied or security check page")
        
        # Comprehensive list of selectors for verification elements
        verification_selectors = [
            # Press and hold specific
            'div[class*="press"]',
            'div[class*="hold"]',
            'button[class*="press"]',
            'button[class*="hold"]',
            'div[class*="press-hold"]',
            'div[class*="hold-button"]',
            'div[class*="verification-button"]',
            'div[class*="verify-button"]',
            
            # General verification elements
            'div[class*="verification"]',
            'div[class*="captcha"]',
            'div[class*="challenge"]',
            'div[class*="security"]',
            'div[class*="bot-check"]',
            'div[class*="human-verification"]',
            'div[class*="human-check"]',
            
            # Button elements
            'button[class*="verification"]',
            'button[class*="captcha"]',
            'button[class*="challenge"]',
            'button[class*="security"]',
            'button[class*="verify"]',
            'button[class*="check"]',
            
            # Interactive elements
            'div[role="button"]',
            'div[tabindex]',
            'div[onclick]',
            'div[class*="clickable"]',
            
            # Common verification patterns
            'div[class*="slider"]',
            'div[class*="puzzle"]',
            'div[class*="checkbox"]',
            'div[class*="recaptcha"]',
            'div[class*="turnstile"]',
            
            # Text-based identifiers (using XPath)
            '//div[contains(text(), "Press")]',
            '//div[contains(text(), "Hold")]',
            '//div[contains(text(), "Verify")]',
            '//div[contains(text(), "Human")]',
            '//div[contains(text(), "Security")]',
            '//div[contains(text(), "Check")]',
            '//button[contains(text(), "Press")]',
            '//button[contains(text(), "Hold")]',
            '//button[contains(text(), "Verify")]',
            
            # Additional patterns
            'div[class*="interactive"]',
            'div[class*="click"]',
            'div[class*="tap"]',
            'div[class*="touch"]'
        ]
        
        # Try to find and interact with verification elements
        for selector in verification_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    logger.debug(f"Found verification element with selector: {selector}")
                    
                    for element in elements:
                        try:
                            # Scroll element into view
                            driver.execute_script("arguments[0].scrollIntoView(true);", element)
                            time.sleep(1)
                            
                            # Try different interaction methods
                            interaction_methods = [
                                # Method 1: Click and hold with ActionChains
                                lambda: ActionChains(driver).click_and_hold(element).pause(3).release().perform(),
                                
                                # Method 2: JavaScript click and hold
                                lambda: driver.execute_script("""
                                    var element = arguments[0];
                                    var mouseDown = new MouseEvent('mousedown', {
                                        bubbles: true,
                                        cancelable: true,
                                        view: window
                                    });
                                    element.dispatchEvent(mouseDown);
                                    setTimeout(function() {
                                        var mouseUp = new MouseEvent('mouseup', {
                                            bubbles: true,
                                            cancelable: true,
                                            view: window
                                        });
                                        element.dispatchEvent(mouseUp);
                                    }, 3000);
                                """, element),
                                
                                # Method 3: Simple click
                                lambda: driver.execute_script("arguments[0].click();", element),
                                
                                # Method 4: Mouse hover then click and hold
                                lambda: ActionChains(driver).move_to_element(element).click_and_hold().pause(3).release().perform(),
                                
                                # Method 5: Touch events (for mobile-like behavior)
                                lambda: driver.execute_script("""
                                    var element = arguments[0];
                                    var touchStart = new TouchEvent('touchstart', {
                                        bubbles: true,
                                        cancelable: true,
                                        view: window
                                    });
                                    element.dispatchEvent(touchStart);
                                    setTimeout(function() {
                                        var touchEnd = new TouchEvent('touchend', {
                                            bubbles: true,
                                            cancelable: true,
                                            view: window
                                        });
                                        element.dispatchEvent(touchEnd);
                                    }, 3000);
                                """, element),
                                
                                # Method 6: Press and hold with longer duration
                                lambda: ActionChains(driver).click_and_hold(element).pause(5).release().perform(),
                                
                                # Method 7: Multiple clicks
                                lambda: driver.execute_script("""
                                    var element = arguments[0];
                                    for(var i = 0; i < 3; i++) {
                                        element.click();
                                        setTimeout(function(){}, 500);
                                    }
                                """, element),
                                
                                # Method 8: Mouse movement simulation
                                lambda: driver.execute_script("""
                                    var element = arguments[0];
                                    var rect = element.getBoundingClientRect();
                                    var centerX = rect.left + rect.width / 2;
                                    var centerY = rect.top + rect.height / 2;
                                    
                                    // Mouse move to element
                                    var moveEvent = new MouseEvent('mousemove', {
                                        bubbles: true,
                                        cancelable: true,
                                        view: window,
                                        clientX: centerX,
                                        clientY: centerY
                                    });
                                    element.dispatchEvent(moveEvent);
                                    
                                    // Mouse down
                                    var mouseDown = new MouseEvent('mousedown', {
                                        bubbles: true,
                                        cancelable: true,
                                        view: window,
                                        clientX: centerX,
                                        clientY: centerY
                                    });
                                    element.dispatchEvent(mouseDown);
                                    
                                    // Hold for 3 seconds
                                    setTimeout(function() {
                                        var mouseUp = new MouseEvent('mouseup', {
                                            bubbles: true,
                                            cancelable: true,
                                            view: window,
                                            clientX: centerX,
                                            clientY: centerY
                                        });
                                        element.dispatchEvent(mouseUp);
                                    }, 3000);
                                """, element)
                            ]
                            
                            for i, method in enumerate(interaction_methods):
                                try:
                                    logger.debug(f"Trying interaction method {i+1}")
                                    method()
                                    time.sleep(2)
                                    
                                    # Check if verification was successful
                                    current_url = driver.current_url
                                    if "ajmadison.com" in current_url:
                                        logger.debug("Verification appears successful")
                                        return True
                                        
                                except Exception as e:
                                    logger.debug(f"Interaction method {i+1} failed: {e}")
                                    continue
                                    
                        except Exception as e:
                            logger.debug(f"Error interacting with element: {e}")
                            continue
                            
            except Exception as e:
                logger.debug(f"Error with selector {selector}: {e}")
                continue
        
        # Check for iframes (common for verification challenges)
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        logger.debug(f"Found {len(iframes)} iframes")
        
        for iframe in iframes:
            try:
                driver.switch_to.frame(iframe)
                logger.debug("Switched to iframe")
                
                # Look for verification elements inside iframe
                iframe_elements = driver.find_elements(By.CSS_SELECTOR, 
                    'div[class*="press"], div[class*="hold"], div[class*="verification"], button[class*="verification"]')
                
                for element in iframe_elements:
                    try:
                        logger.debug("Found verification element in iframe")
                        ActionChains(driver).click_and_hold(element).pause(3).release().perform()
                        time.sleep(2)
                    except Exception as e:
                        logger.debug(f"Error with iframe element: {e}")
                        continue
                        
                driver.switch_to.default_content()
                
            except Exception as e:
                logger.debug(f"Error handling iframe: {e}")
                driver.switch_to.default_content()
                continue
        
        # Wait a bit more for any redirects to complete
        time.sleep(5)
        
        # Check if we're still on the target site
        current_url = driver.current_url
        page_title = driver.title.lower()
        
        logger.debug(f"Final URL after verification: {current_url}")
        logger.debug(f"Final page title: {driver.title}")
        
        if "ajmadison.com" in current_url and "access denied" not in page_title and "blocked" not in page_title:
            logger.debug("Successfully handled verification or no verification found")
            return True
        else:
            logger.warning(f"Still on verification page or redirected away. Current URL: {current_url}")
            return False
                
    except Exception as e:
        logger.error(f"Error in handle_press_and_hold: {e}")
        return False

def human_like_scroll(driver):
    """Perform human-like scrolling"""
    scroll_height = driver.execute_script("return document.body.scrollHeight")
    viewport_height = driver.execute_script("return window.innerHeight")
    
    # Scroll down in small increments
    current_position = 0
    while current_position < scroll_height:
        scroll_amount = random.randint(100, 300)
        current_position += scroll_amount
        driver.execute_script(f"window.scrollTo(0, {current_position});")
        time.sleep(random.uniform(0.5, 1.5))
    
    # Scroll back up partially
    scroll_up_amount = random.randint(200, 500)
    driver.execute_script(f"window.scrollTo(0, {scroll_height - scroll_up_amount});")
    time.sleep(random.uniform(0.5, 1.0))

def add_human_behavior(driver):
    """Add human-like behavior to avoid detection"""
    try:
        # Get viewport dimensions
        viewport_width = driver.execute_script("return window.innerWidth;")
        viewport_height = driver.execute_script("return window.innerHeight;")
        
        # Random mouse movements within viewport bounds
        for _ in range(random.randint(2, 5)):
            x = random.randint(50, max(100, viewport_width - 100))
            y = random.randint(50, max(100, viewport_height - 100))
            try:
                # Use JavaScript to move mouse to absolute position
                driver.execute_script(f"""
                    var event = new MouseEvent('mousemove', {{
                        'view': window,
                        'bubbles': true,
                        'cancelable': true,
                        'clientX': {x},
                        'clientY': {y}
                    }});
                    document.elementFromPoint({x}, {y}).dispatchEvent(event);
                """)
                time.sleep(random.uniform(0.1, 0.3))
            except Exception as e:
                logger.debug(f"Mouse movement failed: {e}")
                break
        
        # Random scrolling
        try:
            human_like_scroll(driver)
        except Exception as e:
            logger.debug(f"Scrolling failed: {e}")
        
        # Random clicks on empty areas within bounds
        for _ in range(random.randint(1, 3)):
            x = random.randint(50, max(100, viewport_width - 100))
            y = random.randint(50, max(100, viewport_height - 100))
            try:
                # Use JavaScript to click at absolute position
                driver.execute_script(f"""
                    var element = document.elementFromPoint({x}, {y});
                    if (element) {{
                        var clickEvent = new MouseEvent('click', {{
                            'view': window,
                            'bubbles': true,
                            'cancelable': true,
                            'clientX': {x},
                            'clientY': {y}
                        }});
                        element.dispatchEvent(clickEvent);
                    }}
                """)
                time.sleep(random.uniform(0.5, 1.0))
            except Exception as e:
                logger.debug(f"Random click failed: {e}")
                break
                
    except Exception as e:
        logger.debug(f"Error in add_human_behavior: {e}")


# def get_product_urls_from_category(driver, category_url, brand, max_pages=3, output_file=None):
#     """Extract product URLs, brands, images, and prices from a category page with pagination.
#        Saves to CSV after each page and also returns all collected data as a list.
#     """
#     if output_file is None:
#         output_file = f"{brand}.csv"

#     print(f"Saving output to: {output_file}")

#     fieldnames = ['product_url', 'product_name', 'brand_name', 'mpn', 'image', 'msrp', 'base_price']

#     # Create CSV with header if it doesn't exist or is empty
#     if not os.path.exists(output_file) or os.path.getsize(output_file) == 0:
#         with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
#             writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#             writer.writeheader()

#     all_data = []
#     page_number = 0

#     try:
#         while page_number < max_pages:
#             offset = 1 + page_number
#             paged_url = f"{category_url}&page={offset}"
#             print(f"Scraping page {offset}: {paged_url}")

#             driver.get(paged_url)
#             time.sleep(20)
#             # Wait until products are loaded
#             try:
#                 WebDriverWait(driver, 10).until(
#                     EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.grid-list-debug article'))
#                 )
#             except:
#                 print("No products loaded — stopping.")
#                 break

#             # Scroll slowly to trigger lazy loading
#             scroll_height = driver.execute_script("return document.body.scrollHeight")
#             for i in range(0, scroll_height, random.randint(500, 700)):
#                 driver.execute_script(f"window.scrollTo(0, {i});")
#                 time.sleep(random.uniform(0.5, 1))

#             time.sleep(1)

#             product_containers = driver.find_elements(By.CSS_SELECTOR, 'div.grid-list-debug article')
#             if not product_containers:
#                 print("No more products found — stopping.")
#                 break

#             page_data = []

#             for container in product_containers:
#                 try:
#                     product_info = {}

#                     # Product URL
#                     try:
#                         link_element = container.find_element(By.CSS_SELECTOR, 'a')
#                         href = link_element.get_attribute('href')
#                         if href and '/product/' in href:
#                             product_info['product_url'] = href.split('?')[0]
#                         else:
#                             continue
#                     except:
#                         continue

#                     product_info['brand_name'] = brand

#                     # MPN
#                     try:
#                         mpn_element = container.find_element(By.CSS_SELECTOR, 'div.GridItemHit_sku__NUeuZ')
#                         product_info['mpn'] = mpn_element.text.strip()
#                     except:
#                         product_info['mpn'] = 'Unknown'

#                     # Image
#                     try:
#                         img_element = container.find_element(By.CSS_SELECTOR, 'a img')
#                         img_src = img_element.get_attribute('src') or img_element.get_attribute('data-src')
#                         product_info['image'] = img_src if img_src else None
#                     except:
#                         product_info['image'] = None

#                     # MSRP
#                     try:
#                         msrp_element = container.find_element(By.CSS_SELECTOR, 'div.GridItemHit_price-list__Vyv2I')
#                         product_info['msrp'] = msrp_element.text.strip()
#                     except:
#                         product_info['msrp'] = None

#                     # Base Price
#                     try:
#                         price_element = container.find_element(By.CSS_SELECTOR, 'div.GridItemHit_price-after-savings__GQlYb')
#                         product_info['base_price'] = price_element.text.strip()
#                     except:
#                         product_info['base_price'] = None

#                     # Product Name
#                     try:
#                         name_element = container.find_element(By.CSS_SELECTOR, 'a.GridItemHit_description-truncated__ijB77')
#                         product_info['product_name'] = name_element.text.strip()
#                     except:
#                         product_info['product_name'] = None

#                     page_data.append(product_info)

#                 except Exception as e:
#                     print(f"Error extracting product info: {e}")
#                     continue

#             # Save this page's data
#             if page_data:
#                 with open(output_file, 'a', newline='', encoding='utf-8') as csvfile:
#                     writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#                     writer.writerows(page_data)

#                 print(f"Saved {len(page_data)} products from page {offset} to {output_file}")

#                 all_data.extend(page_data)

#             page_number += 1

#         print("Scraping complete.")

#     except Exception as e:
#         print(f"Error getting product details: {e}")

#     return all_data

    
def clean_value(value):
    """Convert 'N/A', 'null', or empty strings to None, else return stripped value."""
    if value is None:
        return None
    value = str(value).strip()
    if value.lower() in ['n/a', 'na', 'null', '--', '']:
        return None
    return value


def read_csv_to_list(vendor_id,brand,csv_filename=None):
    """Read CSV data and convert to list with specified structure"""
    if csv_filename is None:
        csv_filename = f"{brand}.csv"
        
    product_list = []
    
    try:
        with open(csv_filename, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            row_count = 0
            
            for row in reader:
                row_count += 1
                temp = {}
                temp2 = {}
                temp['brand_name'] = clean_value(row.get('brand', None))
                temp['product_mpn'] = clean_value(row.get('mpn', None))
                temp['vendor_sku'] = clean_value(row.get('mpn', None))
                temp['product_name'] = clean_value(row.get('name', None))
                temp['product_url'] = clean_value(row.get('url', None))
                temp['product_image'] = clean_value(row.get('image_url', None))
                temp['discount'] = '0.00'
                temp['vendorprice_stock_text'] = None
                temp['vendorprice_stock'] = None
                msrp_raw = clean_value(row.get('msrp', None))
                price_raw = clean_value(row.get('price', None))
                
                if msrp_raw is not None:
                    try:
                        msrp_clean = str(msrp_raw).strip().replace('$', '').replace(',', '')
                        temp['msrp'] = msrp_clean
                    except (InvalidOperation, ValueError) as e:
                        print(f"Failed to convert msrp '{msrp_raw}' → Error: {e}")
                        temp['msrp'] = None
                else:
                    temp['msrp'] = None
                
                if price_raw is not None:
                    try:
                        price_clean = str(price_raw).strip().replace('$', '').replace(',', '')
                        temp['vendorprice_price'] = price_clean
                    except (InvalidOperation, ValueError) as e:
                        print(f"Failed to convert vendorprice_price '{price_raw}' → Error: {e}")
                        temp['vendorprice_price'] = None
                else:
                    temp['vendorprice_price'] = None
                
                temp2['url'] = temp['product_url']
                temp['vendorprice_finalprice'] = temp['vendorprice_price']
                temp2['vendorprice_price'] = temp['vendorprice_price']
                temp2['vendorprice_finalprice'] = temp['vendorprice_price']
                temp2['discount'] = '0.00'
                temp2['vendorprice_stock'] = temp['vendorprice_stock']
                temp2['vendorprice_stock_text'] = temp['vendorprice_stock_text']
                # Add additional fields
                temp2['scraped_by_system'] = "Dheeraj Pc"
                temp2['source'] = "direct_from_website"
                temp2['product_condition'] = 'New'
                
                # print("--------------------------------------------------------")
                # print(temp)
                # print(temp2)
                # exit()
                logger.info(f"Saving row #{row_count} from {csv_filename}")
                # print("--------------------------------------------------------")
                product_id, vendor_product_id = insertIntoMsp(temp, vendor_id)
                print("--------------------------------------------------------")
                print(product_id, vendor_product_id)
                print("--------------------------------------------------------")
                insertall(product_id, vendor_product_id, temp2, vendor_id)
                
            logger.info(f" Finished processing {row_count} rows from: {csv_filename}")
            # return temp, temp2
            
                # product_list.append(temp)
                
        logger.info(f"Successfully loaded {len(product_list)} products from CSV")
        return product_list
        
    except FileNotFoundError:
        logger.error(f"CSV file {csv_filename} not found")
        return []
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        return []

def insertIntoMsp(row, vendor_id):
    product_id = vendor_product_id = None  # Initialize to None
    try:
        brand_id = checkInsertBrand(vendor_id, row['brand_name'])
        product_id = checkInsertProduct(vendor_id, brand_id, row['product_mpn'], row['product_name'], row['msrp'], row['product_image'])
        vendor_product_id = checkInsertProductVendor(vendor_id, product_id, row['vendor_sku'], row['product_name'], row['product_url'], row['msrp'])
        checkInsertProductVendorURL(vendor_id, vendor_product_id, row['product_url'])
    except Exception as e:
        logger.error(f"Error in insertIntoMsp: {e}")
    return product_id, vendor_product_id


def getBrandRawName(brand_name):
    letters, numbers, spaces = [], [], []
    for character in brand_name:
        if character.isalpha():
            letters.append(character)
        elif character.isnumeric():
            numbers.append(character)
        elif character.isspace():
            spaces.append(character)
    if len(letters) > 0: raw_name = "".join(spaces + letters)
    else: raw_name = "".join(spaces + numbers)
    return raw_name


# Add brand if doesn't exists
def checkInsertBrand(vendor_id,brand_name):
    try:
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor(buffered=True)
            this.execute("SELECT brand_id FROM BrandSynonyms WHERE brand_synonym = %s", (brand_name,))
            brand_id = this.fetchone()
            if brand_id:
                logger.info(f"{vendor_id} >> Found brand synonym: {brand_name} ({brand_id[0]})")
                return brand_id[0]
            else:
                brandRawNname = getBrandRawName(brand_name)
                brandRaw = brandRawNname.lower().strip()
                this.execute("SELECT brand_id, brand_name FROM Brand WHERE brand_raw_name = %s",(brandRaw,))
                records = this.fetchone()
                if records:
                    fetchedBrandId = records[0]
                    fetchedBrandName = records[1]
                    if fetchedBrandName != brand_name:
                        insertBrandSynonymsQuery = "INSERT INTO BrandSynonyms (brand_id,brand_synonym) VALUES (%s,%s);"
                        this.execute(insertBrandSynonymsQuery,(fetchedBrandId,brand_name))
                        conn.commit()
                        logger.info(f"Inserted {brandRawNname} as a synonym for {fetchedBrandName}.")
                    else:
                        logger.info(f"{brandRaw} Brand Name Matched")
                        return fetchedBrandId
                else:
                    insertBrandQuery = "INSERT INTO Brand (brand_name,brand_key,brand_raw_name) VALUES (%s,%s,%s);"
                    this.execute(insertBrandQuery,(brand_name,brand_name.replace(" ", "-").lower(),brandRaw))
                    conn.commit()
                    logger.info(f'{vendor_id} >> Added new brand "{brand_name} ({this.lastrowid})".')
                    return this.lastrowid
    except mysql.connector.Error as e:
        logger.warning(f"{vendor_id} >> MySQL ERROR checkInsertBrand() >> {e}")
        logger.warning(f"{vendor_id}, {brand_name}")
    finally:
        if conn.is_connected():
            conn.close()
            this.close()

# Add product if doesn't exists
def checkInsertProduct(vendor_id, brand_id, mpn, name, msrp, image):
    try:
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor(buffered=True)
            checkProductQuery = "SELECT product_id FROM Product WHERE brand_id = %s AND product_mpn = %s"
            this.execute(checkProductQuery, [brand_id,mpn])
            records = this.fetchone()
            # Change this section
            if records is None:  # If no record found
                # Insert new product
                if msrp != '':
                    insertProductQuery = "INSERT INTO Product (brand_id,product_name,product_mpn,msrp,product_image) VALUES (%s,%s,%s,%s,%s)"
                    this.execute(insertProductQuery, (brand_id,name,mpn,msrp,image))
                else:
                    insertProductQuery = "INSERT INTO Product (brand_id,product_name,product_mpn,product_image) VALUES (%s,%s,%s,%s)"
                    this.execute(insertProductQuery, (brand_id,name,mpn,image))
                conn.commit()
                logger.info(f'{vendor_id} >> Added new product with mpn "{mpn} ({this.lastrowid})".')
                return this.lastrowid
            else:
                product_id = int(records[0])
                this.execute("UPDATE Product SET product_image = %s WHERE product_id = %s", [image,product_id])
                conn.commit()
                if msrp != '':
                    this.execute("UPDATE Product SET msrp = %s WHERE product_id = %s AND msrp IS NULL", [msrp,product_id])
                    conn.commit()
                logger.info(f'{vendor_id} >> Updated details for product with mpn "{mpn} ({product_id})".')
                return product_id
    except mysql.connector.Error as e:
        logger.warning(f"{vendor_id} >> MySQL ERROR checkInsertProduct() >> {e}")
        logger.warning(f"{vendor_id}, {brand_id}, {mpn}, {name}, {msrp}, {image}")
        return None
    finally:
        if conn.is_connected():
            conn.close()
            this.close()

# Add product vendor if doesn't exists
def checkInsertProductVendor(vendor_id, product_id, sku, name, product_url, msrp):
    try:
        # First check if we have valid input
        if product_id is None:
            logger.warning(f"{vendor_id} >> Cannot insert vendor product: product_id is None")
            return None
            
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor(buffered=True)
            if msrp == '' or msrp is None:
                msrp = None  # or set to 0.0 if you prefer a default value

            checkProductVendorQuery = "SELECT vendor_product_id FROM ProductVendor WHERE vendor_id = %s AND product_id = %s LIMIT 1"
            this.execute(checkProductVendorQuery, [vendor_id, product_id])
            records = this.fetchone()
            
            # Handle case where no records found
            if records is None:
                # Insert new record
                insertProductVendorQuery = "INSERT INTO ProductVendor (vendor_id, product_id, product_name, vendor_sku, msrp) VALUES (%s, %s, %s, %s, %s)"
                this.execute(insertProductVendorQuery, (vendor_id, product_id, name, sku, msrp))
                conn.commit()
                logger.info(f'{vendor_id} >> Added new product in ProductVendor "{vendor_id} x {product_id}".')
                return this.lastrowid
            else:
                # Update existing record
                vp_id = int(records[0])
                updateProductDetailQuery = "UPDATE ProductVendor SET vendor_sku = %s, msrp = %s WHERE vendor_product_id = %s"
                this.execute(updateProductDetailQuery, [sku, msrp, vp_id])
                conn.commit()
                if this.rowcount == 1:
                    logger.info(f'{vendor_id} >> Updated details for vendor_product_id ({vp_id}).')
                logger.info(f'{vendor_id} >> Returned vendor_product_id ({vp_id}).')
                return vp_id
    except mysql.connector.Error as e:
        logger.error(f"{vendor_id} >> MySQL ERROR checkInsertProductVendor() >> {e}")
        return None
    finally:
        if conn.is_connected():
            conn.close()
            this.close()

# Add product vendor url if doesn't exists
def checkInsertProductVendorURL(vendor_id, vendor_product_id, product_url):
    url = product_url.split('&')[0]
    try:
        if not vendor_product_id:
            logger.warning(f"{vendor_id} >> Invalid vendor_product_id: {vendor_product_id}")
            return  # Exit the function early
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor(buffered=True)
            checkProductVendorURLQuery = "SELECT vendor_product_id FROM VendorURL WHERE vendor_product_id = %s"
            this.execute(checkProductVendorURLQuery, [vendor_product_id,])
            records = this.fetchall()
            if len(records) == 0:
                insertProductVendorURLQuery = "INSERT INTO VendorURL (vendor_product_id, vendor_raw_url, vendor_url) VALUES (%s, %s, %s)"
                this.execute(insertProductVendorURLQuery, [vendor_product_id, product_url, url])
                conn.commit()
                logger.info(f'{vendor_id} >> Added product vendor URL for vendor_product_id "{vendor_product_id}".')
                return this.lastrowid
            else:
                # fatchquary = "SELECT vendor_url_id, vendor_raw_url, vendor_url FROM VendorURL WHERE vendor_product_id = %s"
                # this.execute(fatchquary, [vendor_product_id])
                # results = this.fetchall()
                # if results[0][2] != url:
                # Update the existing record
                updateProductVendorURLQuery = """UPDATE VendorURL SET vendor_raw_url = %s, vendor_url = %s WHERE vendor_product_id = %s"""
                this.execute(updateProductVendorURLQuery, [product_url, url, vendor_product_id])
                conn.commit()
                logger.info(f'{vendor_id} >> Updated product vendor URL for vendor_product_id "{vendor_product_id}".')
                # else:
                #     logger.info(f'{vendor_id} >> Same Product vendor URL already exists for vendor_product_id "{vendor_product_id}".')
                # try:
                #     vendor_url_id, vendor_raw_url, vendor_url = results[0][0], results[0][1], results[0][2]
                #     checkProductVendorURLQuery = "SELECT vendor_bakup_url_id FROM BuilddotcomeDirectScraping_VendorURLBackup WHERE vendor_product_id = %s"
                #     this.execute(checkProductVendorURLQuery, [vendor_product_id,])
                #     Record = this.fetchone()
                #     if Record is None or len(Record) == 0:
                #         insertProductVendorURLQuery = "INSERT INTO BuilddotcomeDirectScraping_VendorURLBackup (vendor_url_id, vendor_product_id, vendor_raw_url, vendor_url) VALUES (%s, %s, %s, %s)"
                #         this.execute(insertProductVendorURLQuery, [vendor_url_id, vendor_product_id, vendor_raw_url, vendor_url])
                #         conn.commit()
                #         logger.info(f'Added product vendor_url for vendor_product_id "{vendor_product_id}" for vendor_bakup_url_id {this.lastrowid}.')
                #     else:
                #         if Record[0] is not None:
                #             fatchquary = "SELECT vendor_url_id, vendor_raw_url, vendor_url FROM BuilddotcomeDirectScraping_VendorURLBackup WHERE vendor_bakup_url_id = %s"
                #             this.execute(fatchquary, [Record[0],])
                #             Records = this.fetchone()
                #             if Records and Records[2] != vendor_url:
                #                 # Update the existing record
                #                 updateProductVendorURLQuery = """UPDATE BuilddotcomeDirectScraping_VendorURLBackup SET vendor_raw_url = %s, vendor_url = %s WHERE vendor_bakup_url_id = %s"""
                #                 this.execute(updateProductVendorURLQuery, [vendor_raw_url, vendor_url, Record[0]])
                #                 conn.commit()
                #                 logger.info(f'Updated vendor_raw_url, vendor_url for vendor_bakup_url_id "{Record[0]}".')
                #             else:
                #                 logger.info(f'Same Product vendor URL already exists for vendor_bakup_url_id "{Record[0]}".')
                # except mysql.connector.Error as e:
                #     logger.warning(f"MySQL ERROR checkInsertProductVendorURL() >> {e}")
                # results.append(Records)
    except mysql.connector.Error as e:
        logger.warning(f"{vendor_id} >> MySQL ERROR checkInsertProductVendorURL() >> {e}")
    finally:
        if conn.is_connected():
            conn.close()
            this.close()

# call all function into this function
def insertall(product_id, vendor_product_id, temp, vendor_id):
    try:
        price = temp['vendorprice_price']
        if (price is not None and price.strip() != ''):
            vendorTempPricing(vendor_product_id, temp)
            rpVendorPricingHistory(vendor_product_id, temp, vendor_id)
            # productMsrpUpdate(product_id, temp)
            # productVendorMsrpUpdate(vendor_product_id, temp)
        else:
            logger.info(f"Invalid price value: {price}")
    except Exception as e:
        logger.error(f"Error in insertall(): {e}")

def getDatetime():
    currentDatetime = datetime.now()
    return currentDatetime.strftime("%Y-%m-%d %H:%M:%S")

# Temp vnendor pricing data
def vendorTempPricing(vendor_product_id, temp):
    dateTime = getDatetime()
    try:
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor(buffered=True)
            checkQuery = "SELECT vendor_product_id FROM TempVendorPricing WHERE vendor_product_id = %s AND source = %s LIMIT 1"
            this.execute(checkQuery, (vendor_product_id, temp['source']))
            records = this.fetchone()
            if records:
                getPricequary = "SELECT * FROM TempVendorPricing WHERE vendor_product_id = %s AND source = 'direct_from_website'"
                this.execute(getPricequary, (records[0],))
                result = this.fetchone()
                savedprice = str(result[2]).strip()
                scrapedprice = str(temp['vendorprice_price']).strip()
                if savedprice == scrapedprice:
                    logger.info(f"Same vendor price already exists for vendor_product_id {vendor_product_id}")
                else:
                    updateQuery = """UPDATE TempVendorPricing SET is_price_changed = %s, price_changed_date = %s WHERE vendor_product_id = %s AND source = %s"""
                    values = ("1", dateTime, vendor_product_id, temp['source'])
                    this.execute(updateQuery, values)
                    conn.commit()
                    logger.info(f"is_price_changed set 1 for vendor_product_id ({vendor_product_id}).")
                updateQuery = """UPDATE TempVendorPricing SET vendorprice_price = %s, vendorprice_finalprice = %s, vendorprice_date = %s, vendorprice_stock = %s, vendorprice_stock_text = %s, product_condition = %s, is_rp_calculated = %s, is_member = %s, scraped_by_system = %s
                    WHERE vendor_product_id = %s AND source = %s"""
                values = (temp['vendorprice_price'], temp['vendorprice_finalprice'], dateTime, temp['vendorprice_stock'], temp['vendorprice_stock_text'] ,temp['product_condition'], '2', '0', temp['scraped_by_system'], vendor_product_id, temp['source'])
                this.execute(updateQuery, values)
                conn.commit()
                logger.info(f"Record Updated for vendor_product_id ({vendor_product_id}) and source ({temp['source']})")
            else:
                insertQuery = """INSERT INTO TempVendorPricing (vendor_product_id, vendorprice_price, vendorprice_finalprice, vendorprice_date, vendorprice_stock, vendorprice_stock_text, product_condition, source, is_rp_calculated, is_member, scraped_by_system) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s)"""
                values = (vendor_product_id, temp['vendorprice_price'], temp['vendorprice_finalprice'], dateTime,temp['vendorprice_stock'], temp['vendorprice_stock_text'], temp['product_condition'], temp['source'], '2', '0', temp['scraped_by_system'])
                this.execute(insertQuery, values)
                conn.commit()
                logger.info(f"Record Inserted for vendor_product_id ({vendor_product_id}) and source ({temp['source']})")
    except mysql.connector.Error as e:
        logger.warning(f"MySQL ERROR vendorTempPricing() >> {e}")
    finally:
        if conn.is_connected():
            conn.close()
            this.close() 

def get_table_structure(host, db, user, password, table_name):
    """Retrieve column details from a table, preserving the column order."""
    try:
        conn = mysql.connector.connect(host=host, database=db, user=user, password=password)
        cursor = conn.cursor()            
        cursor.execute(f"DESCRIBE {table_name}")
        structure = [(row[0], row[1], row[2], row[3], row[4], row[5]) for row in cursor.fetchall()]  
        # (Column Name, Column Type, NULL, Key, Default, Extra)
    except Exception as e:
        logger.error(f"Error fetching table structure for {table_name}: {e}")
        structure = []
    finally:
        cursor.close()
        conn.close()
    return structure

def match_table_structure(source_structure, target_structure):
    """Find missing columns with full definitions and their correct positions."""
    target_columns = {col[0]: col for col in target_structure}  # {Column Name: Column Details}
    missing_columns = []

    for index, column in enumerate(source_structure):
        col_name, col_type, is_null, key, default, extra = column
        if col_name not in target_columns:
            after_column = source_structure[index - 1][0] if index > 0 else None
            missing_columns.append((col_name, col_type, is_null, key, default, extra, after_column))
    if missing_columns and len(missing_columns) > 0:
        logger.info(f"Missing columns: {missing_columns}")
    logger.info(f"History Table is up-to-date.")
    return missing_columns

def rpVendorPricingHistory(vendor_product_id, temp, vendor_id):
    dateTime = getDatetime()
    try:
        # save to AF/HP if vendor_id is one of them
        if vendor_id == 10021 or vendor_id == 10024: conn = mysql.connector.connect(host=HOST2, database=DB2, user=USER2, password=PASS2)
        else: conn = mysql.connector.connect(host=HOST3, database=DB3, user=USER3, password=PASS3)
        if conn.is_connected():
            this = conn.cursor(buffered=True)
            # check if vendor specific vendorPricing table exists or not
            vendor_pricing_table = f"z_{vendor_id}_VendorPricing"
            this.execute(f"""SELECT * 
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = '{vendor_pricing_table}'
            LIMIT 1""")
            result = this.fetchone()
            source_structure = get_table_structure(HOST, DB, USER, PASS, 'TempVendorPricing')
            if not result:
                logger.info(f"Table {vendor_pricing_table} does not exist. Creating table...")
                column_definitions = []
                primary_key = None  # Store primary key column if exists
                for col_name, col_type, is_null, key, default, extra in source_structure:
                    null_option = "NULL" if is_null == "YES" else "NOT NULL"
                    # Handle default values properly
                    if default is not None:
                        if "timestamp" in col_type.lower() or "datetime" in col_type.lower():
                            default_option = "DEFAULT CURRENT_TIMESTAMP" if default.lower() == "current_timestamp()" else ""
                        else:
                            default_option = f"DEFAULT {repr(default)}"
                    else:
                        default_option = ""
                    extra_option = extra if extra else ""
                    # Ensure AUTO_INCREMENT is properly handled
                    if "auto_increment" in extra.lower():
                        extra_option = "AUTO_INCREMENT"
                        primary_key = col_name  # Store primary key
                    column_definitions.append(f"`{col_name}` {col_type} {null_option} {default_option} {extra_option}")
                create_table_query = f"""
                    CREATE TABLE `{vendor_pricing_table}` (
                        {', '.join(column_definitions)}
                        {f", PRIMARY KEY (`{primary_key}`)" if primary_key else ""}
                    );
                """.strip()
                this.execute(create_table_query)
                conn.commit()
                logger.info(f"Table {vendor_pricing_table} created successfully.")
                logger.info(f"==========================================")
            else:
                if vendor_id == 10021 or vendor_id == 10024:
                    target_structure = get_table_structure(HOST2, DB2, USER2, PASS2, vendor_pricing_table)
                else:
                    target_structure = get_table_structure(HOST3, DB3, USER3, PASS3, vendor_pricing_table)
                missing_columns = match_table_structure(source_structure, target_structure)
                if missing_columns and len(missing_columns) > 0:
                    # Add missing columns if table exists
                    for col_name, col_type, is_null, key, default, extra, after_column in missing_columns:
                        null_option = "NULL" if is_null == "YES" else "NOT NULL"
                        # Handle default values properly
                        if default is not None:
                            if "timestamp" in col_type.lower() or "datetime" in col_type.lower():
                                default_option = "DEFAULT CURRENT_TIMESTAMP" if default.lower() == "current_timestamp()" else ""
                            else:
                                default_option = f"DEFAULT {repr(default)}"
                        else:
                            default_option = ""
                        extra_option = extra if extra else ""
                        after_option = f"AFTER `{after_column}`" if after_column else "FIRST"
                        # Prevent adding AUTO_INCREMENT column incorrectly
                        if "auto_increment" in extra.lower():
                            logger.warning(f"Skipping column `{col_name}` because it has AUTO_INCREMENT.")
                            continue  # Do not add AUTO_INCREMENT column
                        alter_query = f"""
                            ALTER TABLE `{vendor_pricing_table}`
                            ADD COLUMN `{col_name}` {col_type} {null_option} {default_option} {extra_option} {after_option};
                        """.strip()
                        this.execute(alter_query)
                    conn.commit()
                    logger.info(f"Table {vendor_pricing_table} altered successfully.")
                    logger.info(f"==========================================")

            insertQuery = f"""INSERT INTO {vendor_pricing_table} (vendor_product_id, vendorprice_price, vendorprice_finalprice, vendorprice_date, vendorprice_stock, 
                vendorprice_stock_text, product_condition, source, is_rp_calculated, is_member, scraped_by_system) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            values = (vendor_product_id, temp['vendorprice_price'], temp['vendorprice_finalprice'], dateTime,temp['vendorprice_stock'], temp['vendorprice_stock_text'], temp['product_condition'], temp['source'], '2', '0', temp['scraped_by_system'])
            this.execute(insertQuery, values)
            conn.commit()
            logger.info(f"Record Inserted for vendor_product_id ({vendor_product_id}) and source ({temp['source']}) In {vendor_pricing_table} history table.")
    except mysql.connector.Error as e:
        logger.warning(f"MySQL ERROR {vendor_pricing_table} >> {e}")
    finally:
        if conn.is_connected():
            conn.close()
            this.close()

# Updating MSRF in Product table
def productMsrpUpdate(product_id, temp):
    try:
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor(buffered=True)
            this.execute("SELECT msrp FROM Product WHERE product_id = %s", (product_id,))
            result = this.fetchone()
            if result:
                # Update MSRP
                if temp['msrp']:
                    this.execute("UPDATE Product SET msrp = %s WHERE product_id = %s", (temp['msrp'], product_id))
                    conn.commit()
                    logger.info(f"Record Updated for product_id ({product_id}).")
    except mysql.connector.Error as e:
        logger.warning(f"{product_id} >> MySQL ERROR productMsrpUpdate() >> {e}")
    finally:
        if conn.is_connected():
            conn.close()
            this.close()

# Updating MSRF in ProductVendor table
def productVendorMsrpUpdate(vendor_product_id, temp):
    try:
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor(buffered=True)
            this.execute("SELECT msrp FROM ProductVendor WHERE vendor_product_id = %s", (vendor_product_id,))
            result = this.fetchone()
            if result:
                # Update MSRP
                if temp['msrp']:
                    this.execute("UPDATE ProductVendor SET msrp = %s WHERE vendor_product_id = %s", (temp['msrp'], vendor_product_id))
                    conn.commit()
                    logger.info(f"Record Updated for vendor_product_id ({vendor_product_id}).")
    except mysql.connector.Error as e:
        logger.warning(f"{vendor_product_id} >> MySQL ERROR productVendorMsrpUpdate() >> {e}")
    finally:
        if conn.is_connected():
            conn.close()
            this.close()
            
            
# def scrape_category_products(category_url, vendor_id, brand, max_products=None):
#     """Scrape all products from a category page"""
#     driver = None
#     try:
#         options = ChromeOptions()
#         driver = uc.Chrome(version_main=139, options=options)

#         if not driver:
#             logger.error("Failed to initialize Selenium WebDriver.")
#             return []

#         logger.debug("Attempting to access category page...")

#         # This now returns the list directly
#         product_list = get_product_urls_from_category(driver, category_url, brand)

#         if not product_list:
#             logger.error("No product data found")
#             return []

#         logger.info(f"Successfully scraped {len(product_list)} products")
#         return product_list

#     except Exception as e:
#         logger.error(f"Error in scrape_category_products: {e}", exc_info=True)
#         return []

#     finally:
#         if driver:
#             try:
#                 driver.quit()
#             except Exception:
#                 pass


def scrape_category_products(category_url, output_file=None):
    # if output_file is None:
    #     output_file = f"{brand}.csv"

    # fieldnames = ['product_url', 'product_name', 'brand_name', 'mpn', 'image', 'msrp', 'base_price']

    # Create CSV file with header if missing or empty
    # if not os.path.exists(output_file) or os.path.getsize(output_file) == 0:
    #     with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
    #         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    #         writer.writeheader()

    all_data = []
    driver = None

    try:
        options = ChromeOptions()
        driver = uc.Chrome(version_main=139, options=options)

        # print(f"Saving output to: {output_file}")

        page_number = 1
        while True:
            paged_url = f"{category_url}"
            print(f"Scraping page {page_number}: {paged_url}")

            driver.get(paged_url)
            time.sleep(20)

            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.container.container--fluid div.clearfix'))
                )
            except:
                print("No products loaded — stopping.")
                break

            # Scroll slowly to load all products
            scroll_height = driver.execute_script("return document.body.scrollHeight")
            for i in range(0, scroll_height, random.randint(500, 700)):
                driver.execute_script(f"window.scrollTo(0, {i});")
                time.sleep(random.uniform(0.5, 1))

            time.sleep(1)

            maindiv = driver.find_element(By.CSS_SELECTOR, 'div.container.container--fluid div.clearfix')
            if not maindiv:
                print("Main div not found")
                break

            page_data = []
            try:
                product_info = {}

                try:
                    brand_element = maindiv.find_element(By.CSS_SELECTOR,'div.divided div')
                    brand_name = brand_element.text.strip()
                except:
                    brand_name = None

                product_info['brand'] = brand_name
                # MPN
                mpn_element = maindiv.find_element(By.CSS_SELECTOR, 'div.divided div.inline-block.gray-5.clearfix').text.strip()
                if mpn == mpn_element:
                    product_info['mpn'] = mpn_element
                else :
                    logger.debug("Pls verify MPN")
                    break

                # Image
                try:
                    img_element = maindiv.find_element(By.CSS_SELECTOR, 'div.product-image2-main-image-container img')
                    img_src = img_element.get_attribute('src') or img_element.get_attribute('data-src')
                    product_info['image'] = img_src if img_src else None
                except:
                    product_info['image'] = None

                # MSRP
                try:
                    msrp_element = maindiv.find_element(By.CSS_SELECTOR, 'td[itemprop="priceSpecification"] del')
                    product_info['msrp'] = msrp_element.text.strip()
                except:
                    product_info['msrp'] = None

                # Base Price
                try:
                    price_element = maindiv.find_element(By.CSS_SELECTOR, 'div[itemprop="price"] span')
                    product_info['base_price'] = price_element.text.strip()
                except:
                    product_info['base_price'] = None

                # Product Name
                try:
                    name_element = maindiv.find_element(By.CSS_SELECTOR, 'h1.pdpTitle div.block')
                    product_info['product_name'] = name_element.text.strip()
                except:
                    product_info['product_name'] = None

                # Product URL
                product_info['product_url'] = category_url

                page_data.append(product_info)
                print(page_data)
            except Exception as e:
                print(f"Error extracting product info: {e}")
                continue

            # Save this page's data immediately
            # if page_data:
            #     with open(output_file, 'a', newline='', encoding='utf-8') as csvfile:
            #         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            #         writer.writerows(page_data)

            #     print(f"Saved {len(page_data)} products from page {page_number} to {output_file}")
            #     all_data.extend(page_data)
            # else:
            #     print("No products found on this page — stopping.")
            #     break

            # page_number += 1  # move to next page

        # print(f"Scraping complete. Total products: {len(all_data)}")
        # return all_data

    except Exception as e:
        print(f"Error scraping category: {e}")
        return []
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass
            del driver

def clean_price(value):
    if value:
        return value.replace("$", "").replace(",", "").replace("\xa0", "").strip()
    return "0"
            
def fetch_product_data():
    csv_file = f"{brand}.csv"
    products = []
    try:
        with open(csv_file, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            products = list(reader)

    except UnicodeDecodeError as e:
        logger.warning(f"UTF-8 decoding failed: {e}. Trying ISO-8859-1...")
        try:
            with open(csv_file, mode='r', encoding='ISO-8859-1') as file:
                reader = csv.DictReader(file)
                products = list(reader)
        except Exception as inner_e:
            logger.error(f"Failed to read CSV with fallback encoding: {inner_e}")
            logger.debug(traceback.format_exc())
            return []

    except Exception as e:
        logger.error(f"An error occurred while fetching product data: {e}")
        logger.debug(traceback.format_exc())
        return []

    logger.info(f"✅ Loaded {len(products)} products from CSV.")

    for row in products:
        raw_base = row.get('base_price')
        raw_msrp = row.get('msrp')
        raw_save = row.get('saveing_price')

        base_price = clean_price(raw_base)
        msrp = clean_price(raw_msrp)
        save_price = clean_price(raw_save)
        print(base_price)
        print(msrp)
        # Treat "0" like empty
        if base_price == "0": base_price = None
        if msrp == "0": msrp = None
        if save_price == "0": save_price = None
  
        # Apply logic
        if base_price and msrp and save_price:
            pass  # All present
        elif base_price and msrp:
            pass  # OK if no save price
        elif not base_price and msrp:
            base_price = msrp
            if msrp == base_price:
                if save_price:
                    msrp = save_price
                    save_price = None
                else:
                    msrp = None
        elif save_price and not msrp:
            msrp = save_price
            save_price = None

        # Final cleanup: avoid duplication
        if base_price == msrp:
            msrp = None
        
        brand_name = clean_price(row.get('brand_name'))
        
        temp = {
            'product_name': row.get('product_name'),
            'vendor_sku': row.get('mpn'),
            'product_mpn': row.get('mpn'),
            'brand_name': brand_name,
            'msrp': msrp,
            'product_image': row.get('image'),
            'product_url': row.get('product_url')
        }

        temp2 = {
            'vendorprice_price': base_price,
            'vendorprice_finalprice': base_price,
            'url': row.get('product_url'),
            'msrp': msrp
        }
        
        temp2['scraped_by_system'] = "Preeti pc"
        temp2['source'] = "direct_from_website"
        temp2['product_condition'] = 'New'

        print("Temp:", temp)
        print("Temp2:", temp2)
        print("=" * 50)
        
        product_id, vendor_product_id = insertIntoMsp(temp, vendor_id)
        print("--------------------------------------------------------")
        print(product_id, vendor_product_id)
        print("--------------------------------------------------------")
        if temp2['vendorprice_price'] is None:
            logger.debug(f"vendorprice_price not found!!")
            continue
        elif isinstance(temp2['vendorprice_price'], str):
            price_lower = temp2['vendorprice_price'].lower()
            if 'best price' in price_lower or 'price unavailable' in price_lower or 'call for best price' in price_lower or 'obsolete' in price_lower:
                logger.debug(f"vendorprice_price not found!! - Price requires contact: {temp2['vendorprice_price']}")
                continue
            else:
                insertall(product_id, vendor_product_id, temp2, vendor_id)
                # evalRanking(vendor_id,product_id)

def random_boolean():
    return random.choice([True, False])


def read_product_urls_from_file(filepath):
    with open(filepath, 'r') as f:
        return [line.strip() for line in f if line.strip()]

if __name__ == '__main__':
    try:        
        start = time.perf_counter() 
        vendor_id = 11654
        # brand = 'LG'
        # Category URL to scrape
        # category_url = f"https://www.fusespecialtyappliances.com/catalog/special/manufacturer/{brand}"
        # category_url = f"https://www.ajmadison.com/c/?brands={brand}"
        # mpns = read_product_urls_from_file(mpns.txt)
        mpns = ['KFGC506JMH','KFGC506JMH']
        for mpn in mpns:
            category_url = f"https://www.ajmadison.com/cgi-bin/ajmadison/{mpn}.html"
        # Scrape all products from the category with retry mechanism
        # product_list = scrape_category_products(category_url,vendor_id,brand, max_products=2000)
            products = scrape_category_products(category_url)
            print(products)
        # print(f"Scraped {len(products)} products.")
        
        fetch_product_data()
        end = time.perf_counter()
        logger.debug(f"Total execution time: {end - start:.2f} seconds")
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")