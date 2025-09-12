import re
import time
import json
import os
import csv
import logging
import requests
import random
import mysql.connector
from datetime import datetime
from bs4 import BeautifulSoup
from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver import FirefoxOptions
from datetime import datetime, timedelta, date
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from random import randint
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup, NavigableString, Tag
from modules.runTimeSecrets import HOST, DB, USER, PASS, HOST2, DB2, USER2, PASS2, HOST3, DB3, USER3, PASS3
from modules.saveRanks import commence as evalRanking
# ------------------------------------------------------------
# HOST, DB, USER, PASS = "157.245.132.95", "wpkhxfqkdc", "wpkhxfqkdc", "fslrITAZvB0Qj8G"
# HOST2, DB2, USER2, PASS2 = "157.245.132.95","kkffhzpedq","kkffhzpedq","MgXHgkx2Nm"
# HOST3, DB3, USER3, PASS3 = "157.245.132.95", "rphrevgncp", "rphrevgncp", "3xeQH6GeEp"
# ------------------------------------------------------------

# logger
# ------------------------------------------------------------
def loggerInit(logFileName):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
    file_handler = logging.FileHandler(f'/root/public/Directscraper/Ajmadison/logs/{logFileName}')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.DEBUG)
    logger.addHandler(stream_handler)
    return logger
logger = loggerInit(logFileName="ProdDetails.log")
# ---------------------------------------------------------------

# ---------------------- WEBDRIVER SETUP ----------------------
def triggerSelenium(useVPN=False, checkIP=False):
    logger.debug(f"Selenium triggered")

    geckoPath = f"/snap/bin/geckodriver"  # Path to your geckodriver
    with open("/root/public/Directscraper/Ajmadison/vpn.config.json") as json_data_file:
        configs = json.load(json_data_file)

    attempts = 0
    while attempts < 3:
        try:
            VPN_IP_PORT = configs['VPN_IP_PORT'][random.randint(0, len(configs['VPN_IP_PORT']) - 1)]
            # try:
            #     response = requests.get("https://www.google.com", proxies={"http": f"http://{VPN_IP_PORT}", "https": f"http://{VPN_IP_PORT}"}, timeout=50)
            # except requests.RequestException as e:
            #     print(f"Proxy Failed: {e}")
            #     attempts += 1
            #     continue 
            seleniumwire_options = {
                'proxy': {
                    "http": f"http://{VPN_IP_PORT}",
                    "https": f"http://{VPN_IP_PORT}",
                    'no_proxy': 'localhost,127.0.0.1'
                }
            }

            options = FirefoxOptions()
            options.add_argument('-headless')
            # options.add_argument('-private')
            service = Service(executable_path=geckoPath)
            # driver = webdriver.Firefox(service=service, options=options)
            if useVPN:
                driver = webdriver.Firefox(service=service, options=options, seleniumwire_options=seleniumwire_options)
            else:
                driver = webdriver.Firefox(service=service, options=options)
            try:
                if checkIP:
                    time.sleep(random.randint(10000, 100000) / 10000)  # Random delay
                    driver.get("https://ifconfig.me/")  # Get current IP
                    time.sleep(random.randint(50, 150) / 1000)  # Random delay
                    driver.refresh()
                    if '''<a href="http://ifconfig.me">What Is My IP Address? - ifconfig.me</a>''' in driver.page_source:
                        logger.debug(f"New Rotated IP: {driver.find_element(by=By.ID, value='ip_address').text}")
                        driver.get("https://ipinfo.io/")  # Double-check with ipinfo.io
                        time.sleep(random.randint(50, 150) / 1000)
                        driver.refresh()
                        return driver
                    time.sleep(random.randint(50, 150) / 1000)
                return driver 
            except Exception as e:
                driver.get_full_page_screenshot_as_file(f"logs/IP Check({datetime.now()})-Exception.png")
                driver.quit()
                raise Exception("BadSession")
        except Exception as e:
            logger.debug(f"Attempt {attempts+1}/10 failed with error: {e}")
            attempts += 1
            if attempts == 3:
                logger.debug(f"triggerSelenium() failed after 10 attempts")
                raise e
    
    
def getAllProUrl(category_url):
    try:
        product_urls = set()
        image_urls = set()
        new_category_url = category_url
        headers = {
            'authority': 'www.google.com',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'dnt': '1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'sec-fetch-site': 'none',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-dest': 'document',
            'accept-language': 'en-US,en;q=0.9',
            'sec-gpc': '1',
        }
        params = (
            ('hl', 'en-US'),
            ('gl', 'us'),
        )
        
        # driver.get(new_category_url)
        # time.sleep(3)
        # page = requests.get(category_url, headers=headers, params=params)
        # soup = BeautifulSoup(driver.page_source, 'html.parser')
        # open("soup.txt", "w", encoding="utf-8").write(str(soup))
        # exit()
        pageNumber = 1 
        while True:
            driver = triggerSelenium(checkIP=True,useVPN=False)
            if pageNumber == 1:
                url = new_category_url
                driver.get(url)
            else:
                url = f"{new_category_url}&page={pageNumber}"
                driver.get(url)

            time.sleep(6)
            logger.debug(url)
            # page = requests.get(url, headers=headers, params=params)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            # open("soup.txt", "w", encoding="utf-8").write(str(soup))
            # exit()
            products = soup.select('div.CustomInfiniteHits_grid-list__P9U1a a.GridItemHit_description-truncated__ijB77')
            if not products:
                logger.debug(f"No products found on page {pageNumber} for {url}.")
                break

            for product in products:
                if isinstance(product, Tag):
                    product_url = product.get('href')
                    if product_url and product_url not in product_urls:
                        product_urls.add(product_url)
                        with open("productUrls(preeti).txt1", "a", encoding="utf-8") as f:
                            f.write(domain + product_url + '\n')

            images = soup.select('div.CustomInfiniteHits_grid-list__P9U1a img')
            if not images:
                logger.debug(f"No images found on page {pageNumber} for {url}.")
                break

            for image in images:
                if isinstance(image, Tag):
                    image_url = image.get('src')
                    if image_url and image_url not in image_urls:
                        image_urls.add(image_url)
                        with open("productUrls(preeti)2.txt", "a", encoding="utf-8") as f:
                            f.write(image_url + '\n')

            print(f"Total unique product URLs found so far: {len(image_urls)}")
            print(f"Total unique product URLs found so far: {len(product_urls)}")
            pageNumber += 1
            if pageNumber == 50:
                break
            driver.quit()
        return product_urls
    finally:
        if driver:
            driver.quit()

import csv
import traceback

def clean_price(value):
    if value:
        return value.replace("$", "").replace(",", "").replace("\xa0","").strip()
    return "0"

def fetch_product_data():
    csv_file = "ajmadison-2025-09-01 KitchenAid.csv"
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
                evalRanking(vendor_id,product_id)

# Saving data to the MSP
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

            # Check if product exists
            checkProductQuery = "SELECT product_id, product_name, product_image FROM Product WHERE brand_id = %s AND product_mpn = %s"
            this.execute(checkProductQuery, [brand_id, mpn])
            records = this.fetchone()

            if records is None:
                if msrp != '':
                    insertProductQuery = """INSERT INTO Product (brand_id, product_name, product_mpn, msrp, product_image) VALUES (%s, %s, %s, %s, %s)"""
                    this.execute(insertProductQuery, (brand_id, name, mpn, msrp, image))
                else:
                    insertProductQuery = """INSERT INTO Product (brand_id, product_name, product_mpn, product_image) VALUES (%s, %s, %s, %s)"""
                    this.execute(insertProductQuery, (brand_id, name, mpn, image))
                conn.commit()
                logger.info(f'{vendor_id} >> Added new product with mpn "{mpn} ({this.lastrowid})".')
                return this.lastrowid
            else:
                product_id, product_name, product_image = records
                if product_name is None:
                    this.execute("UPDATE Product SET product_name = %s WHERE product_id = %s", [name, product_id])
                if product_image is None:
                    this.execute("UPDATE Product SET product_image = %s WHERE product_id = %s", [image, product_id])
                if msrp != '':
                    this.execute("UPDATE Product SET msrp = %s WHERE product_id = %s AND msrp IS NULL", [msrp, product_id])
                conn.commit()
                logger.info(f'{vendor_id} >> Updated details for product with mpn "{mpn} ({product_id})".')
                return product_id
            
    except mysql.connector.Error as e:
        logger.warning(f"{vendor_id} >> MySQL ERROR checkInsertProduct() >> {e}")
        logger.warning(f"{vendor_id}, {brand_id}, {mpn}, {name}, {msrp}, {image}")
        return None

    finally:
        if conn.is_connected():
            this.close()
            conn.close()


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
                updateProductDetailQuery = "UPDATE ProductVendor SET vendor_sku = %s, product_name = %s, msrp = %s WHERE vendor_product_id = %s"
                this.execute(updateProductDetailQuery, [sku, name, msrp, vp_id])
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
        vendorTempPricing(vendor_product_id, temp)
        rpVendorPricingHistory(vendor_product_id, temp, vendor_id)
        productMsrpUpdate(product_id, temp)
        productVendorMsrpUpdate(vendor_product_id, temp)
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

                    # updateQuery = """UPDATE TempVendorPricing SET is_suspicious = %s WHERE vendor_product_id = %s AND source = %s"""
                    # values = ("0" ,vendor_product_id, temp['source'])
                    # this.execute(updateQuery, values)
                    # conn.commit()
                else:
                    updateQuery = """UPDATE TempVendorPricing SET is_price_changed = %s, price_changed_date = %s WHERE vendor_product_id = %s AND source = %s"""
                    values = ("1", dateTime,vendor_product_id, temp['source'])
                    this.execute(updateQuery, values)
                    conn.commit()
                    logger.info(f"is_price_changed set 1 for vendor_product_id ({vendor_product_id}).")
                updateQuery = """UPDATE TempVendorPricing SET vendorprice_price = %s, vendorprice_finalprice = %s, vendorprice_date = %s, product_condition = %s, is_rp_calculated = %s, is_member = %s, scraped_by_system = %s
                    WHERE vendor_product_id = %s AND source = %s"""
                values = (temp['vendorprice_price'], temp['vendorprice_finalprice'], dateTime, temp['product_condition'], '2', '0', temp['scraped_by_system'], vendor_product_id, temp['source'])
                this.execute(updateQuery, values)
                conn.commit()
                logger.info(f"Record Updated for vendor_product_id ({vendor_product_id}) and source ({temp['source']})")
            else:
                insertQuery = """INSERT INTO TempVendorPricing (vendor_product_id, vendorprice_price, vendorprice_finalprice, vendorprice_date, product_condition, source, is_rp_calculated, is_member, scraped_by_system) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                values = (vendor_product_id, temp['vendorprice_price'], temp['vendorprice_finalprice'], dateTime, temp['product_condition'], temp['source'], '2', '0', temp['scraped_by_system'])
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

            insertQuery = f"""INSERT INTO {vendor_pricing_table} (vendor_product_id, vendorprice_price, vendorprice_finalprice, vendorprice_date, 
                product_condition, source, is_rp_calculated, is_member, scraped_by_system) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            values = (vendor_product_id, temp['vendorprice_price'], temp['vendorprice_finalprice'], dateTime, temp['product_condition'], temp['source'], '2', '0', temp['scraped_by_system'])
            this.execute(insertQuery, values)
            conn.commit()
            logger.info(f"Record Inserted for vendor_product_id ({vendor_product_id}) and source ({temp['source']}) In {vendor_pricing_table} history table.")
    except mysql.connector.Error as e:
        logger.warning(f"MySQL ERROR {vendor_pricing_table} >> {e}")
    finally:
        if conn.is_connected():
            conn.close()
            this.close()

# Updating MSRP in Product table
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

def read_product_urls_from_file(filepath):
    with open(filepath, 'r') as f:
        return [line.strip() for line in f if line.strip()]

def fetchbrandCat(url, output_file='brand_category_urls1.txt'):
    print(f"Processing brand page: {url}")
    headers = {
        'authority': 'www.google.com',
        'pragma': 'no-cache',
        'cache-control': 'no-cache',
        'dnt': '1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'sec-fetch-site': 'none',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-dest': 'document',
        'accept-language': 'en-US,en;q=0.9',
        'sec-gpc': '1',
    }
    params = (
        ('hl', 'en-US'),
        ('gl', 'us'),
    )
    
    try:
        page = requests.get(url, headers=headers, params=params, timeout=10)
        page.raise_for_status()
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return

    soup = BeautifulSoup(page.content, 'html.parser')
    brand_links = soup.select('div.brand_categories a.cat_txt')

    full_links = []
    for tag in brand_links:
        href = tag.get('href')
        if href:
            if href.startswith('/'):
                href = 'https://tigerchef.com' + href
            full_links.append(href)
    
    full_links = list(set(full_links))
    logger.debug(full_links)
    # Save to file
    with open(output_file, 'a', encoding='utf-8') as f:
        for link in full_links:
            f.write(link + '\n')

def fetchBrandUrl():
    url = "https://www.mrosupply.com/brands/"
    headers = {
            'authority': 'www.google.com',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'dnt': '1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'sec-fetch-site': 'none',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-dest': 'document',
            'accept-language': 'en-US,en;q=0.9',
            'sec-gpc': '1',
        }
    params = (
        ('hl', 'en-US'),
        ('gl', 'us'),
    )
    page = requests.get(url, headers=headers, params=params)
    soup = BeautifulSoup(page.content, 'html.parser')
    # open("soup.txt", "a", encoding="utf-8").write(str(soup))
    # exit()
    
    brand_links = soup.select('div.js-brands-list a')

    full_links = []
    for tag in brand_links:
        href = tag.get('href')
        if href:
            if href.startswith('/'):
                href = domain + href
            full_links.append(href)
    full_links = list(set(full_links))
    
    for link in full_links:
        logger.debug(f"Brand url : {link}")

    with open("brand_category_urls.txt", 'a', encoding='utf-8') as f:
        for link in full_links:
            f.write(link + '\n')    
    # return full_links


if __name__ == "__main__":  
    start = time.perf_counter() 
    catUrllist = [
       "https://www.ajmadison.com/c/?brands=Sharp"
    ]
    vendor_url = "https://www.ajmadison.com/"
    vendor_id = 11654
    domain = "https://www.ajmadison.com"
    
    fetch_product_data()

    finish = time.perf_counter()
    logger.debug(f'Finished ThreadMain in {round(finish - start, 2)} second(s)')