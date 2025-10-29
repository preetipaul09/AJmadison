import time
import logging
import math
import json
import mysql.connector
from random import uniform
from datetime import datetime
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from undetected_chromedriver import ChromeOptions
from modules.dbScript import coreDB as DBFunctionsForDirectScraping
from modules.runTimeSecrets import HOST, DB, USER, PASS, HOST3, DB3, PASS3, USER3, HOST4 , DB4 ,USER4 ,PASS4
from modules.saveRanks import commence as evalRanking
from selenium.common.exceptions import NoSuchElementException
from multiprocessing import Pool
from datetime import datetime
# ------------------------------------------------------------
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
logger = loggerInit(logFileName="pricing.log")
# ---------------------------------------------------------------

# Marking process as started
def DailyProcessStart(process_name):
    try:
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor()
            this.execute("""
                SELECT
                    daily_process_id
                FROM DailyProcess
                WHERE
                    process_name = %s
                    AND DATE(created_at) = %s
                ORDER BY daily_process_id ASC
                LIMIT 1;
            """, (process_name, datetime.now().strftime("%Y-%m-%d"),))
            record = this.fetchone()
            if record:
                this.execute("UPDATE DailyProcess SET is_started = %s WHERE daily_process_id = %s;", ('1', record[0]))
                conn.commit()
            else:
                this.execute("INSERT INTO DailyProcess (process_name, is_started) VALUES (%s,%s)", (process_name, '1'))
                conn.commit()
    except mysql.connector.Error as e:
        # print("MySQL ERROR DailyProcessStart() >>", e)
        logger.debug(f"MySQL ERROR DailyProcessStart() >> {e}")
    finally:
        if conn.is_connected():
            this.close()
            conn.close()

def getUrls(vendor_id, vendor_url, num_parts=5, distributed=True):
    """
    Fetches all product URLs for a vendor, splits them into chunks, and processes them.

    Args:
        vendor_id (int): Vendor ID to filter.
        vendor_url (str): Base vendor URL.
        num_parts (int): How many parts/chunks to split into.
        distributed (bool): If True, save chunks to JSON files for other PCs.
    """
    conn, conn_1 = None, None
    try:
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        conn_1 = mysql.connector.connect(host=HOST4, database=DB4, user=USER4, password=PASS4)

        productIds = []
        if conn_1.is_connected():
            this1 = conn_1.cursor(buffered=True)
            getProductIds = """
                SELECT DISTINCT(121PreGeneratedReportsData.product_id)
                FROM 121PreGeneratedReportsData
                INNER JOIN 121PreGeneratedReports 
                    ON 121PreGeneratedReports.generated_report_id = 121PreGeneratedReportsData.generated_report_id
                WHERE 121PreGeneratedReports.base_vendor_id = %s 
                   OR 121PreGeneratedReports.competitor_vendor_id = %s
            """
            this1.execute(getProductIds, (vendor_id, vendor_id))
            productIds = [row[0] for row in this1.fetchall()]
            this1.close()

        if not productIds:
            logger.warning("No product IDs found for given vendor.")
            return

        logger.info(f"Found {len(productIds)} product IDs to process")

        if conn.is_connected():
            this2 = conn.cursor(buffered=True)
            getVendorURLQuery = f"""
            SELECT 
                ProductVendor.vendor_product_id,
                Product.product_id,
                Product.product_mpn,
                VendorURL.vendor_url
            FROM VendorURL
            INNER JOIN ProductVendor ON ProductVendor.vendor_product_id = VendorURL.vendor_product_id
            INNER JOIN Product ON Product.product_id = ProductVendor.product_id
            INNER JOIN Brand ON Brand.brand_id = Product.brand_id	
            WHERE ProductVendor.vendor_id = %s
                  AND VendorURL.vendor_url NOT IN ('https://www.ajmadison.com/cgi-bin/ajmadison/HMC54151UC.html')
                  AND Product.product_id IN ({','.join(['%s'] * len(productIds))})
            """
            params = [vendor_id] + productIds
            this2.execute(getVendorURLQuery, params)
            url_list = this2.fetchall()
            this2.close()

            if not url_list:
                logger.warning("No URLs found for given product IDs.")
                return

            logger.info(f"Found {len(url_list)} URLs to process")

            # Split URLs into chunks
            chunk_size = math.ceil(len(url_list) / num_parts)
            chunks = [url_list[i:i + chunk_size] for i in range(0, len(url_list), chunk_size)]

            if distributed:
                # Save each chunk to JSON file for remote processing
                for i, chunk in enumerate(chunks, start=1):
                    filename = f"urls_chunk_{i}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    with open(filename, "w", encoding="utf-8") as f:
                        json.dump(chunk, f, ensure_ascii=False, indent=2)
                    logger.info(f"Saved {len(chunk)} URLs to {filename}")
                logger.info("Distribute these files to remote PCs and run scraper_worker.py on each.")
            else:
                # Run locally using multiprocessing
                with Pool(num_parts) as pool:
                    pool.starmap(process_chunk, [(chunk, vendor_url, vendor_id) for chunk in chunks])

    except mysql.connector.Error as e:
        logger.warning(f"MySQL ERROR getUrls() >> {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()
        if conn_1 and conn_1.is_connected():
            conn_1.close()


def process_chunk(chunk, vendor_url, vendor_id):
    for vendor_product_id, product_id, product_mpn, url in chunk:
        if product_mpn:
            product_mpn = product_mpn.strip()
        if url and "html&" in url:
            url = url.split("html&")[0] + "html"

        try:
            logger.info(f"Processing URL: {url}")
            scraper_unit(vendor_product_id, product_id, url, vendor_url, vendor_id, product_mpn)
        except Exception as e:
            logger.error(f"Error processing URL {url}: {e}")

def scraper_worker(vendor_url, vendor_id, file_path):
    """
    Processes a chunk file containing URLs.
    """
    with open(file_path, "r", encoding="utf-8") as f:
        chunk = json.load(f)

    logger.info(f"Loaded {len(chunk)} URLs from {file_path}")

    for vendor_product_id, product_id, product_mpn, url in chunk:
        if product_mpn:
            product_mpn = product_mpn.strip()
        if url and "html&" in url:
            url = url.split("html&")[0] + "html"

        try:
            logger.info(f"Processing URL: {url}")
            scraper_unit(vendor_product_id, product_id, url, vendor_url, vendor_id, product_mpn)
        except Exception as e:
            logger.error(f"Error processing {url}: {e}")

def scraper_unit(vendor_product_id, product_id, url, vendor_url, vendor_id, product_mpn):
    try:
        temp = {}
        logger.debug(url)
        driver = triggerSelenium_chrome(useVPN=False)
        driver.get(url)
        time.sleep(5)
        driver.refresh()
        random_pause(15, 20)

       # Try multiple selectors for MPN
        selectors = [
            ('div#quote-request', 'data-sku'),
            ('span[itemprop="mpn"]', 'textContent'),
            ('h1.m0.pdpTitle div.inline-block.gray-5.clearfix', 'textContent'),
            ('div.js-shipping-info', 'data-sku'),
            ('div.js-stock-notifier', 'data-sku'),
            ('input[name="sku"]', 'value'),
        ]

    
        temp['vendor_product_id'] = vendor_product_id
        temp['vendorprice_price'] = None
        temp['vendorprice_finalprice'] = None
        temp['msrp'] = None

        # Price
        price_elements = driver.find_elements(By.CSS_SELECTOR, 'div[itemprop="price"] span')
        price_text = price_elements[0].get_attribute("innerText").strip() if price_elements else ""

        if '-' in price_text:
            print(f"Skipping product due to base price range: {price_text}")
            return None

        if price_text:
            # Remove unwanted symbols
            clean_price = (
                price_text.replace("$", "")
                .replace(",", "")
                .replace("Rs.", "")
                .replace(r"\ea", "")
                .strip()
            )

            # Handle case like '842500' (where last 2 digits are cents)
            if clean_price.replace(".", "").isdigit():
                if "." not in clean_price and len(clean_price) > 2:
                    clean_price = clean_price[:-2] + "." + clean_price[-2:]

            base_price = f"{float(clean_price):.2f}"

        else:
            # Fallback selector
            price_element = driver.find_element(
                By.CSS_SELECTOR,
                'div#package-root div.Package_add-to-cart-overlay__Q0xHM.Package_add-to-cart-overlay-desktop__cyRgA p'
            )

            price_text = price_element.get_attribute("innerText") if price_element else ""
            clean_price = (
                price_text.replace("$", "")
                .replace(",", "")
                .replace("Rs.", "")
                .replace(r"\ea", "")
                .strip())
            if clean_price.replace(".", "").isdigit():
                if "." not in clean_price and len(clean_price) > 2:
                    clean_price = clean_price[:-2] + "." + clean_price[-2:]
            base_price = f"{float(clean_price):.2f}" if clean_price else None

        # MSRP
        try:
            try:
                msrp_element = driver.find_element(By.CSS_SELECTOR, 'td[itemprop="priceSpecification"] del')
            except NoSuchElementException:
                msrp_element = driver.find_element(
                    By.CSS_SELECTOR,
                    'div#package-root div.Package_add-to-cart-overlay__Q0xHM.Package_add-to-cart-overlay-desktop__cyRgA p.PackagePriceMain_price-list__ewa3z'
                )
            msrp_text = msrp_element.get_attribute("innerText").strip()
            clean_msrp = (
                msrp_text.replace("$", "")
                .replace(",", "")
                .replace("Rs.", "")
                .replace(r"\ea", "")
                .strip()
            )

            if clean_msrp.replace(".", "").isdigit():
                if "." not in clean_msrp and len(clean_msrp) > 2:
                    clean_msrp = clean_msrp[:-2] + "." + clean_msrp[-2:]

                product_msrp = f"{float(clean_msrp):.2f}"
            else:
                product_msrp = None
        except NoSuchElementException:
            product_msrp = None

        temp['vendorprice_price'] = base_price
        temp['vendorprice_finalprice'] = base_price
        temp['msrp'] = product_msrp
        temp['discount'] = 0.00
        temp["vendorprice_stock_text"] = None
        temp['source'] = "direct_from_website"
        temp['product_condition'] = "New"
        temp['no_of_pieces'] = None
        temp['vendorprice_additional_savings'] = 0.00

        print(temp, product_id)

        if temp['vendorprice_price'] is None:
            logger.warning(f"No price found for product ID {product_id}")
            with open("priceNotFound.txt", "a") as file:
                file.write(f"{vendor_product_id}\n")
            return
        else:
            vendorTempPricing(temp)
            # vendorZPricing(temp, vendor_id)
            evalRanking(vendor_id, product_id)

        if temp["msrp"]:
            productMsrpUpdate(product_id, temp)
            productVendorMsrpUpdate(temp)

    except Exception as e:
        logger.error(f"An error occurred in scraper_unit: {e}")
    finally:
        if driver:
            driver.quit()

def random_pause(min_time=2, max_time=5):
    """
    Add a random pause to simulate human thinking or waiting.
    """
    time.sleep(uniform(min_time, max_time))

# Saving data to the MSP
def insertIntoMsp(row, vendor_id):
    product_id = vendor_product_id = None
    try:
        brand_id = checkInsertBrand(vendor_id, row['brand_name'])
        product_id = checkInsertProduct(vendor_id, brand_id, row['product_mpn'], row['product_name'], row['msrp'], row['product_image'])
        vendor_product_id = checkInsertProductVendor(vendor_id, product_id, row['vendor_sku'], row['product_name'], row['product_url'], row['msrp'])
        checkInsertProductVendorURL(vendor_id, vendor_product_id, row['product_url'])
    except Exception as e:
        logger.error(f"Error in insertIntoMsp: {e}")
    return product_id, vendor_product_id

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

# Temp vnendor pricing data
def vendorTempPricing(data):
    currentDateTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor(buffered=True)
            checkQuery = "SELECT vendor_product_id FROM TempVendorPricing WHERE vendor_product_id = %s AND source = %s LIMIT 1"
            this.execute(checkQuery, (data['vendor_product_id'], data['source']))
            records = this.fetchone()
            if records:
                updateQuery = "UPDATE TempVendorPricing SET vendorprice_price = %s, vendorprice_finalprice = %s, vendorprice_date = %s, vendorprice_extra_discount = %s, vendorprice_additional_savings = %s, vendorprice_stock_text = %s, product_condition = %s, is_rp_calculated = %s, no_of_pieces = %s WHERE vendor_product_id = %s AND source = %s"
                values = (data['vendorprice_price'], data['vendorprice_finalprice'], currentDateTime, data['discount'], data['vendorprice_additional_savings'], data['vendorprice_stock_text'], data['product_condition'], '2', data['no_of_pieces'], data['vendor_product_id'], data['source'])
                this.execute(updateQuery, values)
                conn.commit()
                logger.info(f"Record Updated for vendor_product_id ({data['vendor_product_id']}) and source ({data['source']})")
                # print(f"Record Updated for vendor_product_id ({data['vendor_product_id']}) and source ({data['source']})")
            else:
                insertQuery = "INSERT INTO TempVendorPricing (vendor_product_id, vendorprice_price, vendorprice_finalprice, vendorprice_date, vendorprice_extra_discount, vendorprice_additional_savings, vendorprice_stock_text, product_condition, source, is_rp_calculated, no_of_pieces) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                values = (data['vendor_product_id'], data['vendorprice_price'], data["vendorprice_finalprice"], currentDateTime, data['discount'], data['vendorprice_additional_savings'], data['vendorprice_stock_text'], data['product_condition'], data['source'], '2',data['no_of_pieces'])
                this.execute(insertQuery, values)
                conn.commit()
                logger.info(f"Record Inserted for vendor_product_id ({data['vendor_product_id']}) and source ({data['source']})")
                # print(f"Record Inserted for vendor_product_id ({data['vendor_product_id']}) and source ({data['source']})")
    except mysql.connector.Error as e:
        logger.warning(f"MySQL ERROR vendorTempPricing() >> {e}")
        # print(f"MySQL ERROR vendorTempPricing() >> {e}")
    # except Exception as ex:
    #     logger.error(f"Unexpected error: {ex}")
        # print(f"Unexpected error: {ex}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            conn.close()

def vendorZPricing(data, vendor_id):
    try:
        conn = mysql.connector.connect(host=HOST3, database=DB3, user=USER3, password=PASS3)
        if conn.is_connected():
            this = conn.cursor()
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

            insertQuery = f"INSERT INTO {vendor_pricing_table} (vendor_product_id, vendorprice_price, vendorprice_finalprice, vendorprice_date, vendorprice_extra_discount, vendorprice_additional_savings, vendorprice_stock_text, product_condition, source, no_of_pieces) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (data['vendor_product_id'], data['vendorprice_price'], data["vendorprice_finalprice"], datetime.now().strftime("%Y-%m-%d %H:%M:%S"), data['discount'], data['vendorprice_additional_savings'], data['vendorprice_stock_text'], data['product_condition'], data['source'], data['no_of_pieces'])
            this.execute(insertQuery, values)
            conn.commit()
            logger.info(f"Record Inserted for vendor_product_id ({data['vendor_product_id']}) and source ({data['source']}) Z_{vendor_id}_VendorPricing")
            # print(f"Record Inserted for vendor_product_id ({data['vendor_product_id']}) and source ({data['source']}) Z_")
    except mysql.connector.Error as e:
        logger.warning(f"MySQL ERROR vendorTempPricing() >> {e}")
        # print(f"MySQL ERROR vendorTempPricing() >> {e}")
    finally:
        if conn.is_connected():
            conn.close()

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

def productMsrpUpdate(product_id, data):
    # print(product_id, data['msrp'])
    try:
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor(buffered=True)
            this.execute("SELECT msrp FROM Product WHERE product_id = %s", (product_id,))
            result = this.fetchone()
            if result:
                # Update MSRP
                if data['msrp']:
                    this.execute("UPDATE Product SET msrp = %s WHERE product_id = %s", (data['msrp'], product_id))
                    conn.commit()
                    # print(f"Record Updated for product_id ({product_id}).")
                    logger.info(f"Record Updated for product_id ({product_id}).")
    except mysql.connector.Error as e:
        logger.warning(f"{product_id} >> MySQL ERROR productMsrpUpdate() >> {e}")
    finally:
        if conn.is_connected():
            conn.close()
            this.close()

# Updating MSRF in ProductVendor table
def productVendorMsrpUpdate(data):
    # print(data['vendor_product_id'], data['msrp'])
    try:
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor(buffered=True)
            this.execute("SELECT msrp FROM ProductVendor WHERE vendor_product_id = %s", (data['vendor_product_id'],))
            result = this.fetchone()
            if result:
                # Update MSRP
                if data['msrp']:
                    this.execute("UPDATE ProductVendor SET msrp = %s WHERE vendor_product_id = %s", (data['msrp'], data['vendor_product_id']))
                    conn.commit()              
                    # print(f"Record Updated for vendor_product_id ({data['vendor_product_id']}).")
                    logger.info(f"Record Updated for vendor_product_id ({data['vendor_product_id']}).")

    except mysql.connector.Error as e:
        logger.warning(f"{data['vendor_product_id']} >> MySQL ERROR productVendorMsrpUpdate() >> {e}")
    finally:
        if conn.is_connected():
            conn.close()
            this.close()

def Counts(vendor_id):
    from datetime import datetime
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date = current_date.split()[0]
    try:
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor()
            # Ams - total products scraped
            this.execute("""
                SELECT
                    COUNT(DISTINCT ProductVendor.product_id)
                FROM ProductVendor
                INNER JOIN TempVendorPricing ON TempVendorPricing.vendor_product_id = ProductVendor.vendor_product_id
                WHERE
                    TempVendorPricing.vendorprice_date = %s
                    AND ProductVendor.vendor_id = %s
                    AND TempVendorPricing.source = %s;
            """, (date, vendor_id, "direct_from_website"))
            count = this.fetchone()
            if count:
                totalScrapedProducts = count[0]
            else:
                totalScrapedProducts = 0
            
            return totalScrapedProducts
    except mysql.connector.Error as e:
        # print("MySQL ERROR Counts() >>", e)
        logger.debug(f"MySQL ERROR Counts() >> {e}")
    finally:
        if conn.is_connected():
            this.close()
            conn.close()

# Marking process as completed
def DailyProcessCompleted(process_name, counts):
    try:
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor()
            this.execute("""
                SELECT
                    daily_process_id
                FROM DailyProcess
                WHERE
                    process_name = %s
                    AND DATE(created_at) = %s
                ORDER BY daily_process_id ASC
                LIMIT 1;
            """, (process_name, datetime.now().strftime("%Y-%m-%d"),))
            record = this.fetchone()
            if record:
                this.execute("UPDATE DailyProcess SET is_completed = %s, counts = %s WHERE daily_process_id = %s;", ('1', counts, record[0]))
                conn.commit()
    except mysql.connector.Error as e:
        # print("MySQL ERROR DailyProcessCompleted() >>", e)
        logger.debug(f"MySQL ERROR DailyProcessCompleted() >> {e}")
    finally:
        if conn.is_connected():
            this.close()
            conn.close()

import json, random, time
import undetected_chromedriver as uc
# from selenium.webdriver.chrome.options import Options
from undetected_chromedriver import ChromeOptions

def triggerSelenium_chrome(useVPN=False, checkIP=False, config_path="vpn.config.json"):
    with open(config_path) as f:
        configs = json.load(f)

    vpn_user = configs['VPN_User']
    vpn_pass = configs['VPN_Pass']
    vpn_ip_port = random.choice(configs['VPN_IP_PORT'])

    seleniumwire_options = {
        'proxy': {
            "http": f"http://{vpn_user}:{vpn_pass}@{vpn_ip_port}",
            "https": f"https://{vpn_user}:{vpn_pass}@{vpn_ip_port}",
            'no_proxy': 'localhost,127.0.0.1'
        }
    }
    print(vpn_ip_port)
    options = ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    # options.add_argument("--headless=new")

    driver = uc.Chrome(version_main=141, options=options, seleniumwire_options=seleniumwire_options)

    if checkIP:
        driver.get("https://api.ipify.org")
        ip = driver.find_element("tag name", "body").text
        print("Proxy IP:", ip)

    return driver

if __name__ == '__main__':
    try:
        start = time.perf_counter()
        
        process_name = 'AJ Madison Daily Pricing Scraper'
        
        DailyProcessStart(process_name)
        vendor_url = "https://www.ajmadison.com/"
        domain = "https://www.ajmadison.com"
        vendor_id = 11654
        scraper_worker(vendor_url, vendor_id, file_path="urls_chunk_1_20251029_102502.json",)
        totalScrapedProducts = Counts(vendor_id)
        
        print("-----------------------")
        # print(totalScrapedProducts)
        logger.info(totalScrapedProducts)
        print("-----------------------")

        DailyProcessCompleted(process_name, totalScrapedProducts)
        
        finish = time.perf_counter()
        logger.info(f'Finished processes in {round(finish - start, 2)} second(s)')
        logger.info('----------------------------------- END -----------------------------------')
        print(f'Finished processes in {round(finish - start, 2)} second(s)')

    except Exception as e:
        logger.error(f"An error occurred: {e}")
