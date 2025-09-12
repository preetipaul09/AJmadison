import logging
import mysql.connector
from datetime import datetime
from modules.runTimeSecrets import HOST, DB, USER, PASS, HOST2, DB2, USER2, PASS2, HOST3, DB3, USER3, PASS3
from modules.saveRanks import commence as evalRanking
# logger
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
logger = loggerInit(logFileName="dbScript.log")
# ---------------------------------------------------------------

# Saving data to the MSP
def insertIntoMsp(vendor_product_data, vendor_id):
    product_id = vendor_product_id = None  # Initialize to None

    brand_name = vendor_product_data.get('brand_name', '') or None
    product_mpn = vendor_product_data.get('product_mpn', '') or None
    product_name = vendor_product_data.get('product_name', '') or None
    msrp = vendor_product_data.get('msrp', '') or None
    product_image = vendor_product_data.get('product_image', '') or None
    vendor_sku = vendor_product_data.get('vendor_sku', '') or None
    product_url = vendor_product_data.get('product_url', '') or None

    if not brand_name or not product_mpn:
        logger.error(f"Missing required fields for vendor_id {vendor_id}")
        return None, None  # Return None if required fields are missing
    try:
        brand_id = checkInsertBrand(vendor_id, brand_name)
        product_id = checkInsertProduct(vendor_id, brand_id, product_mpn, product_name, msrp, product_image)
        vendor_product_id = checkInsertProductVendor(vendor_id, product_id, vendor_sku, product_name, product_url, msrp)
        checkInsertProductVendorURL(vendor_id, vendor_product_id, product_url)
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
                # this.execute("UPDATE Product SET product_name = %s, product_image = %s WHERE product_id = %s", [name,image,product_id])
                # conn.commit()
                # if msrp != '':
                #     this.execute("UPDATE Product SET msrp = %s WHERE product_id = %s AND msrp IS NULL", [msrp,product_id])
                #     conn.commit()
                # logger.info(f'{vendor_id} >> Updated details for product with mpn "{mpn} ({product_id})".')
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
                updateProductDetailQuery = "UPDATE ProductVendor SET vendor_sku = %s, product_name = %s, msrp = %s WHERE vendor_product_id = %s"
                this.execute(updateProductDetailQuery, [sku, name, msrp, vp_id])
                conn.commit()
                # if this.rowcount == 1:
                logger.info(f'{vendor_id} >> Updated details for vendor_product_id ({vp_id}).')
                # logger.info(f'{vendor_id} >> Returned vendor_product_id ({vp_id}).')
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
                fatchquary = "SELECT vendor_url_id, vendor_raw_url, vendor_url FROM VendorURL WHERE vendor_product_id = %s"
                this.execute(fatchquary, [vendor_product_id])
                results = this.fetchall()
                if results[0][2] != url:
                # Update the existing record
                    updateProductVendorURLQuery = """UPDATE VendorURL SET vendor_url = %s WHERE vendor_product_id = %s"""
                    this.execute(updateProductVendorURLQuery, [product_url, url, vendor_product_id])
                    conn.commit()
                    logger.info(f'{vendor_id} >> Updated product vendor URL for vendor_product_id "{vendor_product_id}".')
                else:
                    logger.info(f'{vendor_id} >> Same Product vendor URL already exists for vendor_product_id "{vendor_product_id}".')
    except mysql.connector.Error as e:
        logger.warning(f"{vendor_id} >> MySQL ERROR checkInsertProductVendorURL() >> {e}")
    finally:
        if conn.is_connected():
            conn.close()
            this.close()

# call all function into this function
def insertall(product_id, vendor_product_id, vendor_price_data, vendor_id):

    vendorprice_price = vendor_price_data.get('vendorprice_price', '') or None
    vendorprice_finalprice = vendor_price_data.get('vendorprice_finalprice', '') or None
    discount = vendor_price_data.get('discount', '') or '0.00'
    product_condition = vendor_price_data.get('product_condition', '') or None
    source = vendor_price_data.get('source', '') or None
    vendorprice_stock = vendor_price_data.get('vendorprice_stock', '') or None
    vendorprice_stock_text = vendor_price_data.get('vendorprice_stock_text', '') or None
    delivery_text = vendor_price_data.get('delivery_text', '') or None
    vendorprice_delivery_date = vendor_price_data.get('vendorprice_delivery_date', '') or None

    if not vendor_product_id or not vendorprice_price:
        logger.error(f"Missing required fields for vendor_product_id {vendor_product_id}")
        return  # Exit the function early if required fields are missing
    try:
        vendorTempPricing(vendor_product_id, vendorprice_price, vendorprice_finalprice, discount, vendorprice_stock, vendorprice_stock_text, delivery_text, vendorprice_delivery_date, product_condition, source)
        rpVendorPricingHistory(vendor_product_id, vendor_id, vendorprice_price, vendorprice_finalprice, discount, vendorprice_stock, vendorprice_stock_text, delivery_text, vendorprice_delivery_date, product_condition, source)
        # productMsrpUpdate(product_id, vendor_price_data)
        # productVendorMsrpUpdate(vendor_product_id, vendor_price_data)
    except Exception as e:
        logger.error(f"Error in insertall(): {e}")

def getDatetime():
    currentDatetime = datetime.now()
    return currentDatetime.strftime("%Y-%m-%d %H:%M:%S")

# Temp vnendor pricing data
def vendorTempPricing(vendor_product_id, vendorprice_price, vendorprice_finalprice, discount, vendorprice_stock, vendorprice_stock_text, delivery_text, vendorprice_delivery_date, product_condition, source):
    dateTime = getDatetime()
    try:
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor(buffered=True)
            checkQuery = "SELECT vendor_product_id FROM TempVendorPricing WHERE vendor_product_id = %s AND source = %s LIMIT 1"
            this.execute(checkQuery, (vendor_product_id, source))
            records = this.fetchone()
            if records:
                getPricequary = "SELECT * FROM TempVendorPricing WHERE vendor_product_id = %s AND source = 'direct_from_website'"
                this.execute(getPricequary, (records[0],))
                result = this.fetchone()
                savedprice = str(result[2])
                scrapedprice = str(vendorprice_price)
                if savedprice == scrapedprice:
                    logger.info(f"Same vendor price already exists for vendor_product_id {vendor_product_id}")
                else:
                    updateQuery = """UPDATE TempVendorPricing SET is_price_changed = %s, price_changed_date = %s WHERE vendor_product_id = %s AND source = %s"""
                    values = ("1", dateTime, vendor_product_id, source)
                    this.execute(updateQuery, values)
                    conn.commit()
                    logger.info(f"is_price_changed set 1 for vendor_product_id ({vendor_product_id}).")
                updateQuery = """UPDATE TempVendorPricing SET vendorprice_price = %s, vendorprice_finalprice = %s, vendorprice_date = %s, 
                    vendorprice_stock = %s, vendorprice_stock_text = %s, vendorprice_delivery_date = %s, delivery_text = %s, vendorprice_extra_discount = %s, 
                    product_condition = %s, is_rp_calculated = %s WHERE vendor_product_id = %s AND source = %s"""
                values = (vendorprice_price, vendorprice_finalprice, dateTime, vendorprice_stock, vendorprice_stock_text, vendorprice_delivery_date, 
                          delivery_text, discount, product_condition, '2', vendor_product_id, source)
                this.execute(updateQuery, values)
                conn.commit()
                logger.info(f"Record Updated for vendor_product_id ({vendor_product_id}) and source ({source})")
            else:
                insertQuery = """INSERT INTO TempVendorPricing (vendor_product_id, vendorprice_price, vendorprice_finalprice, vendorprice_date, 
                    vendorprice_stock, vendorprice_stock_text, vendorprice_delivery_date, delivery_text, vendorprice_extra_discount, product_condition, source, is_rp_calculated) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
                values = (vendor_product_id, vendorprice_price, vendorprice_finalprice, dateTime, vendorprice_stock, vendorprice_stock_text, vendorprice_delivery_date, 
                          delivery_text, discount, product_condition, source, '2')
                this.execute(insertQuery, values)
                conn.commit()
                logger.info(f"Record Inserted for vendor_product_id ({vendor_product_id}) and source ({source})")
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

def rpVendorPricingHistory(vendor_product_id, vendor_id, vendorprice_price, vendorprice_finalprice, discount, vendorprice_stock, vendorprice_stock_text, delivery_text, vendorprice_delivery_date, product_condition, source):
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
                    vendorprice_stock, vendorprice_stock_text, vendorprice_delivery_date, delivery_text, vendorprice_extra_discount, product_condition, source, is_rp_calculated) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            values = (vendor_product_id, vendorprice_price, vendorprice_finalprice, dateTime, vendorprice_stock, vendorprice_stock_text, vendorprice_delivery_date, delivery_text, discount, product_condition, source, '2')
            this.execute(insertQuery, values)
            conn.commit()
            logger.info(f"Record Inserted for vendor_product_id ({vendor_product_id}) and source ({source}) In {vendor_pricing_table} history table.")
            logger.info(f"==========================================")
    except mysql.connector.Error as e:
        logger.warning(f"MySQL ERROR {vendor_pricing_table} >> {e}")
    finally:
        if conn.is_connected():
            conn.close()
            this.close()

# Updating MSRF in Product table
def productMsrpUpdate(product_id, vendor_price_data):
    try:
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor(buffered=True)
            this.execute("SELECT msrp FROM Product WHERE product_id = %s", (product_id,))
            result = this.fetchone()
            if result:
                # Update MSRP
                if vendor_price_data['msrp']:
                    this.execute("UPDATE Product SET msrp = %s WHERE product_id = %s", (vendor_price_data['msrp'], product_id))
                    conn.commit()
                    logger.info(f"Record Updated for product_id ({product_id}).")
    except mysql.connector.Error as e:
        logger.warning(f"{product_id} >> MySQL ERROR productMsrpUpdate() >> {e}")
    finally:
        if conn.is_connected():
            conn.close()
            this.close()

# Updating MSRF in ProductVendor table
def productVendorMsrpUpdate(vendor_product_id, vendor_price_data):
    try:
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor(buffered=True)
            this.execute("SELECT msrp FROM ProductVendor WHERE vendor_product_id = %s", (vendor_product_id,))
            result = this.fetchone()
            if result:
                # Update MSRP
                if vendor_price_data['msrp']:
                    this.execute("UPDATE ProductVendor SET msrp = %s WHERE vendor_product_id = %s", (vendor_price_data['msrp'], vendor_product_id))
                    conn.commit()
                    logger.info(f"Record Updated for vendor_product_id ({vendor_product_id}).")
    except mysql.connector.Error as e:
        logger.warning(f"{vendor_product_id} >> MySQL ERROR productVendorMsrpUpdate() >> {e}")
    finally:
        if conn.is_connected():
            conn.close()
            this.close()
            
def coreDB(vendor_product_data, vendor_price_data, vendor_id):
    try:
        product_id, vendor_product_id = insertIntoMsp(vendor_product_data, vendor_id)
        print("--------------------------------------------------------")
        print(product_id, vendor_product_id)
        print("--------------------------------------------------------")
        if vendor_price_data['vendorprice_price'] is None:
            logger.debug(f"vendorprice_price not found!!")
            return
        elif isinstance(vendor_price_data['vendorprice_price'], str):
            price_lower = vendor_price_data['vendorprice_price'].lower()
            if 'best price' in price_lower or 'price unavailable' in price_lower or 'call for best price' in price_lower or 'obsolete' in price_lower:
                logger.debug(f"vendorprice_price not found!! - Price requires contact: {vendor_price_data['vendorprice_price']}")
                return
            else:
                insertall(product_id, vendor_product_id, vendor_price_data, vendor_id)
                evalRanking(vendor_id, product_id)
    except Exception as e:
        logger.error(f"Error in core function: {e}")
        # print(f"Error in core function: {e}")