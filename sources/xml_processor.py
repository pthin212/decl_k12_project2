import requests
from bs4 import BeautifulSoup
from mysql.connector import Error

class XMLProcessor:
    def __init__(self, db_connection, logger):
        self.db_connection = db_connection
        self.logger = logger
        self.successful_transactions = 0

    def process_image_data(self, cursor, url_tag, prod_id):
        pagemap = url_tag.find('PageMap')
        if pagemap:
            thumbnail_src = pagemap.find('Attribute', {'name': 'src'})['value']
            cursor.execute(
                "INSERT INTO images (src, isthumbnail, prod_id) VALUES (%s, %s, %s)",
                (thumbnail_src, True, prod_id)
            )

        for image in url_tag.find_all('image:loc'):
            image_src = image.text.strip()
            if pagemap and image_src == thumbnail_src:
                continue
            cursor.execute(
                "INSERT INTO images (src, isthumbnail, prod_id) VALUES (%s, %s, %s)",
                (image_src, False, prod_id)
            )

    def process_single_url(self, cursor, url_tag):
        loc = url_tag.find('loc').text.strip()
        lastmod = url_tag.find('lastmod').text.strip()
        caption = url_tag.find('PageMap').find('Attribute', {'name': 'name'})['value'] if url_tag.find(
            'PageMap') else ''

        cursor.execute(
            "INSERT INTO products (prod_id, loc, lastmod, caption) VALUES (NULL, %s, %s, %s)",
            (loc, lastmod, caption)
        )
        return cursor.lastrowid

    def process_xml_url(self, xml_url):
        cursor = self.db_connection.cursor()
        try:
            response = requests.get(xml_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'xml')

            cursor.execute("START TRANSACTION")

            for url_tag in soup.find_all('url'):
                try:
                    prod_id = self.process_single_url(cursor, url_tag)
                    self.process_image_data(cursor, url_tag, prod_id)
                except Error as e:
                    self.logger.error(f"Error processing URL in sitemap: {e}")
                    raise

            cursor.execute("COMMIT")
            self.successful_transactions += 1
            self.logger.info(f"Sitemap transaction completed successfully: {xml_url}")

        except (Error, requests.exceptions.RequestException) as e:
            if 'cursor' in locals():
                cursor.execute("ROLLBACK")
            self.logger.error(f"Failed to process sitemap {xml_url}: {e}")
            raise
        finally:
            cursor.close()

    def get_transaction_count(self):
        return self.successful_transactions