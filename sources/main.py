from db_config import get_db_connection
from logger import setup_logger, log_transaction
from xml_processor import XMLProcessor


def main():
    sitemap_urls = [
        "https://www.glamira.com/media/sitemap/glus/product_image_provider-41-1.xml",
        "https://www.glamira.com/media/sitemap/glus/product_image_provider-41-2.xml",
        "https://www.glamira.com/media/sitemap/glus/product_image_provider-41-3.xml",
        "https://www.glamira.com/media/sitemap/glus/product_image_provider-41-4.xml",
        "https://www.glamira.com/media/sitemap/glus/product_image_provider-41-5.xml",
        "https://www.glamira.com/media/sitemap/glus/product_image_provider-41-6.xml",
        "https://www.glamira.com/media/sitemap/glus/product_image_provider-41-7.xml",
        "https://www.glamira.com/media/sitemap/glus/product_image_provider-41-8.xml",
        "https://www.glamira.com/media/sitemap/glus/product_image_provider-41-9.xml",
    ]

    logger = setup_logger()

    try:
        db_connection = get_db_connection()
        processor = XMLProcessor(db_connection, logger)

        for url in sitemap_urls:
            try:
                processor.process_xml_url(url)
                log_transaction(logger, url)
                logger.info(f"Total successful transactions so far: {processor.get_transaction_count()}")
                print(f"Total successful transactions so far: {processor.get_transaction_count()}")
            except Exception as e:
                log_transaction(logger, url, success=False, error=str(e))

    except Exception as e:
        logger.error(f"Application error: {e}")
    finally:
        if 'db_connection' in locals():
            db_connection.close()


if __name__ == "__main__":
    main()