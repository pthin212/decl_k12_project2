import logging
from datetime import datetime

def setup_logger():
    logging.basicConfig(
        filename=f'xml_processing_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def log_transaction(logger, url, success=True, error=None):
    if success:
        logger.info(f"Successfully processed URL: {url}")
    else:
        logger.error(f"Failed to process URL: {url}. Error: {error}")