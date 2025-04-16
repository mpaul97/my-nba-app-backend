import logging
from datetime import datetime

def setup_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            # logging.FileHandler(datetime.now().strftime('log_%y-%m-%d.log')),
            logging.FileHandler('log.log', mode='w'),
            logging.StreamHandler()
        ],
    )
    return