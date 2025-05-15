import logging
from datetime import datetime
import os

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(datetime.now().strftime(f'runtime_log.log'), mode='w'),
            logging.StreamHandler()
        ],
    )
    return