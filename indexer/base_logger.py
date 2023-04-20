# base_logger.py

import logging

logger = logging
logger.basicConfig(
    filename='app2.log',
    format='%(name)s - %(levelname)s - %(message)s'
)

# logger.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
