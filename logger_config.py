import logging
import os
from logging.handlers import RotatingFileHandler
from config import SystemConfig

def setup_logging():
    logger = logging.getLogger('stock_analysis')
    logger.setLevel(getattr(logging, SystemConfig.LOG_LEVEL))
    
    if logger.handlers:
        return logger
    
    log_dir = os.path.dirname(SystemConfig.LOG_FILE)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    file_handler = RotatingFileHandler(
        SystemConfig.LOG_FILE,
        maxBytes=SystemConfig.LOG_MAX_BYTES,
        backupCount=SystemConfig.LOG_BACKUP_COUNT,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

logger = setup_logging()
