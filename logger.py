from loguru import logger
import sys

class MyLogger:
    def __init__(self):
        logger.remove()  # Remove default loguru handlers
        
        # Define custom log format
        log_format = (
            "<green>{time:YYYY-MM-DD}</green> | "
            "<cyan>{time:HH:mm:ss}</cyan> | "
            "<level>{level: <8}</level> | "
            "<magenta>{module}</magenta> | "
            "{message}"
        )
        
        # Add a console handler
        logger.add(sys.stdout, format=log_format, level="DEBUG")

    def get_logger(self):
        return logger