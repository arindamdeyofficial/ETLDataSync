import logging

from pydantic import BaseModel

class LogHelper:
    def __init__(self, log_file: str ="app.log", level: int =logging.INFO):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(level)

        # Create a file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(file_handler)

        # Create a console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)  # Adjust console log level as needed
        console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)

    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message, exc_info=False):
        self.logger.error(message, exc_info=exc_info)

    def critical(self, message):
        self.logger.critical(message)

# Example usage:
#logger = LogHelper("my_app.log", logging.DEBUG)
#logger.info("This is an info message")
#logger.error("An error occurred", exc_info=True)
