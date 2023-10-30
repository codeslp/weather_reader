import logging


def configure_logger():
    # Creating a formatter for the logs
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Creating a handler to write log messages to a file
    file_handler = logging.FileHandler("logs.log")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    # Creating a handler to write log messages to the console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # Getting the root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Adding handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

