import logging
import os

def start_logging(log_filename="etlslogs.log"):
    """
    Initialize a logger that writes to logs/<log_filename>.
    Default file: logs/etlslogs.log
    """
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    logger_name = os.path.splitext(log_filename)[0]  # e.g. "etlslogs"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    log_file = os.path.join(log_dir, log_filename)
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)

    if not logger.handlers:   # avoid duplicate handlers
        logger.addHandler(file_handler)

    return logger
