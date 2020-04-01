u"""write log to file."""
import logging
import os
import sys
import settings

ROOT_PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))


def webui_logger(log_file: str, formatter=None, add_sys=True, level=logging.INFO):
    u"""Return logger."""

    # '%Y-%m-%d %X',
    formatter = formatter if formatter else \
        logging.Formatter('[%(asctime)s] %(levelname)s - %(filename)s:%(lineno)d - %(message)s')

    logger = logging.Logger("webui")

    if add_sys:
        stdout_handler = logging.StreamHandler(sys.stdout)
        log_filter = LogFilter(logging.NOTSET)
        stdout_handler.addFilter(log_filter)
        stdout_handler.setLevel(logging.DEBUG)
        logger.addHandler(stdout_handler)

        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setLevel(logging.DEBUG)
        logger.addHandler(stderr_handler)

    log_path = log_file if log_file.startswith("/") else os.path.join(settings.LOG_DEFAULT_PATH, f"{log_file}.log")
    file_handler = logging.FileHandler(log_path, encoding='utf-8', delay=1)
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def get_logger(filename, logger_name=None, formatter=None, level=None, add_sys=True, show_screen=False):
    u"""Return logger."""
    if not logger_name:
        logger_name = filename

    if not formatter:
        formatter = logging.Formatter(
            '[%(asctime)s] %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
        # '%Y-%m-%d %X'

    if not level:
        level = logging.INFO

    logger = logging.getLogger()

    if add_sys:
        stdout_hdlr = logging.StreamHandler(sys.stdout)
        log_filter = LogFilter(logging.WARNING)
        stdout_hdlr.addFilter(log_filter)
        logger.addHandler(stdout_hdlr)

        stderr_hdlr = logging.StreamHandler(sys.stderr)
        stderr_hdlr.setLevel(logging.WARNING)
        logger.addHandler(stderr_hdlr)

    file_handler = logging.FileHandler('/tmp/%s.log' % filename)
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    return logger


class LogFilter(logging.Filter):
    '''Filters (lets through) all messages with level < LEVEL'''
    def __init__(self, level):
        super(LogFilter, self).__init__()
        self.level = level

    def filter(self, record):
        return record.levelno < self.level
