import logging
import datetime
import os
import sys


class StreamFormatter(logging.Formatter):
    # Background Colors
    BLACKB   = "\x1b[40;20m"
    REDB     = "\x1b[41;20m"
    GREENB   = "\x1b[42;20m"
    YELLOWB  = "\x1b[43;20m"
    BLUEB    = "\x1b[44;20m"
    PURPLEB  = "\x1b[45;20m"
    CYANB    = "\x1b[46;20m"
    WHITEB   = "\x1b[47;20m"
    # Font Colors
    red      = "\x1b[31;20m"
    green    = "\x1b[32;20m"
    yellow   = "\x1b[33;20m"
    blue     = "\x1b[34;20m"
    purple   = "\x1b[35;20m"
    cyan     = "\x1b[36;20m"
    white    = "\x1b[37;20m"
    grey     = "\x1b[38;20m"
    bold_red = "\x1b[31;1m"
    reset    = "\x1b[0m"
    FORMATS = {
        logging.DEBUG: cyan
        + "%(asctime)s - %(name)s - {levelname:} - %(message)s".format(
            levelname="ОТЛАДКА"
        )
        + reset,
        logging.INFO: green
        + "%(asctime)s - %(name)s - {levelname:} - %(message)s".format(
            levelname="ШТАТНАЯ РАБОТА"
        )
        + reset,
        logging.WARNING: yellow
        + "%(asctime)s - %(name)s - {levelname:} - %(message)s".format(
            levelname="ПРЕДУПРЕЖДЕНИЕ"
        )
        + reset,
        logging.ERROR: red
        + "%(asctime)s - %(name)s - {levelname:} - %(message)s".format(
            levelname="ОШИБКА"
        )
        + reset,
        logging.CRITICAL: bold_red
        + "%(asctime)s - %(name)s - {levelname:} - %(message)s".format(
            levelname="КРИТИЧЕСКАЯ ОШИБКА"
        )
        + reset,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


class FileFormatter(logging.Formatter):
    blue = "\x1b[34;20m"
    green = "\x1b[32;20m"
    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"

    FORMATS = {
        logging.DEBUG: "%(asctime)s - %(name)s - {levelname:} - %(message)s".format(
            levelname="ОТЛАДКА"
        ),
        logging.INFO: "%(asctime)s - %(name)s - {levelname:} - %(message)s".format(
            levelname="ШТАТНАЯ РАБОТА"
        ),
        logging.WARNING: "%(asctime)s - %(name)s - {levelname:} - %(message)s".format(
            levelname="ПРЕДУПРЕЖДЕНИЕ"
        ),
        logging.ERROR: "%(asctime)s - %(name)s - {levelname:} - %(message)s".format(
            levelname="ОШИБКА"
        ),
        logging.CRITICAL: "%(asctime)s - %(name)s - {levelname:} - %(message)s".format(
            levelname="КРИТИЧЕСКАЯ ОШИБКА"
        ),
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def log(name, level, log_folder_path: str):
    now = datetime.datetime.now().strftime("%Y_%m_%d")
    if not os.path.isdir(log_folder_path):
        os.makedirs(log_folder_path)

    logger = logging.getLogger(name)

    file_handler = logging.FileHandler(f"{log_folder_path}/{now}.log", encoding="utf-8")
    file_handler.setFormatter(FileFormatter())
    
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(StreamFormatter())

    crit_handler = logging.FileHandler(f"{log_folder_path}/errors.log", encoding="utf-8")
    crit_handler.setLevel(logging.ERROR)
    crit_handler.setFormatter(FileFormatter())

    logging.basicConfig(
        level=level,
        handlers=[crit_handler, file_handler, stream_handler],
        force=True
    )
    return logger
