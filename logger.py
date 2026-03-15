"""
logger.py — 双轨日志系统
- 终端：关键进度日志（INFO 级别，带颜色）
- 文件：全量详细日志（DEBUG 级别）
"""

import logging
import os
import sys
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler


class ColorFormatter(logging.Formatter):
    """终端彩色输出格式化"""

    COLORS = {
        "DEBUG": "\033[37m",     # 白色
        "INFO": "\033[32m",      # 绿色
        "WARNING": "\033[33m",   # 黄色
        "ERROR": "\033[31m",     # 红色
        "CRITICAL": "\033[35m",  # 紫色
    }
    RESET = "\033[0m"

    def format(self, record):
        color = self.COLORS.get(record.levelname, self.RESET)
        msg = super().format(record)
        return f"{color}{msg}{self.RESET}"


def setup_logger(log_path: str, max_days: int = 7, name: str = "seatalk") -> logging.Logger:
    """初始化双轨日志器（文件按天轮转，最多保留 max_days 天）"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    # 文件 Handler：按天轮转，DEBUG 级别
    file_handler = TimedRotatingFileHandler(
        log_path, when="midnight", interval=1,
        backupCount=max_days, encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter(
            fmt="[%(asctime)s][%(levelname)s][%(module)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )

    # 终端 Handler：INFO 及以上
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(
        ColorFormatter(fmt="[%(asctime)s][%(levelname)s] %(message)s", datefmt="%H:%M:%S")
    )

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def get_logger() -> logging.Logger:
    return logging.getLogger("seatalk")
