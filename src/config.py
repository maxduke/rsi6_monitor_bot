# -*- coding: utf-8 -*-

import logging
import os
import sys

# --- 日志配置 ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
)
for _logger_name in ("httpx", "telegram.ext", "apscheduler"):
    logging.getLogger(_logger_name).setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# --- 机器人配置 (从环境变量读取) ---
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
ADMIN_USER_ID_STR = os.getenv('ADMIN_USER_ID')
ADMIN_USER_ID = int(ADMIN_USER_ID_STR) if ADMIN_USER_ID_STR and ADMIN_USER_ID_STR.isdigit() else None
CHECK_INTERVAL_SECONDS = int(os.getenv('CHECK_INTERVAL_SECONDS', '60'))
DB_FILE = os.getenv('DB_FILE', 'rules.db')

# --- 监控参数配置 ---
RSI_PERIOD = int(os.getenv('RSI_PERIOD', '6'))
USE_ADJUST = os.getenv('USE_ADJUST', 'true').lower() == 'true'
HIST_FETCH_DAYS = int(os.getenv('HIST_FETCH_DAYS', '200'))
MAX_NOTIFICATIONS_PER_TRIGGER = int(os.getenv('MAX_NOTIFICATIONS_PER_TRIGGER', '1'))

# --- 高级配置 ---
RANDOM_DELAY_MAX_SECONDS = float(os.getenv('RANDOM_DELAY_MAX_SECONDS', '0'))
FETCH_FAILURE_THRESHOLD = int(os.getenv('FETCH_FAILURE_THRESHOLD', '5'))
REQUEST_INTERVAL_SECONDS = float(os.getenv('REQUEST_INTERVAL_SECONDS', '1.0'))
ENABLE_DAILY_BRIEFING = os.getenv('ENABLE_DAILY_BRIEFING', 'false').lower() == 'true'
BRIEFING_TIMES_STR = os.getenv('DAILY_BRIEFING_TIMES', '15:30')
FETCH_RETRY_ATTEMPTS = int(os.getenv('FETCH_RETRY_ATTEMPTS', '3'))
FETCH_RETRY_DELAY_SECONDS = int(os.getenv('FETCH_RETRY_DELAY_SECONDS', '5'))
EM_BLOCK_CHECK_INTERVAL_SECONDS = int(os.getenv('EM_BLOCK_CHECK_INTERVAL_SECONDS', '300'))
EM_BLOCK_CHECK_URL = "https://i.eastmoney.com/websitecaptcha/api/checkuser?callback=wsc_checkuser"

# --- 应用内常量 ---
KEY_HIST_CACHE = 'hist_data_cache'
KEY_NAME_CACHE = 'name_cache'
KEY_CACHE_DATE = 'cache_date'
KEY_FAILURE_COUNT = 'fetch_failure_count'
KEY_FAILURE_SENT = 'failure_notification_sent'
STOCK_PREFIXES = ('0', '3', '6', '4', '8')
ETF_PREFIXES = ('5', '1')
NAME_CACHE_MAX_SIZE = 500


def validate_config():
    """验证关键配置值的合法性，不合法则退出。"""
    errors = []

    if not TELEGRAM_TOKEN:
        errors.append("TELEGRAM_TOKEN 未设置")
    if not ADMIN_USER_ID:
        errors.append("ADMIN_USER_ID 未设置或不是合法的正整数")

    if CHECK_INTERVAL_SECONDS <= 0:
        errors.append(f"CHECK_INTERVAL_SECONDS 必须 > 0，当前值: {CHECK_INTERVAL_SECONDS}")
    if RSI_PERIOD <= 0:
        errors.append(f"RSI_PERIOD 必须 > 0，当前值: {RSI_PERIOD}")
    if HIST_FETCH_DAYS <= RSI_PERIOD:
        errors.append(f"HIST_FETCH_DAYS({HIST_FETCH_DAYS}) 必须 > RSI_PERIOD({RSI_PERIOD})")
    if REQUEST_INTERVAL_SECONDS < 0:
        errors.append(f"REQUEST_INTERVAL_SECONDS 必须 >= 0，当前值: {REQUEST_INTERVAL_SECONDS}")
    if FETCH_RETRY_ATTEMPTS < 1:
        errors.append(f"FETCH_RETRY_ATTEMPTS 必须 >= 1，当前值: {FETCH_RETRY_ATTEMPTS}")
    if MAX_NOTIFICATIONS_PER_TRIGGER < 1:
        errors.append(f"MAX_NOTIFICATIONS_PER_TRIGGER 必须 >= 1，当前值: {MAX_NOTIFICATIONS_PER_TRIGGER}")

    if errors:
        for err in errors:
            logger.critical(f"配置错误: {err}")
        sys.exit(1)


def log_config():
    """在启动时打印当前配置。"""
    logger.info("--- 机器人配置 ---")
    logger.info(f"RSI 周期: {RSI_PERIOD}")
    logger.info(f"历史数据天数: {HIST_FETCH_DAYS}")
    logger.info(f"是否复权: {USE_ADJUST}")
    logger.info(f"最大通知次数/次: {MAX_NOTIFICATIONS_PER_TRIGGER}")
    logger.info(f"检查间隔: {CHECK_INTERVAL_SECONDS}秒")
    logger.info(f"数据库文件: {DB_FILE}")
    logger.info(f"最大随机延迟: {RANDOM_DELAY_MAX_SECONDS}秒")
    logger.info(f"失败通知阈值: {FETCH_FAILURE_THRESHOLD}次")
    logger.info(f"请求间隔: {REQUEST_INTERVAL_SECONDS}秒")
    logger.info(f"每日简报主开关: {'开启' if ENABLE_DAILY_BRIEFING else '关闭'}")
    if ENABLE_DAILY_BRIEFING:
        logger.info(f"每日简报发送时间: {BRIEFING_TIMES_STR} (上海时间)")
    logger.info("--------------------")
