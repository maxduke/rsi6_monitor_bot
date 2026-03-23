# -*- coding: utf-8 -*-

import logging
import sqlite3
import pandas as pd
import akshare as ak
import html
import math
from datetime import datetime, time, timedelta
import pytz
import asyncio
from functools import wraps
from typing import Union, Dict, List, Tuple
import os
import random
import requests
import pandas_market_calendars as mcal
from collections import defaultdict

from telegram import Update, BotCommand
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.constants import ParseMode
from telegram.error import Forbidden, RetryAfter

# --- 机器人配置 (从环境变量读取) ---
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
ADMIN_USER_ID_STR = os.getenv('ADMIN_USER_ID')
ADMIN_USER_ID = int(ADMIN_USER_ID_STR) if ADMIN_USER_ID_STR and ADMIN_USER_ID_STR.isdigit() else None
CHECK_INTERVAL_SECONDS = int(os.getenv('CHECK_INTERVAL_SECONDS', '60'))
DB_FILE = os.getenv('DB_FILE', 'rules.db')

# --- 监控参数配置 ---
RSI_PERIOD = int(os.getenv('RSI_PERIOD', '6'))
# [配置] 是否复权: 'true'(默认,前复权), 'false'(不复权)
USE_ADJUST = os.getenv('USE_ADJUST', 'true').lower() == 'true'
# 历史数据获取天数
HIST_FETCH_DAYS = int(os.getenv('HIST_FETCH_DAYS', '200'))
MAX_NOTIFICATIONS_PER_TRIGGER = int(os.getenv('MAX_NOTIFICATIONS_PER_TRIGGER', '1'))

# --- 高级配置 ---
RANDOM_DELAY_MAX_SECONDS = float(os.getenv('RANDOM_DELAY_MAX_SECONDS', '0'))
FETCH_FAILURE_THRESHOLD = int(os.getenv('FETCH_FAILURE_THRESHOLD', '5'))
# Sina接口建议间隔稍微大一点，避免高频封禁
REQUEST_INTERVAL_SECONDS = float(os.getenv('REQUEST_INTERVAL_SECONDS', '1.0'))
ENABLE_DAILY_BRIEFING = os.getenv('ENABLE_DAILY_BRIEFING', 'false').lower() == 'true'
BRIEFING_TIMES_STR = os.getenv('DAILY_BRIEFING_TIMES', '15:30')
FETCH_RETRY_ATTEMPTS = int(os.getenv('FETCH_RETRY_ATTEMPTS', '3'))
FETCH_RETRY_DELAY_SECONDS = int(os.getenv('FETCH_RETRY_DELAY_SECONDS', '5'))
EM_BLOCK_CHECK_INTERVAL_SECONDS = int(os.getenv('EM_BLOCK_CHECK_INTERVAL_SECONDS', '300'))
EM_BLOCK_CHECK_URL = "https://i.eastmoney.com/websitecaptcha/api/checkuser?callback=wsc_checkuser"

# --- 日志配置 ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
for logger_name in ["httpx", "telegram.ext", "apscheduler"]:
    logging.getLogger(logger_name).setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# --- 应用内常量 ---
KEY_HIST_CACHE = 'hist_data_cache'
KEY_NAME_CACHE = 'name_cache'
KEY_CACHE_DATE = 'cache_date'
KEY_FAILURE_COUNT = 'fetch_failure_count'
KEY_FAILURE_SENT = 'failure_notification_sent'
STOCK_PREFIXES = ('0', '3', '6', '4', '8')
ETF_PREFIXES = ('5', '1')

CHINA_CALENDAR = mcal.get_calendar('XSHG')
EM_BLOCK_CACHE: Dict[str, Union[bool, datetime, None]] = {"blocked": None, "checked_at": None}
TRADE_DAY_CACHE: Dict[str, Union[set, datetime, None]] = {"days": None, "loaded_at": None}


# --- 数据库模块 ---
def db_init():
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, asset_code TEXT NOT NULL, 
            asset_name TEXT, rsi_min REAL NOT NULL, rsi_max REAL NOT NULL, is_active INTEGER DEFAULT 1,
            last_notified_rsi REAL DEFAULT 0, notification_count INTEGER NOT NULL DEFAULT 0,
            UNIQUE(user_id, asset_code, rsi_min, rsi_max)
        )''')
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS whitelist (
            user_id INTEGER PRIMARY KEY,
            daily_briefing_enabled INTEGER NOT NULL DEFAULT 0
        )''')
        if ADMIN_USER_ID:
            cursor.execute('INSERT OR IGNORE INTO whitelist (user_id) VALUES (?)', (ADMIN_USER_ID,))
        conn.commit()
        logger.info("数据库初始化完成。")

def db_execute(query, params=(), fetchone=False, fetchall=False, swallow_errors=True):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            if fetchone: return cursor.fetchone()
            if fetchall: return cursor.fetchall()
            return None
    except sqlite3.Error as e:
        logger.error(f"数据库操作失败: {e}")
        if not swallow_errors:
            raise
        return None

# --- 白名单与装饰器 ---
def is_whitelisted(user_id: int) -> bool: return db_execute("SELECT 1 FROM whitelist WHERE user_id = ?", (user_id,), fetchone=True) is not None
def add_to_whitelist(user_id: int): db_execute("INSERT OR IGNORE INTO whitelist (user_id) VALUES (?)", (user_id,))
def remove_from_whitelist(user_id: int): db_execute("DELETE FROM whitelist WHERE user_id = ?", (user_id,))
def get_whitelist(): return db_execute("SELECT * FROM whitelist", fetchall=True)
def whitelisted_only(func):
    @wraps(func)
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if not is_whitelisted(update.effective_user.id):
            await update.message.reply_text("抱歉，您没有权限使用此机器人。")
            return
        return await func(update, context, *args, **kwargs)
    return wrapped
def admin_only(func):
    @wraps(func)
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if update.effective_user.id != ADMIN_USER_ID:
            await update.message.reply_text("抱歉，此命令仅限管理员使用。")
            return
        return await func(update, context, *args, **kwargs)
    return wrapped

# --- 核心：数据获取与计算模块 (优化版) ---

async def _run_with_retries(operation, description: str):
    for attempt in range(1, FETCH_RETRY_ATTEMPTS + 1):
        result = await operation()
        if result is not None:
            return result
        if attempt < FETCH_RETRY_ATTEMPTS:
            logger.warning(
                f"{description} 失败，{FETCH_RETRY_DELAY_SECONDS}秒后重试 "
                f"({attempt}/{FETCH_RETRY_ATTEMPTS})。"
            )
            await asyncio.sleep(FETCH_RETRY_DELAY_SECONDS)
    return None

def ensure_daily_history_cache(context: ContextTypes.DEFAULT_TYPE, now: datetime) -> Dict[str, pd.DataFrame]:
    bot_data = context.bot_data
    today_str = now.strftime('%Y-%m-%d')
    if bot_data.get(KEY_CACHE_DATE) != today_str:
        logger.info(f"日期变更或首次运行，清空并重建 {today_str} 的历史数据缓存。")
        bot_data[KEY_HIST_CACHE] = {}
        bot_data[KEY_CACHE_DATE] = today_str
    return bot_data.get(KEY_HIST_CACHE, {})

def get_sina_symbol(code: str) -> str:
    """转换代码为新浪接口格式"""
    if code.startswith(('6', '5', '9')): return f"sh{code}"
    elif code.startswith(('0', '3', '1', '2')): return f"sz{code}"
    elif code.startswith(('4', '8')): return f"bj{code}"
    return code

async def _is_em_blocked() -> bool:
    now = datetime.now()
    last_checked = EM_BLOCK_CACHE.get("checked_at")
    if last_checked and (now - last_checked).total_seconds() < EM_BLOCK_CHECK_INTERVAL_SECONDS:
        blocked = EM_BLOCK_CACHE.get("blocked")
        return bool(blocked)

    def fetch_status() -> bool:
        try:
            response = requests.get(EM_BLOCK_CHECK_URL, timeout=5)
            text = response.text or ""
            return '"block":true' in text or '"block": true' in text
        except Exception as e:
            logger.warning(f"检测东方财富封禁状态失败: {e}")
            return False

    blocked = await asyncio.to_thread(fetch_status)
    EM_BLOCK_CACHE["blocked"] = blocked
    EM_BLOCK_CACHE["checked_at"] = now
    if blocked:
        logger.warning("检测到东方财富接口被封禁，后续将直接使用新浪接口。")
    return blocked

async def get_asset_name_with_cache(asset_code: str, context: ContextTypes.DEFAULT_TYPE) -> str:
    name_cache = context.bot_data.get(KEY_NAME_CACHE, {})
    if asset_code in name_cache:
        logger.debug(f"从缓存命中资产名称: {asset_code} -> {name_cache[asset_code]}")
        return name_cache[asset_code]
    
    logger.info(f"缓存未命中，尝试获取资产名称: {asset_code}")
    await asyncio.sleep(REQUEST_INTERVAL_SECONDS)
    name = None

    async def fetch_name():
        if asset_code.startswith(STOCK_PREFIXES):
            info_df = await asyncio.to_thread(ak.stock_individual_info_em, symbol=asset_code)
            if info_df is not None and not info_df.empty and 'value' in info_df.columns:
                match = info_df.loc[info_df['item'] == '股票简称', 'value']
                if not match.empty:
                    return match.iloc[0]
        if asset_code.startswith(ETF_PREFIXES):
            name_df = await asyncio.to_thread(ak.fund_name_em)
            if name_df is not None and not name_df.empty:
                match = name_df.loc[name_df['基金代码'] == asset_code, '基金简称']
                if not match.empty:
                    return match.iloc[0]
        return None

    name = await _run_with_retries(fetch_name, f"获取资产名称({asset_code})")
    if not name:
        name = f"Asset_{asset_code}"

    name_cache[asset_code] = name
    logger.debug(f"已将新资产名称存入缓存: {asset_code} -> {name}")
    return name

async def get_history_data(asset_code: str, days: int) -> Union[pd.DataFrame, None]:
    """获取单个资产的历史日线数据，并在需要时计算复权因子。"""
    try:
        today = datetime.now()
        start_date = (today - timedelta(days=days)).strftime('%Y%m%d')
        end_date = today.strftime('%Y%m%d')
        adjust = "qfq" if USE_ADJUST else ""

        def _normalize_hist_df(hist_df: pd.DataFrame) -> pd.DataFrame:
            if hist_df is None or hist_df.empty:
                return hist_df
            rename_map = {
                "date": "日期",
                "open": "开盘",
                "high": "最高",
                "low": "最低",
                "close": "收盘",
                "volume": "成交量",
                "amount": "成交额",
            }
            hist_df = hist_df.rename(columns={k: v for k, v in rename_map.items() if k in hist_df.columns})
            if "日期" in hist_df.columns:
                hist_df["日期"] = pd.to_datetime(hist_df["日期"])
            return hist_df

        async def fetch_hist_em():
            try:
                if asset_code.startswith(STOCK_PREFIXES):
                    return await asyncio.to_thread(
                        ak.stock_zh_a_hist,
                        symbol=asset_code,
                        period="daily",
                        start_date=start_date,
                        end_date=end_date,
                        adjust=adjust,
                    )
                if asset_code.startswith(ETF_PREFIXES):
                    return await asyncio.to_thread(
                        ak.fund_etf_hist_em,
                        symbol=asset_code,
                        period="daily",
                        start_date=start_date,
                        end_date=end_date,
                        adjust=adjust,
                    )
            except Exception as e:
                logger.warning(f"东方财富接口获取历史数据失败({asset_code}): {e}")
            return None

        async def fetch_hist_sina():
            try:
                sina_symbol = get_sina_symbol(asset_code)
                if asset_code.startswith(STOCK_PREFIXES):
                    return await asyncio.to_thread(
                        ak.stock_zh_a_daily,
                        symbol=sina_symbol,
                        start_date=start_date,
                        end_date=end_date,
                        adjust=adjust,
                    )
                if asset_code.startswith(ETF_PREFIXES):
                    return await asyncio.to_thread(
                        ak.fund_etf_hist_sina,
                        symbol=sina_symbol,
                    )
            except Exception as e:
                logger.warning(f"新浪接口获取历史数据失败({asset_code}): {e}")
            return None

        use_em = not await _is_em_blocked()
        df = None
        source = "sina"
        if use_em:
            df = await _run_with_retries(fetch_hist_em, f"获取历史数据({asset_code})")
            source = "em"
        if df is None or (df is not None and df.empty):
            logger.info(f"尝试使用新浪接口获取历史数据({asset_code})。")
            df = await _run_with_retries(fetch_hist_sina, f"获取历史数据-新浪({asset_code})")
            source = "sina"
        if df is None:
            return None
        df = _normalize_hist_df(df)
        if df is not None and not df.empty and "日期" in df.columns:
            df.set_index("日期", inplace=True)
            if USE_ADJUST:
                if source == "sina" and asset_code.startswith(ETF_PREFIXES):
                    logger.info(f"ETF({asset_code}) 使用新浪历史数据，仅能提供不复权数据。")
                    df.attrs["adjust_factor"] = 1.0
                else:
                    df.attrs['adjust_factor'] = await _get_adjust_factor(asset_code, df)
        return df
    except Exception as e:
        logger.error(f"获取 {asset_code} 历史数据失败: {e}")
        return None

async def _get_adjust_factor(asset_code: str, hist_df: pd.DataFrame) -> float:
    """
    计算复权因子（复权收盘 / 未复权收盘），用于将实时价格转换到复权尺度。
    若无法计算，则返回 1.0。
    """
    try:
        base_date = hist_df.index[-1]
        today_date = datetime.now(pytz.timezone('Asia/Shanghai')).date()
        if base_date.date() >= today_date and len(hist_df.index) > 1:
            base_date = hist_df.index[-2]
        raw_start = (base_date - timedelta(days=30)).strftime('%Y%m%d')
        raw_end = (base_date + timedelta(days=1)).strftime('%Y%m%d')

        def _normalize_hist_df(hist_df: pd.DataFrame) -> pd.DataFrame:
            if hist_df is None or hist_df.empty:
                return hist_df
            rename_map = {
                "date": "日期",
                "open": "开盘",
                "high": "最高",
                "low": "最低",
                "close": "收盘",
                "volume": "成交量",
                "amount": "成交额",
            }
            hist_df = hist_df.rename(columns={k: v for k, v in rename_map.items() if k in hist_df.columns})
            if "日期" in hist_df.columns:
                hist_df["日期"] = pd.to_datetime(hist_df["日期"])
            return hist_df

        async def fetch_raw_hist_em():
            try:
                if asset_code.startswith(STOCK_PREFIXES):
                    return await asyncio.to_thread(
                        ak.stock_zh_a_hist,
                        symbol=asset_code,
                        period="daily",
                        start_date=raw_start,
                        end_date=raw_end,
                        adjust="",
                    )
                if asset_code.startswith(ETF_PREFIXES):
                    return await asyncio.to_thread(
                        ak.fund_etf_hist_em,
                        symbol=asset_code,
                        period="daily",
                        start_date=raw_start,
                        end_date=raw_end,
                        adjust="",
                    )
            except Exception as e:
                logger.warning(f"东方财富接口获取未复权数据失败({asset_code}): {e}")
            return None

        async def fetch_raw_hist_sina():
            try:
                if asset_code.startswith(STOCK_PREFIXES):
                    sina_symbol = get_sina_symbol(asset_code)
                    return await asyncio.to_thread(
                        ak.stock_zh_a_daily,
                        symbol=sina_symbol,
                        start_date=raw_start,
                        end_date=raw_end,
                        adjust="",
                    )
                if asset_code.startswith(ETF_PREFIXES):
                    sina_symbol = get_sina_symbol(asset_code)
                    return await asyncio.to_thread(
                        ak.fund_etf_hist_sina,
                        symbol=sina_symbol,
                    )
            except Exception as e:
                logger.warning(f"新浪接口获取未复权数据失败({asset_code}): {e}")
            return None

        use_em = not await _is_em_blocked()
        raw_df = None
        sina_attempted = False

        # ETF 在复权模式下，优先用新浪获取未复权价格序列，以减少东方财富请求次数。
        prefer_sina_for_etf_raw = USE_ADJUST and asset_code.startswith(ETF_PREFIXES)
        if prefer_sina_for_etf_raw:
            sina_attempted = True
            raw_df = await _run_with_retries(fetch_raw_hist_sina, f"获取未复权数据-新浪({asset_code})")

        if (raw_df is None or raw_df.empty) and use_em:
            raw_df = await _run_with_retries(fetch_raw_hist_em, f"获取未复权数据({asset_code})")

        if (raw_df is None or raw_df.empty) and not sina_attempted:
            logger.info(f"尝试使用新浪接口获取未复权数据({asset_code})。")
            raw_df = await _run_with_retries(fetch_raw_hist_sina, f"获取未复权数据-新浪({asset_code})")
        if raw_df is None or raw_df.empty:
            return 1.0
        raw_df = _normalize_hist_df(raw_df)
        if raw_df is None or raw_df.empty or "日期" not in raw_df.columns:
            return 1.0
        raw_df.set_index('日期', inplace=True)

        # 日期对齐：优先使用 base_date；若不存在则使用两侧都存在且不晚于 base_date 的最近交易日。
        base_ts = pd.Timestamp(base_date)
        raw_idx = pd.to_datetime(raw_df.index)
        adj_idx = pd.to_datetime(hist_df.index)
        common_dates = raw_idx.intersection(adj_idx)
        candidate_dates = common_dates[common_dates <= base_ts]
        if candidate_dates.empty:
            return 1.0
        aligned_date = candidate_dates.max()

        raw_close = raw_df.loc[aligned_date, '收盘']
        if raw_close is None or raw_close == 0:
            return 1.0
        adjusted_close = hist_df.loc[aligned_date, '收盘']
        return float(adjusted_close) / float(raw_close)
    except Exception as e:
        logger.warning(f"计算复权因子失败({asset_code}): {e}")
        return 1.0

def _adjust_spot_price(hist_df: pd.DataFrame, spot_price: float) -> float:
    """将实时价格调整到与历史复权数据一致的价格尺度。"""
    if not USE_ADJUST:
        return float(spot_price)
    adjust_factor = hist_df.attrs.get('adjust_factor')
    if not adjust_factor or adjust_factor == 0:
        return float(spot_price)
    return float(spot_price) * float(adjust_factor)

async def _fetch_single_realtime_price(code: str) -> Union[float, None]:
    """通过新浪分时接口获取最新价 (最稳健)"""
    sina_symbol = get_sina_symbol(code)
    async def fetch_price():
        try:
            df = await asyncio.to_thread(ak.stock_zh_a_minute, symbol=sina_symbol, period='1')
            if df is not None and not df.empty:
                return float(df.iloc[-1]['close'])
        except Exception as e:
            logger.warning(f"获取 {code} 实时价格失败: {e}")
        return None

    return await _run_with_retries(fetch_price, f"获取实时价格({code})")

async def _fetch_all_spot_data(context: ContextTypes.DEFAULT_TYPE, codes: List[str], price_key: str = '最新价') -> Tuple[Dict, bool]:
    """
    获取实时数据
    优化点：循环使用 _fetch_single_realtime_price，解决批量接口不稳定问题。
    """
    spot_dict = {}
    success_count = 0
    
    # 逐个获取，虽然慢一点但稳定
    for code in codes:
        await asyncio.sleep(REQUEST_INTERVAL_SECONDS) # 避免速率限制
        price = await _fetch_single_realtime_price(code)
        if price is not None:
            spot_dict[code] = price
            success_count += 1
            
    # 只要有一个成功就算成功，避免全盘报错
    if success_count == 0 and len(codes) > 0:
        logger.warning("本次未获取到任何有效价格。")
        context.bot_data[KEY_FAILURE_COUNT] = context.bot_data.get(KEY_FAILURE_COUNT, 0) + 1
        count = context.bot_data[KEY_FAILURE_COUNT]
        
        if count >= FETCH_FAILURE_THRESHOLD and not context.bot_data.get(KEY_FAILURE_SENT) and ADMIN_USER_ID:
            admin_message = (f"🚨 **机器人警报** 🚨\n\n连续获取数据失败已达 **{count}** 次。\n请检查新浪接口连通性。")
            try:
                await context.bot.send_message(chat_id=ADMIN_USER_ID, text=admin_message, parse_mode=ParseMode.MARKDOWN)
                logger.warning(f"已向管理员发送数据获取失败的警报通知。")
                context.bot_data[KEY_FAILURE_SENT] = True
            except Exception as e:
                logger.error(f"向管理员发送数据获取失败告警时出错: {e}")
        return {}, False
    
    # 成功获取，重置失败计数器
    if context.bot_data.get(KEY_FAILURE_COUNT, 0) > 0: logger.info("数据获取成功，重置失败计数器。")
    context.bot_data[KEY_FAILURE_COUNT] = 0
    context.bot_data[KEY_FAILURE_SENT] = False
    return spot_dict, True


def get_prices_for_rsi(hist_df: pd.DataFrame, spot_price: float) -> Union[pd.Series, None]:
    """根据历史和实时价格准备用于 RSI 计算的价格序列。"""
    if hist_df is None or hist_df.empty: return None
    if '收盘' not in hist_df.columns: return None
    close_prices = hist_df['收盘'].copy()
    last_date_in_hist = close_prices.index[-1].date()
    today_date = datetime.now(pytz.timezone('Asia/Shanghai')).date()
    adjusted_spot_price = _adjust_spot_price(hist_df, spot_price)
    # 关键逻辑：确保最后一行是当前价格，用于实时 RSI 计算。
    if last_date_in_hist < today_date:
        close_prices.loc[pd.Timestamp(today_date)] = adjusted_spot_price
    else:
        close_prices.iloc[-1] = adjusted_spot_price
    return close_prices

def calculate_rsi_exact(prices: pd.Series, period: int = 6) -> Union[float, None]:
    """
    完全复刻同花顺/东财算法的 RSI 计算函数。
    注意：prices 应当已处于目标价格尺度（复权或未复权）。
    使用 pandas 原生 ewm(alpha=1/N) 实现 Wilder 平滑。
    """
    try:
        if len(prices) < period + 1: return None
        
        # 1. 计算涨跌幅
        delta = prices.diff()
        
        # 2. 分离涨跌
        gain = delta.clip(lower=0)
        loss = -1 * delta.clip(upper=0)
        
        # 3. 应用 Wilder 平滑 (alpha = 1/N)
        # 同花顺口径：ewm 使用 adjust=True。
        avg_gain = gain.ewm(alpha=1/period, adjust=True).mean()
        avg_loss = loss.ewm(alpha=1/period, adjust=True).mean()
        
        # 4. 计算 RS 和 RSI
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        # 返回最后一个值
        return round(rsi.iloc[-1], 2)
    except Exception as e:
        logger.error(f"RSI计算出错: {e}")
        return None

def calculate_rsi(prices: pd.Series) -> Union[float, None]:
    # 直接调用手动的精确算法，废弃pandas-ta
    return calculate_rsi_exact(prices, period=RSI_PERIOD)

# --- 市场时间检查 ---
def _load_trade_days_from_ak() -> Union[set, None]:
    try:
        df = ak.tool_trade_date_hist_sina()
        if df is None or df.empty:
            return None
        date_col = 'trade_date' if 'trade_date' in df.columns else '日期'
        if date_col not in df.columns:
            return None
        return set(pd.to_datetime(df[date_col]).dt.date.tolist())
    except Exception as e:
        logger.warning(f"从 AKShare 获取交易日历失败，将回退到本地交易所日历: {e}")
        return None

def is_trading_day(check_date: datetime) -> bool:
    cn_date = check_date.date()
    today_cn = datetime.now(pytz.timezone('Asia/Shanghai')).date()

    # 优先使用 AKShare 提供的交易日（对当年节假日更准确），每天最多刷新一次
    loaded_at = TRADE_DAY_CACHE.get("loaded_at")
    need_refresh = not loaded_at or loaded_at.date() != today_cn
    if need_refresh:
        trade_days = _load_trade_days_from_ak()
        if trade_days is not None:
            TRADE_DAY_CACHE["days"] = trade_days
            TRADE_DAY_CACHE["loaded_at"] = datetime.now()

    trade_days_cache = TRADE_DAY_CACHE.get("days")
    if isinstance(trade_days_cache, set) and cn_date <= today_cn:
        return cn_date in trade_days_cache

    # 对未来日期或 AKShare 不可用场景，回退到 pandas_market_calendars
    return not CHINA_CALENDAR.valid_days(start_date=cn_date, end_date=cn_date).empty

def is_market_hours() -> bool:
    tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(tz)
    if not is_trading_day(now): return False
    time_now = now.time()
    return (time(9, 30) <= time_now <= time(11, 30)) or \
           (time(13, 0) <= time_now <= time(15, 0))


# --- Telegram 命令处理 ---
@whitelisted_only
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    config_info = f"复权: {'是' if USE_ADJUST else '否'}"
    await update.message.reply_html(f"你好, {user.mention_html()}!\n\n这是一个A股/ETF的RSI({RSI_PERIOD})监控机器人。\n({config_info})\n使用 /help 查看所有可用命令。")

@whitelisted_only
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    briefing_status_row = db_execute("SELECT daily_briefing_enabled FROM whitelist WHERE user_id = ?", (user_id,), fetchone=True)
    briefing_status = "开启" if briefing_status_row and briefing_status_row['daily_briefing_enabled'] else "关闭"
    help_text = f"""
<b>可用命令:</b>

<b>规则管理</b>
/add <code>CODE</code> <i>min</i> <i>max</i> - 添加规则
/del <code>ID</code> - 删除规则
/list - 查看我的规则
/on <code>ID</code> - 开启规则
/off <code>ID</code> - 关闭规则

<b>功能开关</b>
/check - 立即查询当前RSI值
/briefing <code>on|off</code> - 开/关您的每日简报 (您当前: <b>{briefing_status}</b>)

<b>白名单管理 (仅限管理员)</b>
/add_w <code>ID</code> - 添加用户
/del_w <code>ID</code> - 移除用户
/list_w - 查看白名单

<b>全局配置:</b>
- RSI 周期: <b>{RSI_PERIOD}</b>
- 计算模式: ({'复权' if USE_ADJUST else '不复权'})
- 请求间隔: <b>{REQUEST_INTERVAL_SECONDS}秒</b>
- 每日简报主开关: <b>{'开启' if ENABLE_DAILY_BRIEFING else '关闭'} ({BRIEFING_TIMES_STR})</b>
    """
    await update.message.reply_html(help_text)


@whitelisted_only
async def check_rsi_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    rules = db_execute("SELECT * FROM rules WHERE user_id = ? AND is_active = 1", (user_id,), fetchall=True)
    if not rules:
        await update.message.reply_text("您没有任何已激活的监控规则。")
        return
    sent_message = await update.message.reply_text("正在查询您规则中所有资产的最新RSI值，请稍候...")
    
    rules_by_code = defaultdict(list)
    for rule in rules: rules_by_code[rule['asset_code']].append(rule)
    unique_codes = sorted(list(rules_by_code.keys()))
    
    rsi_results = {}
    
    # 获取实时价格
    spot_data, success = await _fetch_all_spot_data(context, unique_codes)
    if not success:
        await sent_message.edit_text("获取实时价格失败，请稍后重试。")
        return
    
    # 获取缓存
    cache = context.bot_data.get(KEY_HIST_CACHE, {})

    for code in unique_codes:
        spot_price = spot_data.get(code)
        if spot_price is None:
            rsi_results[code] = "获取价格失败"
            continue
        
        # 缓存逻辑
        hist_df = cache.get(code)
        if hist_df is None:
            logger.info(f"/check: 缓存未命中，为 {code} 单独获取历史数据。")
            await asyncio.sleep(REQUEST_INTERVAL_SECONDS)
            hist_df = await get_history_data(code, HIST_FETCH_DAYS)
            if hist_df is not None: cache[code] = hist_df
        
        if hist_df is None:
            rsi_results[code] = "获取历史失败"
            continue

        prices = get_prices_for_rsi(hist_df, spot_price)
        rsi_value = calculate_rsi(prices)
        rsi_results[code] = f"{rsi_value:.2f}" if rsi_value is not None else "计算失败"

    message = f"<b>📈 最新RSI值查询结果:</b>\n\n"
    for code, code_rules in rules_by_code.items():
        asset_name = code_rules[0]['asset_name']
        rsi_val_str = rsi_results.get(code, "未查询")
        message += f"<b>{asset_name}</b> (<code>{code}</code>)\n"
        message += f"  - 当前 RSI({RSI_PERIOD}): <b>{rsi_val_str}</b>\n"
        for rule in code_rules:
            message += f"  - 监控区间: {rule['rsi_min']} - {rule['rsi_max']}\n"
        message += "\n"
    await sent_message.edit_text(message, parse_mode=ParseMode.HTML)


@whitelisted_only
async def briefing_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not context.args:
        status_row = db_execute("SELECT daily_briefing_enabled FROM whitelist WHERE user_id = ?", (user_id,), fetchone=True)
        status = "开启" if status_row and status_row['daily_briefing_enabled'] else "关闭"
        await update.message.reply_html(f"您的每日简报当前为 <b>{status}</b> 状态。\n\n使用 <code>/briefing on</code> 或 <code>/briefing off</code> 来进行设置。")
        return
    command = context.args[0].lower()
    if command == 'on':
        db_execute("UPDATE whitelist SET daily_briefing_enabled = 1 WHERE user_id = ?", (user_id,))
        await update.message.reply_text("✅ 已为您开启每日收盘简报功能。")
    elif command == 'off':
        db_execute("UPDATE whitelist SET daily_briefing_enabled = 0 WHERE user_id = ?", (user_id,))
        await update.message.reply_text("✅ 已为您关闭每日收盘简报功能。")
    else:
        await update.message.reply_text("指令格式错误。请使用 /briefing on 或 /briefing off。")
@whitelisted_only
async def add_rule_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    sent_message = None
    try:
        parts = update.message.text.split()
        if len(parts) != 4:
            await update.message.reply_text("命令格式错误。\n正确格式: /add <code> <min_rsi> <max_rsi>")
            return
        _, asset_code, rsi_min_str, rsi_max_str = parts
        rsi_min = float(rsi_min_str); rsi_max = float(rsi_max_str)
        if rsi_min >= rsi_max:
            await update.message.reply_text("错误：RSI最小值必须小于最大值。")
            return
        if rsi_min < 0 or rsi_max > 100:
            await update.message.reply_text("错误：RSI区间必须在 0 到 100 之间。")
            return
        sent_message = await update.message.reply_text(f"正在验证代码 {asset_code}...")
        
        # 验证代码有效性
        price = await _fetch_single_realtime_price(asset_code)
        if not price:
             await sent_message.edit_text(f"❌ 错误：无法获取代码 {asset_code} 的数据，请确认代码正确。")
             return

        asset_name = await get_asset_name_with_cache(asset_code, context)
        try:
            db_execute(
                "INSERT INTO rules (user_id, asset_code, asset_name, rsi_min, rsi_max) VALUES (?, ?, ?, ?, ?)",
                (user_id, asset_code, asset_name, rsi_min, rsi_max),
                swallow_errors=False,
            )
            await sent_message.edit_text(f"✅ 规则已添加:\n[{asset_name}({asset_code})] RSI区间: {rsi_min}-{rsi_max}")
        except sqlite3.IntegrityError:
            await sent_message.edit_text(f"❌ 错误：完全相同的规则 (代码和RSI区间) 已存在。")
    except ValueError:
        await update.message.reply_text("命令格式错误：RSI值必须是数字。")
    except Exception as e:
        logger.error(f"添加规则时出错: {e}")
        error_message = "添加规则时发生内部错误。"
        if sent_message: await sent_message.edit_text(error_message)
        else: await update.message.reply_text(error_message)
@whitelisted_only
async def list_rules_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    try:
        rules = db_execute("SELECT * FROM rules WHERE user_id = ?", (user_id,), fetchall=True)
        if not rules:
            await update.message.reply_text("您还没有设置任何规则。使用 /add 命令添加一个。")
            return
        message = "<b>您的监控规则列表:</b>\n\n"
        for rule in rules:
            status_icon = "🟢" if rule['is_active'] else "🔴"
            notif_text = ""
            is_triggered = rule['rsi_min'] <= rule['last_notified_rsi'] <= rule['rsi_max']
            if is_triggered and rule['notification_count'] > 0:
                notif_text = f"  - 触发中 (已通知: {rule['notification_count']}/{MAX_NOTIFICATIONS_PER_TRIGGER}次)\n"
            message += (f"{status_icon} <b>ID: {rule['id']}</b>\n  - 名称: {rule['asset_name']} ({rule['asset_code']})\n"
                        f"  - RSI 范围: {rule['rsi_min']} - {rule['rsi_max']}\n{notif_text}"
                        f"  - 状态: {'开启' if rule['is_active'] else '关闭'}\n\n")
        await update.message.reply_html(message)
    except Exception as e:
        logger.error(f"列出规则时出错: {e}")
        await update.message.reply_text("获取规则列表时发生错误。")
@whitelisted_only
async def delete_rule_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    try:
        _, rule_id_str = update.message.text.split()
        rule_id = int(rule_id_str)
        rule = db_execute("SELECT id FROM rules WHERE id = ? AND user_id = ?", (rule_id, user_id), fetchone=True)
        if not rule:
            await update.message.reply_text(f"错误：未找到ID为 {rule_id} 的规则，或该规则不属于您。")
            return
        db_execute("DELETE FROM rules WHERE id = ? AND user_id = ?", (rule_id, user_id))
        await update.message.reply_text(f"✅ 规则 ID: {rule_id} 已被删除。")
    except (ValueError, IndexError):
        await update.message.reply_text("命令格式错误。\n正确格式: /del <rule_id>")
    except Exception as e:
        logger.error(f"删除规则时出错: {e}")
        await update.message.reply_text("删除规则时发生错误。")
@whitelisted_only
async def toggle_rule_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    command, _, rule_id_str = update.message.text.partition(' ')
    new_status = 1 if command == '/on' else 0
    try:
        rule_id = int(rule_id_str)
        rule = db_execute("SELECT id FROM rules WHERE id = ? AND user_id = ?", (rule_id, user_id), fetchone=True)
        if not rule:
            await update.message.reply_text(f"错误：未找到ID为 {rule_id} 的规则，或该规则不属于您。")
            return
        db_execute("UPDATE rules SET is_active = ? WHERE id = ? AND user_id = ?", (new_status, rule_id, user_id))
        status_text = "开启" if new_status else "关闭"
        await update.message.reply_text(f"✅ 规则 ID: {rule_id} 已被设置为 **{status_text}** 状态。", parse_mode=ParseMode.MARKDOWN)
    except (ValueError, IndexError):
        await update.message.reply_text(f"命令格式错误。\n正确格式: {command} <rule_id>")
    except Exception as e:
        logger.error(f"切换规则状态时出错: {e}")
        await update.message.reply_text("切换规则状态时发生错误。")
@admin_only
async def add_whitelist_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        _, user_id_str = update.message.text.split(); user_id_to_add = int(user_id_str)
        add_to_whitelist(user_id_to_add)
        await update.message.reply_text(f"✅ 用户 {user_id_to_add} 已添加到白名单。")
    except (ValueError, IndexError): await update.message.reply_text("命令格式错误。\n正确格式: /add_w <user_id>")
@admin_only
async def del_whitelist_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        _, user_id_str = update.message.text.split(); user_id_to_del = int(user_id_str)
        if user_id_to_del == ADMIN_USER_ID:
            await update.message.reply_text("❌ 不能将管理员从白名单中删除。")
            return
        remove_from_whitelist(user_id_to_del)
        await update.message.reply_text(f"✅ 用户 {user_id_to_del} 已从白名单中移除。")
    except (ValueError, IndexError): await update.message.reply_text("命令格式错误。\n正确格式: /del_w <user_id>")
@admin_only
async def list_whitelist_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    users = db_execute("SELECT * FROM whitelist", fetchall=True)
    if not users: await update.message.reply_text("白名单中没有任何用户。"); return
    message = "<b>白名单用户列表:</b>\n\n"
    for user in users:
        is_admin_text = " (管理员)" if user['user_id'] == ADMIN_USER_ID else ""
        briefing_enabled_text = " (简报:开)" if user['daily_briefing_enabled'] else ""
        message += f"- <code>{user['user_id']}</code>{is_admin_text}{briefing_enabled_text}\n"
    await update.message.reply_html(message)


# --- 后台监控任务 ---
async def check_rules_job(context: ContextTypes.DEFAULT_TYPE):
    if not is_market_hours(): return
    if RANDOM_DELAY_MAX_SECONDS > 0:
        delay = random.uniform(0, RANDOM_DELAY_MAX_SECONDS)
        logger.info(f"应用启动延迟: {delay:.2f}秒")
        await asyncio.sleep(delay)
    
    logger.info("交易时间，开始执行规则检查...")
    active_rules = db_execute("SELECT * FROM rules WHERE is_active = 1", fetchall=True)
    if not active_rules: return

    bot_data = context.bot_data
    all_codes = {rule['asset_code'] for rule in active_rules}
    
    now = datetime.now(pytz.timezone('Asia/Shanghai'))
    hist_data_cache = ensure_daily_history_cache(context, now)
    codes_to_fetch_hist = [code for code in all_codes if code not in hist_data_cache]
    
    if codes_to_fetch_hist:
        logger.info(f"需要为 {len(codes_to_fetch_hist)} 个新资产顺序获取历史数据...")
        for code in codes_to_fetch_hist:
            logger.debug(f"正在获取 {code} 的历史数据...")
            data = await get_history_data(code, HIST_FETCH_DAYS)
            if data is not None:
                hist_data_cache[code] = data
            logger.debug(f"应用请求间隔: {REQUEST_INTERVAL_SECONDS}秒")
            await asyncio.sleep(REQUEST_INTERVAL_SECONDS)

    spot_data, success = await _fetch_all_spot_data(context, list(all_codes))
    if not success: return

    rsi_by_code: Dict[str, float] = {}
    for code in all_codes:
        hist_df = hist_data_cache.get(code)
        spot_price = spot_data.get(code)
        if hist_df is None or spot_price is None:
            continue
        prices = get_prices_for_rsi(hist_df, spot_price)
        current_rsi = calculate_rsi(prices)
        if current_rsi is not None:
            rsi_by_code[code] = current_rsi

    def _in_range(rsi_value: Union[float, int, None], rsi_min: float, rsi_max: float) -> bool:
        if rsi_value is None:
            return False
        try:
            value = float(rsi_value)
            if math.isnan(value):
                return False
            return rsi_min <= value <= rsi_max
        except (TypeError, ValueError):
            return False

    def _build_notification_chunks(
        rules_for_user: List[Tuple[sqlite3.Row, float]],
        max_len: int = 3500
    ) -> List[Tuple[str, List[Tuple[sqlite3.Row, float]]]]:
        """
        将单用户触发规则分块，避免 Telegram 4096 字符上限导致整条消息发送失败。
        每个 chunk 返回 (message, 对应规则列表)。
        """
        header = "🎯 <b>RSI 警报汇总</b> 🎯\n\n"
        chunks: List[Tuple[str, List[Tuple[sqlite3.Row, float]]]] = []
        current_parts: List[str] = [header]
        current_rules: List[Tuple[sqlite3.Row, float]] = []

        for rule, current_rsi in rules_for_user:
            safe_asset_name = html.escape(str(rule['asset_name'] or "未知资产"))
            section = (
                f"• <b>{safe_asset_name} ({rule['asset_code']})</b>\n"
                f"  RSI({RSI_PERIOD}): <b>{current_rsi:.2f}</b>\n"
                f"  目标区间: <code>{rule['rsi_min']} - {rule['rsi_max']}</code>\n"
                f"  通知次数: <b>{rule['notification_count'] + 1}/{MAX_NOTIFICATIONS_PER_TRIGGER}</b>\n\n"
            )

            tentative = "".join(current_parts) + section
            if len(tentative) > max_len and current_rules:
                chunks.append(("".join(current_parts).strip(), current_rules.copy()))
                current_parts = [header, section]
                current_rules = [(rule, current_rsi)]
            else:
                current_parts.append(section)
                current_rules.append((rule, current_rsi))

        if current_rules:
            chunks.append(("".join(current_parts).strip(), current_rules.copy()))
        return chunks

    pending_notifications: Dict[int, List[Tuple[sqlite3.Row, float]]] = defaultdict(list)
    for rule in active_rules:
        asset_code = rule['asset_code']
        current_rsi = rsi_by_code.get(asset_code)
        if current_rsi is None:
            continue

        logger.debug(f"检查: {rule['asset_name']}({asset_code}) | RSI({RSI_PERIOD}): {current_rsi}")
        is_triggered = _in_range(current_rsi, rule['rsi_min'], rule['rsi_max'])
        last_notified_rsi_in_range = _in_range(rule['last_notified_rsi'], rule['rsi_min'], rule['rsi_max'])

        if is_triggered and rule['notification_count'] < MAX_NOTIFICATIONS_PER_TRIGGER:
            pending_notifications[rule['user_id']].append((rule, current_rsi))
            continue

        if not is_triggered and last_notified_rsi_in_range:
            logger.info(f"离开区间: {asset_code} | 重置通知计数器。")
            db_execute(
                "UPDATE rules SET last_notified_rsi = ?, notification_count = 0 WHERE id = ?",
                (current_rsi, rule['id'])
            )
        elif is_triggered:
            db_execute("UPDATE rules SET last_notified_rsi = ? WHERE id = ?", (current_rsi, rule['id']))

    for user_id, triggered_rules in pending_notifications.items():
        triggered_rules_sorted = sorted(
            triggered_rules,
            key=lambda item: (item[0]['asset_code'], item[0]['rsi_min'], item[0]['rsi_max'], item[0]['id'])
        )
        message_chunks = _build_notification_chunks(triggered_rules_sorted)
        for message, rules_in_chunk in message_chunks:
            sent = False
            for _ in range(2):
                try:
                    await context.bot.send_message(chat_id=user_id, text=message, parse_mode=ParseMode.HTML)
                    sent = True
                    break
                except RetryAfter as e:
                    wait_seconds = int(getattr(e, "retry_after", 1)) + 1
                    logger.warning(f"发送通知触发限流，{wait_seconds}秒后重试。用户: {user_id}")
                    await asyncio.sleep(wait_seconds)
                except Exception as e:
                    logger.error(f"向用户 {user_id} 发送通知失败: {e}")
                    break

            if sent:
                for rule, current_rsi in rules_in_chunk:
                    logger.info(
                        f"已发送通知: {rule['asset_code']} | 用户: {user_id} | "
                        f"(第 {rule['notification_count'] + 1} 次)"
                    )
                    db_execute(
                        "UPDATE rules SET last_notified_rsi = ?, notification_count = notification_count + 1 WHERE id = ?",
                        (current_rsi, rule['id'])
                    )

async def daily_briefing_job(context: ContextTypes.DEFAULT_TYPE):
    if not ENABLE_DAILY_BRIEFING: return
    tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(tz)
    if not is_trading_day(now):
        logger.info(f"今天 ({now.strftime('%Y-%m-%d')}) 非交易日，跳过每日简报。")
        return
    
    logger.info("开始执行每日收盘RSI简报任务...")
    enabled_users_rows = db_execute("SELECT user_id FROM whitelist WHERE daily_briefing_enabled = 1", fetchall=True)
    if not enabled_users_rows: return
    
    enabled_user_ids = {row['user_id'] for row in enabled_users_rows}
    all_briefing_rules = db_execute("SELECT * FROM rules WHERE is_active = 1 AND user_id IN ({})".format(','.join('?' for _ in enabled_user_ids)), tuple(enabled_user_ids), fetchall=True)
    if not all_briefing_rules: return

    all_unique_codes = sorted(list({rule['asset_code'] for rule in all_briefing_rules}))
    
    spot_data, success = await _fetch_all_spot_data(context, all_unique_codes)
    if not success:
        logger.error("执行每日简报任务时获取数据失败，任务中止。")
        return

    hist_data_cache = ensure_daily_history_cache(context, now)

    rsi_results: Dict[str, Union[str, float]] = {}
    for code in all_unique_codes:
        spot_price = spot_data.get(code)
        if spot_price is None:
            rsi_results[code] = "N/A"
            continue
        
        hist_df = hist_data_cache.get(code)
        if hist_df is None:
            await asyncio.sleep(REQUEST_INTERVAL_SECONDS)
            hist_df = await get_history_data(code, HIST_FETCH_DAYS)
            if hist_df is None:
                rsi_results[code] = "N/A"
                continue
            hist_data_cache[code] = hist_df
        
        prices = get_prices_for_rsi(hist_df, spot_price)
        rsi_value = calculate_rsi(prices) if prices is not None else "N/A"
        rsi_results[code] = rsi_value

    today_str_display = now.strftime('%Y年%m月%d日')
    rules_by_user = defaultdict(list)
    for rule in all_briefing_rules: rules_by_user[rule['user_id']].append(rule)
        
    for user_id, user_rules in rules_by_user.items():
        message = f"📰 <b>收盘RSI简报 ({today_str_display})</b>\n\n"
        user_rules_by_code = defaultdict(list)
        for rule in user_rules: user_rules_by_code[rule['asset_code']].append(rule)
        
        for code, code_rules in sorted(user_rules_by_code.items()):
            asset_name = code_rules[0]['asset_name']
            rsi_val = rsi_results.get(code)
            if isinstance(rsi_val, float):
                is_triggered = any(rule['rsi_min'] <= rsi_val <= rule['rsi_max'] for rule in code_rules)
                icon = "🎯" if is_triggered else "▪️"
                rsi_str = f"<b>{rsi_val:.2f}</b>"
            else:
                icon = "❓"; rsi_str = "查询失败"
            message += f"{icon} <b>{asset_name}</b> (<code>{code}</code>)\n"
            message += f"  - 收盘 RSI({RSI_PERIOD}): {rsi_str}\n"
            for rule in code_rules: message += f"  - 监控区间: {rule['rsi_min']} - {rule['rsi_max']}\n"
            message += "\n"
        try:
            await context.bot.send_message(chat_id=user_id, text=message, parse_mode=ParseMode.HTML)
            logger.info(f"已成功向用户 {user_id} 发送每日简报。")
        except Forbidden:
            logger.warning(f"无法向用户 {user_id} 发送每日简报，可能已被禁用。")
        except Exception as e:
            logger.error(f"向用户 {user_id} 发送每日简报时发生未知错误: {e}")


# --- 启动与初始化 ---
async def post_init(application: Application):
    """在机器人启动后设置自定义命令并初始化bot_data。"""
    commands = [
        BotCommand("start", "开始使用机器人"), BotCommand("help", "获取帮助信息"),
        BotCommand("check", "立即查询当前RSI"), BotCommand("briefing", "开关每日简报"),
        BotCommand("add", "添加监控: CODE min max"), BotCommand("del", "删除监控: ID"),
        BotCommand("list", "查看我的监控"), BotCommand("on", "开启监控: ID"),
        BotCommand("off", "关闭监控: ID"),
    ]
    await application.bot.set_my_commands(commands)
    bot_data = application.bot_data
    for key in [KEY_HIST_CACHE, KEY_NAME_CACHE]: bot_data[key] = {}
    for key in [KEY_FAILURE_COUNT, KEY_FAILURE_SENT]: bot_data[key] = 0
    bot_data[KEY_CACHE_DATE] = None
    
    # 预加载缓存
    all_rules = db_execute("SELECT asset_code, asset_name FROM rules", fetchall=True)
    if all_rules:
        for rule in all_rules:
            if rule['asset_code'] and rule['asset_name']:
                bot_data[KEY_NAME_CACHE][rule['asset_code']] = rule['asset_name']
        logger.info(f"从数据库预加载了 {len(bot_data[KEY_NAME_CACHE])} 个资产名称到缓存。")
    logger.info("Bot application data 初始化完成。")

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """记录所有未被捕获的异常。"""
    logger.error(f"未捕获的异常: {context.error}", exc_info=False)

def main():
    """主函数，用于启动机器人。"""
    if not TELEGRAM_TOKEN or not ADMIN_USER_ID:
        logger.critical("错误: 环境变量 TELEGRAM_TOKEN 和 ADMIN_USER_ID 必须被正确设置!")
        return
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
    db_init()
    application = Application.builder().token(TELEGRAM_TOKEN).post_init(post_init).build()
    application.add_error_handler(error_handler)
    handlers = [
        CommandHandler("start", start_command), CommandHandler("help", help_command),
        CommandHandler("check", check_rsi_command), CommandHandler("briefing", briefing_command),
        CommandHandler("add", add_rule_command), CommandHandler("list", list_rules_command),
        CommandHandler("del", delete_rule_command), CommandHandler("on", toggle_rule_status_command),
        CommandHandler("off", toggle_rule_status_command), CommandHandler("add_w", add_whitelist_command),
        CommandHandler("del_w", del_whitelist_command), CommandHandler("list_w", list_whitelist_command)
    ]
    application.add_handlers(handlers)
    job_queue = application.job_queue
    job_queue.run_repeating(check_rules_job, interval=CHECK_INTERVAL_SECONDS, first=10)
    if ENABLE_DAILY_BRIEFING:
        briefing_times = [t.strip() for t in BRIEFING_TIMES_STR.split(',') if t.strip()]
        successful_times = []
        for time_str in briefing_times:
            try:
                hour, minute = map(int, time_str.split(':'))
                tz_shanghai = pytz.timezone('Asia/Shanghai')
                briefing_time = time(hour, minute, tzinfo=tz_shanghai)
                job_queue.run_daily(daily_briefing_job, time=briefing_time, name=f"daily_briefing_{time_str}")
                successful_times.append(time_str)
            except (ValueError, IndexError):
                logger.error(f"每日简报时间格式错误 ('{time_str}')，应为 HH:MM 格式。该时间点的任务未开启。")
        if successful_times:
            logger.info(f"已成功注册每日简报任务，将于每天 {', '.join(successful_times)} (上海时间) 执行。")
    logger.info("机器人正在启动...")
    application.run_polling()

if __name__ == '__main__':
    main()
