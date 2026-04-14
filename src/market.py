# -*- coding: utf-8 -*-

import asyncio
import logging
from datetime import datetime, time
from typing import Dict, Union

import akshare as ak
import pandas as pd
import pandas_market_calendars as mcal
import pytz
import requests

from .config import EM_BLOCK_CHECK_INTERVAL_SECONDS, EM_BLOCK_CHECK_URL

logger = logging.getLogger(__name__)

CHINA_CALENDAR = mcal.get_calendar('XSHG')

# --- 带锁的全局缓存 ---
_em_block_lock = asyncio.Lock()
_em_block_cache: Dict[str, Union[bool, datetime, None]] = {"blocked": None, "checked_at": None}

_trade_day_lock = asyncio.Lock()
_trade_day_cache: Dict[str, Union[set, datetime, None]] = {"days": None, "loaded_at": None}


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

    loaded_at = _trade_day_cache.get("loaded_at")
    need_refresh = not loaded_at or loaded_at.date() != today_cn
    if need_refresh:
        trade_days = _load_trade_days_from_ak()
        if trade_days is not None:
            _trade_day_cache["days"] = trade_days
            _trade_day_cache["loaded_at"] = datetime.now()

    trade_days_cache = _trade_day_cache.get("days")
    if isinstance(trade_days_cache, set) and cn_date <= today_cn:
        return cn_date in trade_days_cache

    return not CHINA_CALENDAR.valid_days(start_date=cn_date, end_date=cn_date).empty


def is_market_hours() -> bool:
    tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(tz)
    if not is_trading_day(now):
        return False
    time_now = now.time()
    return (time(9, 30) <= time_now <= time(11, 30)) or \
           (time(13, 0) <= time_now <= time(15, 0))


async def is_em_blocked() -> bool:
    """检测东方财富接口是否被封禁，带异步锁保护。"""
    async with _em_block_lock:
        now = datetime.now()
        last_checked = _em_block_cache.get("checked_at")
        if last_checked and (now - last_checked).total_seconds() < EM_BLOCK_CHECK_INTERVAL_SECONDS:
            blocked = _em_block_cache.get("blocked")
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
        _em_block_cache["blocked"] = blocked
        _em_block_cache["checked_at"] = now
        if blocked:
            logger.warning("检测到东方财富接口被封禁，后续将直接使用新浪接口。")
        return blocked
