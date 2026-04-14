# -*- coding: utf-8 -*-

import asyncio
import logging
from collections import OrderedDict
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Union

import akshare as ak
import pandas as pd
import pytz
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from .config import (
    ADMIN_USER_ID,
    ETF_PREFIXES,
    FETCH_FAILURE_THRESHOLD,
    FETCH_RETRY_ATTEMPTS,
    FETCH_RETRY_DELAY_SECONDS,
    HIST_FETCH_DAYS,
    KEY_CACHE_DATE,
    KEY_FAILURE_COUNT,
    KEY_FAILURE_SENT,
    KEY_HIST_CACHE,
    KEY_NAME_CACHE,
    NAME_CACHE_MAX_SIZE,
    REQUEST_INTERVAL_SECONDS,
    RSI_PERIOD,
    STOCK_PREFIXES,
    USE_ADJUST,
)
from .market import is_em_blocked
from .utils import get_sina_symbol, normalize_hist_df

logger = logging.getLogger(__name__)


# --- 重试逻辑（改进5: 指数退避） ---

async def _run_with_retries(operation, description: str):
    for attempt in range(1, FETCH_RETRY_ATTEMPTS + 1):
        result = await operation()
        if result is not None:
            return result
        if attempt < FETCH_RETRY_ATTEMPTS:
            delay = FETCH_RETRY_DELAY_SECONDS * (2 ** (attempt - 1))
            logger.warning(
                f"{description} 失败，{delay}秒后重试 "
                f"({attempt}/{FETCH_RETRY_ATTEMPTS})。"
            )
            await asyncio.sleep(delay)
    return None


# --- 缓存 ---

def ensure_daily_history_cache(context: ContextTypes.DEFAULT_TYPE, now: datetime) -> Dict[str, pd.DataFrame]:
    bot_data = context.bot_data
    today_str = now.strftime('%Y-%m-%d')
    if bot_data.get(KEY_CACHE_DATE) != today_str:
        logger.info(f"日期变更或首次运行，清空并重建 {today_str} 的历史数据缓存。")
        bot_data[KEY_HIST_CACHE] = {}
        bot_data[KEY_CACHE_DATE] = today_str
    return bot_data.get(KEY_HIST_CACHE, {})


# --- 资产名称缓存（改进7: LRU 上限） ---

async def get_asset_name_with_cache(asset_code: str, context: ContextTypes.DEFAULT_TYPE) -> str:
    name_cache: OrderedDict = context.bot_data.get(KEY_NAME_CACHE, OrderedDict())
    # 确保 bot_data 中存的是 OrderedDict
    if not isinstance(name_cache, OrderedDict):
        name_cache = OrderedDict(name_cache)
        context.bot_data[KEY_NAME_CACHE] = name_cache

    if asset_code in name_cache:
        # 移到末尾（最近使用）
        name_cache.move_to_end(asset_code)
        logger.debug(f"从缓存命中资产名称: {asset_code} -> {name_cache[asset_code]}")
        return name_cache[asset_code]

    logger.info(f"缓存未命中，尝试获取资产名称: {asset_code}")
    await asyncio.sleep(REQUEST_INTERVAL_SECONDS)

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

    # 淘汰最旧条目
    while len(name_cache) >= NAME_CACHE_MAX_SIZE:
        name_cache.popitem(last=False)

    name_cache[asset_code] = name
    logger.debug(f"已将新资产名称存入缓存: {asset_code} -> {name}")
    return name


# --- 历史数据获取 ---

async def get_history_data(asset_code: str, days: int) -> Union[pd.DataFrame, None]:
    """获取单个资产的历史日线数据，并在需要时计算复权因子。"""
    try:
        today = datetime.now()
        start_date = (today - timedelta(days=days)).strftime('%Y%m%d')
        end_date = today.strftime('%Y%m%d')
        adjust = "qfq" if USE_ADJUST else ""

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

        use_em = not await is_em_blocked()
        df = None
        source = "sina"
        if use_em:
            df = await _run_with_retries(fetch_hist_em, f"获取历史数据({asset_code})")
            source = "em"
        # Bug3: 统一使用 is None or empty 判断
        if df is None or df.empty:
            logger.info(f"尝试使用新浪接口获取历史数据({asset_code})。")
            df = await _run_with_retries(fetch_hist_sina, f"获取历史数据-新浪({asset_code})")
            source = "sina"
        if df is None or df.empty:
            return None
        df = normalize_hist_df(df)
        if df is None or df.empty or "日期" not in df.columns:
            return None
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

        use_em = not await is_em_blocked()
        raw_df = None
        sina_attempted = False

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
        raw_df = normalize_hist_df(raw_df)
        if raw_df is None or raw_df.empty or "日期" not in raw_df.columns:
            return 1.0
        raw_df.set_index('日期', inplace=True)

        base_ts = pd.Timestamp(base_date)
        raw_idx = pd.to_datetime(raw_df.index)
        adj_idx = pd.to_datetime(hist_df.index)
        common_dates = raw_idx.intersection(adj_idx)
        candidate_dates = common_dates[common_dates <= base_ts]
        if candidate_dates.empty:
            return 1.0
        aligned_date = candidate_dates.max()

        raw_close = raw_df.loc[aligned_date, '收盘']
        # Bug6: 使用 pd.isna 检测 NaN
        if pd.isna(raw_close) or raw_close == 0:
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
    # Bug5: 明确检查 None 和 0
    if adjust_factor is None or adjust_factor == 0:
        return float(spot_price)
    return float(spot_price) * float(adjust_factor)


# --- 实时价格获取 ---

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


async def _fetch_all_spot_data(context: ContextTypes.DEFAULT_TYPE, codes: List[str]) -> Tuple[Dict, bool]:
    """获取实时数据，逐个获取以保证稳定性。"""
    spot_dict = {}
    success_count = 0

    for code in codes:
        await asyncio.sleep(REQUEST_INTERVAL_SECONDS)
        price = await _fetch_single_realtime_price(code)
        if price is not None:
            spot_dict[code] = price
            success_count += 1

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

    if context.bot_data.get(KEY_FAILURE_COUNT, 0) > 0:
        logger.info("数据获取成功，重置失败计数器。")
    context.bot_data[KEY_FAILURE_COUNT] = 0
    context.bot_data[KEY_FAILURE_SENT] = False
    return spot_dict, True


# --- RSI 计算 ---

def get_prices_for_rsi(hist_df: pd.DataFrame, spot_price: float) -> Union[pd.Series, None]:
    """根据历史和实时价格准备用于 RSI 计算的价格序列。"""
    if hist_df is None or hist_df.empty:
        return None
    if '收盘' not in hist_df.columns:
        return None
    close_prices = hist_df['收盘'].copy()
    last_date_in_hist = close_prices.index[-1].date()
    today_date = datetime.now(pytz.timezone('Asia/Shanghai')).date()
    adjusted_spot_price = _adjust_spot_price(hist_df, spot_price)
    if last_date_in_hist < today_date:
        close_prices.loc[pd.Timestamp(today_date)] = adjusted_spot_price
    else:
        close_prices.iloc[-1] = adjusted_spot_price
    return close_prices


def calculate_rsi_exact(prices: pd.Series, period: int = 6) -> Union[float, None]:
    """
    完全复刻同花顺/东财算法的 RSI 计算函数。
    使用 pandas 原生 ewm(alpha=1/N) 实现 Wilder 平滑。
    """
    try:
        if len(prices) < period + 1:
            return None

        delta = prices.diff()
        gain = delta.clip(lower=0)
        loss = -1 * delta.clip(upper=0)

        avg_gain = gain.ewm(alpha=1 / period, adjust=True).mean()
        avg_loss = loss.ewm(alpha=1 / period, adjust=True).mean()

        # Bug2: 除零保护 — avg_loss 为 0 时 RSI 定义为 100
        last_avg_loss = avg_loss.iloc[-1]
        if last_avg_loss == 0:
            return 100.0

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        result = rsi.iloc[-1]
        if pd.isna(result):
            return None
        return round(float(result), 2)
    except Exception as e:
        logger.error(f"RSI计算出错: {e}")
        return None


def calculate_rsi(prices: pd.Series) -> Union[float, None]:
    return calculate_rsi_exact(prices, period=RSI_PERIOD)
