# -*- coding: utf-8 -*-

import asyncio
import html
import logging
import math
import random
import sqlite3
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Tuple, Union

import pytz
from telegram.constants import ParseMode
from telegram.error import Forbidden, RetryAfter
from telegram.ext import ContextTypes

from .config import (
    ADMIN_USER_ID,
    ENABLE_DAILY_BRIEFING,
    HIST_FETCH_DAYS,
    KEY_HIST_CACHE,
    MAX_NOTIFICATIONS_PER_TRIGGER,
    RANDOM_DELAY_MAX_SECONDS,
    REQUEST_INTERVAL_SECONDS,
    RSI_PERIOD,
)
from .database import db_execute
from .data_fetcher import (
    _fetch_all_spot_data,
    calculate_rsi,
    ensure_daily_history_cache,
    get_history_data,
    get_prices_for_rsi,
)
from .market import is_market_hours, is_trading_day

logger = logging.getLogger(__name__)


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


# --- 后台监控任务 ---

async def check_rules_job(context: ContextTypes.DEFAULT_TYPE):
    if not is_market_hours():
        return
    if RANDOM_DELAY_MAX_SECONDS > 0:
        delay = random.uniform(0, RANDOM_DELAY_MAX_SECONDS)
        logger.info(f"应用启动延迟: {delay:.2f}秒")
        await asyncio.sleep(delay)

    logger.info("交易时间，开始执行规则检查...")
    active_rules = db_execute("SELECT * FROM rules WHERE is_active = 1", fetchall=True)
    if not active_rules:
        return

    all_codes = {rule['asset_code'] for rule in active_rules}

    now = datetime.now(pytz.timezone('Asia/Shanghai'))
    hist_data_cache = ensure_daily_history_cache(context, now)
    codes_to_fetch_hist = [code for code in all_codes if code not in hist_data_cache]

    if codes_to_fetch_hist:
        logger.info(f"需要为 {len(codes_to_fetch_hist)} 个新资产顺序获取历史数据...")
        for code in codes_to_fetch_hist:
            logger.debug(f"正在获取 {code} 的历史数据...")
            data = await get_history_data(code, HIST_FETCH_DAYS)
            if data is not None and not data.empty:
                hist_data_cache[code] = data
            logger.debug(f"应用请求间隔: {REQUEST_INTERVAL_SECONDS}秒")
            await asyncio.sleep(REQUEST_INTERVAL_SECONDS)

    spot_data, success = await _fetch_all_spot_data(context, list(all_codes))
    if not success:
        return

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
    if not ENABLE_DAILY_BRIEFING:
        return
    tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(tz)
    if not is_trading_day(now):
        logger.info(f"今天 ({now.strftime('%Y-%m-%d')}) 非交易日，跳过每日简报。")
        return

    logger.info("开始执行每日收盘RSI简报任务...")
    enabled_users_rows = db_execute(
        "SELECT user_id FROM whitelist WHERE daily_briefing_enabled = 1", fetchall=True
    )
    if not enabled_users_rows:
        return

    enabled_user_ids = {row['user_id'] for row in enabled_users_rows}
    all_briefing_rules = db_execute(
        "SELECT * FROM rules WHERE is_active = 1 AND user_id IN ({})".format(
            ','.join('?' for _ in enabled_user_ids)
        ),
        tuple(enabled_user_ids),
        fetchall=True,
    )
    if not all_briefing_rules:
        return

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
            if hist_df is None or hist_df.empty:
                rsi_results[code] = "N/A"
                continue
            hist_data_cache[code] = hist_df

        prices = get_prices_for_rsi(hist_df, spot_price)
        rsi_value = calculate_rsi(prices) if prices is not None else "N/A"
        rsi_results[code] = rsi_value

    today_str_display = now.strftime('%Y年%m月%d日')
    rules_by_user = defaultdict(list)
    for rule in all_briefing_rules:
        rules_by_user[rule['user_id']].append(rule)

    for user_id, user_rules in rules_by_user.items():
        message = f"📰 <b>收盘RSI简报 ({today_str_display})</b>\n\n"
        user_rules_by_code = defaultdict(list)
        for rule in user_rules:
            user_rules_by_code[rule['asset_code']].append(rule)

        for code, code_rules in sorted(user_rules_by_code.items()):
            asset_name = code_rules[0]['asset_name']
            rsi_val = rsi_results.get(code)
            if isinstance(rsi_val, float):
                is_triggered = any(rule['rsi_min'] <= rsi_val <= rule['rsi_max'] for rule in code_rules)
                icon = "🎯" if is_triggered else "▪️"
                rsi_str = f"<b>{rsi_val:.2f}</b>"
            else:
                icon = "❓"
                rsi_str = "查询失败"
            message += f"{icon} <b>{asset_name}</b> (<code>{code}</code>)\n"
            message += f"  - 收盘 RSI({RSI_PERIOD}): {rsi_str}\n"
            for rule in code_rules:
                message += f"  - 监控区间: {rule['rsi_min']} - {rule['rsi_max']}\n"
            message += "\n"
        try:
            await context.bot.send_message(chat_id=user_id, text=message, parse_mode=ParseMode.HTML)
            logger.info(f"已成功向用户 {user_id} 发送每日简报。")
        except Forbidden:
            logger.warning(f"无法向用户 {user_id} 发送每日简报，可能已被禁用。")
        except Exception as e:
            logger.error(f"向用户 {user_id} 发送每日简报时发生未知错误: {e}")
