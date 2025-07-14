# -*- coding: utf-8 -*-

import logging
import sqlite3
import pandas as pd
import pandas_ta as ta
import akshare as ak
from datetime import datetime, time, timedelta
import pytz
import asyncio
from functools import wraps
from typing import Union, Dict
import os
import random
import pandas_market_calendars as mcal
from collections import defaultdict

from telegram import Update, BotCommand
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.constants import ParseMode
from telegram.error import Forbidden, NetworkError

# --- 机器人配置 (从环境变量读取) ---
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
ADMIN_USER_ID_STR = os.getenv('ADMIN_USER_ID')
ADMIN_USER_ID = int(ADMIN_USER_ID_STR) if ADMIN_USER_ID_STR and ADMIN_USER_ID_STR.isdigit() else None
CHECK_INTERVAL_SECONDS = int(os.getenv('CHECK_INTERVAL_SECONDS', '60'))
DB_FILE = os.getenv('DB_FILE', 'rules.db')

# --- 监控参数配置 (从环境变量读取) ---
RSI_PERIOD = int(os.getenv('RSI_PERIOD', '6'))
HIST_FETCH_DAYS = int(os.getenv('HIST_FETCH_DAYS', '60'))
MAX_NOTIFICATIONS_PER_TRIGGER = int(os.getenv('MAX_NOTIFICATIONS_PER_TRIGGER', '1'))

# --- 高级配置 (从环境变量读取) ---
RANDOM_DELAY_MAX_SECONDS = float(os.getenv('RANDOM_DELAY_MAX_SECONDS', '0'))
FETCH_FAILURE_THRESHOLD = int(os.getenv('FETCH_FAILURE_THRESHOLD', '5'))
REQUEST_INTERVAL_SECONDS = float(os.getenv('REQUEST_INTERVAL_SECONDS', '1.0'))
ENABLE_DAILY_BRIEFING = os.getenv('ENABLE_DAILY_BRIEFING', 'false').lower() == 'true'
BRIEFING_TIME_STR = os.getenv('DAILY_BRIEFING_TIME', '20:30')

# --- 日志配置 ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram.ext").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# --- 应用内常量 ---
KEY_HIST_CACHE = 'hist_data_cache'
KEY_NAME_CACHE = 'name_cache'
KEY_CACHE_DATE = 'cache_date'
KEY_FAILURE_COUNT = 'fetch_failure_count'
KEY_FAILURE_SENT = 'failure_notification_sent'
STOCK_PREFIXES = ('0', '3', '6')
ETF_PREFIXES = ('5', '1')

CHINA_CALENDAR = mcal.get_calendar('XSHG')


# --- 数据库模块 ---
def db_init():
    """初始化数据库，如果文件不存在则创建，并创建必要的表。"""
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


def db_execute(query, params=(), fetchone=False, fetchall=False):
    """执行数据库查询的通用函数。"""
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
        return None


# --- 白名单管理 ---
def is_whitelisted(user_id: int) -> bool: return db_execute("SELECT 1 FROM whitelist WHERE user_id = ?", (user_id,), fetchone=True) is not None
def add_to_whitelist(user_id: int): db_execute("INSERT OR IGNORE INTO whitelist (user_id) VALUES (?)", (user_id,))
def remove_from_whitelist(user_id: int): db_execute("DELETE FROM whitelist WHERE user_id = ?", (user_id,))
def get_whitelist(): return db_execute("SELECT * FROM whitelist", fetchall=True)


# --- 装饰器 ---
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


# --- 数据获取与计算模块 ---
async def get_asset_name_with_cache(asset_code: str, context: ContextTypes.DEFAULT_TYPE) -> str:
    """优先从缓存获取资产名称，否则通过API获取并存入缓存。"""
    name_cache = context.bot_data.get(KEY_NAME_CACHE, {})
    if asset_code in name_cache:
        logger.debug(f"从缓存命中资产名称: {asset_code} -> {name_cache[asset_code]}")
        return name_cache[asset_code]
    logger.info(f"缓存未命中，通过API获取资产名称: {asset_code}")
    await asyncio.sleep(REQUEST_INTERVAL_SECONDS)
    name = None
    try:
        if asset_code.startswith(STOCK_PREFIXES):
            stock_info = await asyncio.to_thread(ak.stock_individual_info_em, symbol=asset_code)
            name = stock_info.loc[stock_info['item'] == '股票简称', 'value'].iloc[0]
        elif asset_code.startswith(ETF_PREFIXES):
            all_etfs = await asyncio.to_thread(ak.fund_etf_spot_em)
            target = all_etfs.loc[all_etfs['代码'] == asset_code, '名称']
            if not target.empty: name = target.iloc[0]
    except Exception as e:
        logger.error(f"API获取 {asset_code} 名称时发生错误: {e}")
    if name:
        name_cache[asset_code] = name
        logger.debug(f"已将新资产名称存入缓存: {asset_code} -> {name}")
        return name
    else:
        return f"未知资产({asset_code})"


async def get_history_data(asset_code: str) -> Union[pd.DataFrame, None]:
    """获取单个资产的历史日线数据。"""
    try:
        today = datetime.now()
        start_date = (today - timedelta(days=HIST_FETCH_DAYS)).strftime('%Y%m%d')
        end_date = today.strftime('%Y%m%d')
        if asset_code.startswith(STOCK_PREFIXES):
            df = await asyncio.to_thread(ak.stock_zh_a_hist, symbol=asset_code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
        elif asset_code.startswith(ETF_PREFIXES):
            df = await asyncio.to_thread(ak.fund_etf_hist_em, symbol=asset_code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
        else:
            return None
        if df is not None and not df.empty:
            df['日期'] = pd.to_datetime(df['日期'])
            df.set_index('日期', inplace=True)
        return df
    except Exception as e:
        logger.error(f"获取 {asset_code} 历史数据失败: {e}")
        return None


def calculate_rsi_with_spot_price(hist_df: pd.DataFrame, spot_price: float) -> Union[float, None]:
    """使用已获取的历史数据和实时价格计算RSI，正确处理当天数据。"""
    try:
        if hist_df is None or hist_df.empty: return None
        price_col = '收盘'
        if price_col not in hist_df.columns: return None
        close_prices = hist_df[price_col].copy()
        last_date_in_hist = close_prices.index[-1].date()
        today_date = datetime.now(pytz.timezone('Asia/Shanghai')).date()
        if last_date_in_hist < today_date:
            today_timestamp = pd.Timestamp(today_date)
            new_row_series = pd.Series([spot_price], index=[today_timestamp])
            close_prices = pd.concat([close_prices, new_row_series])
        else:
            close_prices.iloc[-1] = float(spot_price)
        rsi = ta.rsi(close_prices, length=RSI_PERIOD, mamode="wilder")
        if rsi is None or rsi.empty: return None
        return round(rsi.iloc[-1], 2)
    except Exception as e:
        logger.error(f"从预加载数据计算RSI时出错: {e}")
        return None


# --- 市场时间检查 ---
def is_trading_day(check_date: datetime) -> bool:
    """使用日历检查指定日期是否为交易日。"""
    return not CHINA_CALENDAR.valid_days(start_date=check_date.date(), end_date=check_date.date()).empty


def is_market_hours() -> bool:
    """检查当前是否为A股交易时间 (已包含节假日判断)。"""
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
    await update.message.reply_html(f"你好, {user.mention_html()}!\n\n这是一个A股/ETF的RSI({RSI_PERIOD})监控机器人。\n使用 /help 查看所有可用命令。")


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
- 请求间隔: <b>{REQUEST_INTERVAL_SECONDS}秒</b>
- 每日简报主开关: <b>{'开启' if ENABLE_DAILY_BRIEFING else '关闭'} ({BRIEFING_TIME_STR})</b>
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
    for rule in rules:
        rules_by_code[rule['asset_code']].append(rule)
    unique_codes = sorted(list(rules_by_code.keys()))
    has_stocks = any(c.startswith(STOCK_PREFIXES) for c in unique_codes)
    has_etfs = any(c.startswith(ETF_PREFIXES) for c in unique_codes)
    rsi_results = {}
    try:
        stock_spot_df, etf_spot_df = pd.DataFrame(), pd.DataFrame()
        if has_stocks:
            stock_spot_df = await asyncio.to_thread(ak.stock_zh_a_spot_em)
            if has_etfs: await asyncio.sleep(REQUEST_INTERVAL_SECONDS)
        if has_etfs:
            etf_spot_df = await asyncio.to_thread(ak.fund_etf_spot_em)
        all_spot_df = pd.concat([stock_spot_df, etf_spot_df])
        spot_data = {}
        if not all_spot_df.empty:
            all_spot_df.set_index('代码', inplace=True)
            spot_data = all_spot_df['最新价'].to_dict()
        for code in unique_codes:
            spot_price = spot_data.get(code)
            if spot_price is None or pd.isna(spot_price):
                rsi_results[code] = "获取价格失败"
                continue
            await asyncio.sleep(REQUEST_INTERVAL_SECONDS)
            hist_df = await get_history_data(code)
            if hist_df is None:
                rsi_results[code] = "获取历史失败"
                continue
            rsi_value = calculate_rsi_with_spot_price(hist_df, spot_price)
            rsi_results[code] = f"{rsi_value:.2f}" if isinstance(rsi_value, float) else "计算失败"
    except Exception as e:
        logger.error(f"执行 /check 命令时出错: {e}")
        await sent_message.edit_text("查询时发生错误，请稍后重试。")
        return
    message = "<b>📈 最新RSI值查询结果:</b>\n\n"
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
        sent_message = await update.message.reply_text(f"正在验证代码 {asset_code}...")
        asset_name = await get_asset_name_with_cache(asset_code, context)
        if "未知资产" in asset_name:
            await sent_message.edit_text(f"❌ 错误：无法找到代码 {asset_code} 的信息，请检查代码是否正确。")
            return
        try:
            db_execute("INSERT INTO rules (user_id, asset_code, asset_name, rsi_min, rsi_max) VALUES (?, ?, ?, ?, ?)", (user_id, asset_code, asset_name, rsi_min, rsi_max))
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
    has_stocks = any(c.startswith(STOCK_PREFIXES) for c in all_codes)
    has_etfs = any(c.startswith(ETF_PREFIXES) for c in all_codes)
    today_str = datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d')
    if bot_data.get(KEY_CACHE_DATE) != today_str:
        logger.info(f"日期变更或首次运行，清空并重建 {today_str} 的历史数据缓存。")
        bot_data[KEY_HIST_CACHE] = {}
        bot_data[KEY_CACHE_DATE] = today_str
    hist_data_cache = bot_data.get(KEY_HIST_CACHE, {})
    codes_to_fetch_hist = [code for code in all_codes if code not in hist_data_cache]
    if codes_to_fetch_hist:
        logger.info(f"需要为 {len(codes_to_fetch_hist)} 个新资产顺序获取历史数据...")
        for code in codes_to_fetch_hist:
            logger.debug(f"正在获取 {code} 的历史数据...")
            data = await get_history_data(code)
            hist_data_cache[code] = data
            logger.debug(f"应用请求间隔: {REQUEST_INTERVAL_SECONDS}秒")
            await asyncio.sleep(REQUEST_INTERVAL_SECONDS)
    stock_spot_df, etf_spot_df = pd.DataFrame(), pd.DataFrame()
    try:
        if has_stocks:
            logger.info("监控列表中包含股票，获取A股实时行情...")
            stock_spot_df = await asyncio.to_thread(ak.stock_zh_a_spot_em)
            if has_etfs:
                logger.debug(f"应用请求间隔: {REQUEST_INTERVAL_SECONDS}秒")
                await asyncio.sleep(REQUEST_INTERVAL_SECONDS)
        if has_etfs:
            logger.info("监控列表中包含ETF，获取ETF实时行情...")
            etf_spot_df = await asyncio.to_thread(ak.fund_etf_spot_em)
        if bot_data.get(KEY_FAILURE_COUNT, 0) > 0: logger.info("数据获取成功，重置失败计数器。")
        bot_data[KEY_FAILURE_COUNT] = 0
        bot_data[KEY_FAILURE_SENT] = False
    except Exception as e:
        bot_data[KEY_FAILURE_COUNT] = bot_data.get(KEY_FAILURE_COUNT, 0) + 1
        count = bot_data[KEY_FAILURE_COUNT]
        logger.error(f"检查任务中获取实时行情失败 (连续第 {count} 次): {e}")
        if count >= FETCH_FAILURE_THRESHOLD and not bot_data.get(KEY_FAILURE_SENT):
            admin_message = (f"🚨 **机器人警报** 🚨\n\n数据获取连续失败已达到 **{count}** 次，超过阈值 ({FETCH_FAILURE_THRESHOLD})。\n\n"
                             f"请检查机器人日志和网络连接。\n\n最后一次错误: `{e}`")
            try:
                await context.bot.send_message(chat_id=ADMIN_USER_ID, text=admin_message, parse_mode=ParseMode.MARKDOWN)
                logger.warning(f"已向管理员发送数据获取失败的警报通知。")
                bot_data[KEY_FAILURE_SENT] = True
            except Exception as notify_e:
                logger.error(f"发送失败警报给管理员时出错: {notify_e}")
        return
    all_spot_df = pd.concat([stock_spot_df, etf_spot_df])
    if all_spot_df.empty:
        logger.warning("未能获取到任何有效的实时行情数据。")
        return
    all_spot_df.set_index('代码', inplace=True)
    spot_data = all_spot_df['最新价'].to_dict()
    for rule in active_rules:
        asset_code = rule['asset_code']
        hist_df = hist_data_cache.get(asset_code)
        spot_price = spot_data.get(asset_code)
        if hist_df is None or spot_price is None or pd.isna(spot_price): continue
        current_rsi = calculate_rsi_with_spot_price(hist_df, spot_price)
        if current_rsi is None: continue
        logger.debug(f"检查: {rule['asset_name']}({asset_code}) | RSI({RSI_PERIOD}): {current_rsi}")
        is_triggered = rule['rsi_min'] <= current_rsi <= rule['rsi_max']
        last_notified_rsi_in_range = rule['rsi_min'] <= rule['last_notified_rsi'] <= rule['rsi_max']
        if is_triggered and rule['notification_count'] < MAX_NOTIFICATIONS_PER_TRIGGER:
            message = (f"🎯 <b>RSI 警报 ({rule['notification_count'] + 1}/{MAX_NOTIFICATIONS_PER_TRIGGER})</b> 🎯\n\n"
                       f"<b>{rule['asset_name']} ({rule['asset_code']})</b>\n\n"
                       f"当前 RSI({RSI_PERIOD}): <b>{current_rsi:.2f}</b>\n已进入目标区间: <code>{rule['rsi_min']} - {rule['rsi_max']}</code>")
            try:
                await context.bot.send_message(chat_id=rule['user_id'], text=message, parse_mode=ParseMode.HTML)
                logger.info(f"已发送通知: {asset_code} | 用户: {rule['user_id']} | (第 {rule['notification_count'] + 1} 次)")
                db_execute("UPDATE rules SET last_notified_rsi = ?, notification_count = notification_count + 1 WHERE id = ?", (current_rsi, rule['id']))
            except Exception as e:
                logger.error(f"向用户 {rule['user_id']} 发送通知失败: {e}")
        elif not is_triggered and last_notified_rsi_in_range:
             logger.info(f"离开区间: {asset_code} | 重置通知计数器。")
             db_execute("UPDATE rules SET last_notified_rsi = ?, notification_count = 0 WHERE id = ?", (current_rsi, rule['id']))
        elif is_triggered:
            db_execute("UPDATE rules SET last_notified_rsi = ? WHERE id = ?", (current_rsi, rule['id']))


async def daily_briefing_job(context: ContextTypes.DEFAULT_TYPE):
    if not ENABLE_DAILY_BRIEFING:
        logger.debug("每日简报主开关为关闭状态，跳过任务。")
        return
    tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(tz)
    if not is_trading_day(now):
        logger.info(f"今天 ({now.strftime('%Y-%m-%d')}) 非交易日，跳过每日简报。")
        return
    logger.info("开始执行每日收盘RSI简报任务...")
    enabled_users_rows = db_execute("SELECT user_id FROM whitelist WHERE daily_briefing_enabled = 1", fetchall=True)
    if not enabled_users_rows:
        logger.info("没有用户开启每日简报，无需发送。")
        return
    enabled_user_ids = {row['user_id'] for row in enabled_users_rows}
    all_briefing_rules = db_execute("SELECT * FROM rules WHERE is_active = 1 AND user_id IN ({})".format(','.join('?' for _ in enabled_user_ids)), tuple(enabled_user_ids), fetchall=True)
    if not all_briefing_rules:
        logger.info("开启了简报的用户没有任何激活的规则，无需发送。")
        return
    rules_by_user = defaultdict(list)
    for rule in all_briefing_rules:
        rules_by_user[rule['user_id']].append(rule)
    all_unique_codes = sorted(list({rule['asset_code'] for rule in all_briefing_rules}))
    has_stocks = any(c.startswith(STOCK_PREFIXES) for c in all_unique_codes)
    has_etfs = any(c.startswith(ETF_PREFIXES) for c in all_unique_codes)
    rsi_results: Dict[str, Union[str, float]] = {}
    try:
        stock_spot_df, etf_spot_df = pd.DataFrame(), pd.DataFrame()
        if has_stocks:
            stock_spot_df = await asyncio.to_thread(ak.stock_zh_a_spot_em)
            if has_etfs: await asyncio.sleep(REQUEST_INTERVAL_SECONDS)
        if has_etfs:
            etf_spot_df = await asyncio.to_thread(ak.fund_etf_spot_em)
        all_spot_df = pd.concat([stock_spot_df, etf_spot_df])
        spot_data = {}
        if not all_spot_df.empty:
            all_spot_df.set_index('代码', inplace=True)
            spot_data = all_spot_df.get('收盘', all_spot_df['最新价']).to_dict()
        for code in all_unique_codes:
            spot_price = spot_data.get(code)
            if spot_price is None or pd.isna(spot_price):
                rsi_results[code] = "N/A"
                continue
            await asyncio.sleep(REQUEST_INTERVAL_SECONDS)
            hist_df = await get_history_data(code)
            if hist_df is None:
                rsi_results[code] = "N/A"
                continue
            rsi_value = calculate_rsi_with_spot_price(hist_df, spot_price)
            rsi_results[code] = rsi_value if isinstance(rsi_value, float) else "N/A"
    except Exception as e:
        logger.error(f"执行每日简报任务时获取数据失败: {e}")
        return
    today_str_display = now.strftime('%Y年%m月%d日')
    for user_id, user_rules in rules_by_user.items():
        if not user_rules: continue
        user_rules_by_code = defaultdict(list)
        for rule in user_rules:
            user_rules_by_code[rule['asset_code']].append(rule)
        message = f"📰 <b>收盘RSI简报 ({today_str_display})</b>\n\n"
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


# --- 启动与初始化 ---
async def post_init(application: Application):
    """在机器人启动后设置自定义命令并初始化bot_data。"""
    commands = [
        BotCommand("start", "开始使用机器人"),
        BotCommand("help", "获取帮助信息"),
        BotCommand("check", "立即查询当前RSI"),
        BotCommand("briefing", "开关每日简报"),
        BotCommand("add", "添加监控: CODE min max"),
        BotCommand("del", "删除监控: ID"),
        BotCommand("list", "查看我的监控"),
        BotCommand("on", "开启监控: ID"),
        BotCommand("off", "关闭监控: ID"),
    ]
    await application.bot.set_my_commands(commands)
    bot_data = application.bot_data
    bot_data[KEY_HIST_CACHE] = {}
    bot_data[KEY_CACHE_DATE] = None
    bot_data[KEY_FAILURE_COUNT] = 0
    bot_data[KEY_FAILURE_SENT] = False
    name_cache = {}
    all_rules = db_execute("SELECT asset_code, asset_name FROM rules", fetchall=True)
    if all_rules:
        for rule in all_rules:
            if rule['asset_code'] and rule['asset_name']:
                name_cache[rule['asset_code']] = rule['asset_name']
        logger.info(f"从数据库预加载了 {len(name_cache)} 个资产名称到缓存。")
    bot_data[KEY_NAME_CACHE] = name_cache
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
    logger.info(f"最大通知次数/次: {MAX_NOTIFICATIONS_PER_TRIGGER}")
    logger.info(f"检查间隔: {CHECK_INTERVAL_SECONDS}秒")
    logger.info(f"数据库文件: {DB_FILE}")
    logger.info(f"最大随机延迟: {RANDOM_DELAY_MAX_SECONDS}秒")
    logger.info(f"失败通知阈值: {FETCH_FAILURE_THRESHOLD}次")
    logger.info(f"请求间隔: {REQUEST_INTERVAL_SECONDS}秒")
    logger.info(f"每日简报主开关: {'开启' if ENABLE_DAILY_BRIEFING else '关闭'}")
    if ENABLE_DAILY_BRIEFING:
        logger.info(f"每日简报发送时间: {BRIEFING_TIME_STR} (上海时间)")
    logger.info("--------------------")
    db_init()
    application = Application.builder().token(TELEGRAM_TOKEN).post_init(post_init).build()
    application.add_error_handler(error_handler)
    handlers = [
        CommandHandler("start", start_command),
        CommandHandler("help", help_command),
        CommandHandler("check", check_rsi_command),
        CommandHandler("briefing", briefing_command),
        CommandHandler("add", add_rule_command),
        CommandHandler("list", list_rules_command),
        CommandHandler("del", delete_rule_command),
        CommandHandler("on", toggle_rule_status_command),
        CommandHandler("off", toggle_rule_status_command),
        CommandHandler("add_w", add_whitelist_command),
        CommandHandler("del_w", del_whitelist_command),
        CommandHandler("list_w", list_whitelist_command)
    ]
    application.add_handlers(handlers)
    job_queue = application.job_queue
    job_queue.run_repeating(check_rules_job, interval=CHECK_INTERVAL_SECONDS, first=10)
    if ENABLE_DAILY_BRIEFING:
        try:
            hour, minute = map(int, BRIEFING_TIME_STR.split(':'))
            tz_shanghai = pytz.timezone('Asia/Shanghai')
            briefing_time = time(hour, minute, tzinfo=tz_shanghai)
            job_queue.run_daily(daily_briefing_job, time=briefing_time, name="daily_briefing")
            logger.info(f"已成功注册每日简报任务，将于每天 {BRIEFING_TIME_STR} (上海时间) 执行。")
        except (ValueError, IndexError):
            logger.error(f"每日简报时间格式错误 ('{BRIEFING_TIME_STR}')，应为 HH:MM 格式。每日简报功能未开启。")
    logger.info("机器人正在启动...")
    application.run_polling()


if __name__ == '__main__':
    main()