# -*- coding: utf-8 -*-

import logging
import sqlite3
import pandas as pd
import pandas_ta as ta
import akshare as ak
from datetime import datetime, timedelta
import pytz
import asyncio
from functools import wraps
from typing import Union
import os
import random
import pandas_market_calendars as mcal

from telegram import Update, BotCommand
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.constants import ParseMode

# --- 机器人配置 (从环境变量读取) ---
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
ADMIN_USER_ID_STR = os.getenv('ADMIN_USER_ID')
ADMIN_USER_ID = int(ADMIN_USER_ID_STR) if ADMIN_USER_ID_STR and ADMIN_USER_ID_STR.isdigit() else None
CHECK_INTERVAL_SECONDS = int(os.getenv('CHECK_INTERVAL_SECONDS', '60'))
DB_FILE = os.getenv('DB_FILE', 'rules.db')

# --- 监控参数配置 (从环境变量读取) ---
RSI_PERIOD = int(os.getenv('RSI_PERIOD', '6'))
HIST_FETCH_DAYS = int(os.getenv('HIST_FETCH_DAYS', '30'))
MAX_NOTIFICATIONS_PER_TRIGGER = int(os.getenv('MAX_NOTIFICATIONS_PER_TRIGGER', '1'))

# --- 高级配置 (从环境变量读取) ---
RANDOM_DELAY_MAX_SECONDS = float(os.getenv('RANDOM_DELAY_MAX_SECONDS', '0'))
FETCH_FAILURE_THRESHOLD = int(os.getenv('FETCH_FAILURE_THRESHOLD', '5'))
REQUEST_INTERVAL_SECONDS = float(os.getenv('REQUEST_INTERVAL_SECONDS', '1.0'))

# --- 日志配置 ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
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

# 在全局创建一个交易所日历对象，避免重复创建
CHINA_CALENDAR = mcal.get_calendar('XSHG') # 使用上海证券交易所日历


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
        cursor.execute('CREATE TABLE IF NOT EXISTS whitelist (user_id INTEGER PRIMARY KEY)')
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
            if not target.empty:
                name = target.iloc[0]
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
            return await asyncio.to_thread(ak.stock_zh_a_hist, symbol=asset_code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
        elif asset_code.startswith(ETF_PREFIXES):
            return await asyncio.to_thread(ak.fund_etf_hist_em, symbol=asset_code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
        return None
    except Exception as e:
        logger.error(f"获取 {asset_code} 历史数据失败: {e}")
        return None


def calculate_rsi_with_spot_price(hist_df: pd.DataFrame, spot_price: float) -> Union[float, None]:
    """使用已获取的历史数据和实时价格计算RSI。"""
    try:
        if hist_df is None or hist_df.empty: return None
        price_col = '收盘'
        if price_col not in hist_df.columns: return None
        
        close_prices = hist_df[price_col].copy()
        close_prices.iloc[-1] = float(spot_price)
        rsi = ta.rsi(close_prices, length=RSI_PERIOD)
        
        if rsi is None or rsi.empty: return None
        return round(rsi.iloc[-1], 2)
    except Exception as e:
        logger.error(f"从预加载数据计算RSI时出错: {e}")
        return None


# --- 市场时间检查 ---
def is_market_hours() -> bool:
    """检查当前是否为A股交易时间 (已包含节假日判断)。"""
    tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(tz)
    
    # 1. 快速检查时间范围和星期
    if now.weekday() >= 5: return False
    time_now = now.time()
    is_in_time = (datetime.strptime("09:30", "%H:%M").time() <= time_now <= datetime.strptime("11:30", "%H:%M").time()) or \
                 (datetime.strptime("13:00", "%H:%M").time() <= time_now <= datetime.strptime("15:00", "%H:%M").time())
    if not is_in_time: return False

    # 2. ★★★ 关键修复: 精确检查当天是否为交易日 ★★★
    today_str = now.strftime('%Y-%m-%d')
    return not CHINA_CALENDAR.valid_days(start_date=today_str, end_date=today_str).empty


# --- Telegram 命令处理 ---
@whitelisted_only
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await update.message.reply_html(f"你好, {user.mention_html()}!\n\n这是一个A股/ETF的RSI({RSI_PERIOD})监控机器人。\n使用 /help 查看所有可用命令。")


@whitelisted_only
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = f"""
<b>可用命令:</b>

<b>规则管理</b>
/add <code>CODE</code> <i>min_rsi</i> <i>max_rsi</i>
  (示例: <code>/add 510300 20 30</code>)
/del <code>RULE_ID</code>
  (示例: <code>/del 3</code>)
/list - 查看所有监控规则
/on <code>RULE_ID</code> - 开启一条规则
/off <code>RULE_ID</code> - 关闭一条规则

<b>白名单管理 (仅限管理员)</b>
/add_w <code>USER_ID</code> - 添加用户到白名单
/del_w <code>USER_ID</code> - 从白名单移除用户
/list_w - 查看白名单列表

<b>当前机器人配置:</b>
- RSI 周期: <b>{RSI_PERIOD}</b>
- 最大通知次数/次: <b>{MAX_NOTIFICATIONS_PER_TRIGGER}</b>
- 请求间隔: <b>{REQUEST_INTERVAL_SECONDS}秒</b>
    """
    await update.message.reply_html(help_text)


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
            # ★★★ 优化: 直接使用 asset_name，不再需要 true_name ★★★
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
    users = get_whitelist()
    if not users: await update.message.reply_text("白名单中没有任何用户。"); return
    message = "<b>白名单用户列表:</b>\n\n"
    for user in users:
        is_admin_text = " (管理员)" if user['user_id'] == ADMIN_USER_ID else ""
        message += f"- <code>{user['user_id']}</code>{is_admin_text}\n"
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
            hist_data_cache[code] = data if data is not None and not data.empty else None
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


# --- 启动与初始化 ---
async def post_init(application: Application):
    """在机器人启动后设置自定义命令并初始化bot_data。"""
    commands = [
        BotCommand("start", "开始使用机器人"), BotCommand("help", "获取帮助信息"),
        BotCommand("add", "添加监控: CODE min max"), BotCommand("del", "删除监控: ID"),
        BotCommand("list", "查看我的监控"), BotCommand("on", "开启监控: ID"), BotCommand("off", "关闭监控: ID"),
    ]
    await application.bot.set_my_commands(commands)
    application.bot_data[KEY_HIST_CACHE] = {}
    application.bot_data[KEY_CACHE_DATE] = None
    application.bot_data[KEY_FAILURE_COUNT] = 0
    application.bot_data[KEY_FAILURE_SENT] = False
    name_cache = {}
    all_rules = db_execute("SELECT asset_code, asset_name FROM rules", fetchall=True)
    if all_rules:
        for rule in all_rules:
            if rule['asset_code'] and rule['asset_name']:
                name_cache[rule['asset_code']] = rule['asset_name']
        logger.info(f"从数据库预加载了 {len(name_cache)} 个资产名称到缓存。")
    application.bot_data[KEY_NAME_CACHE] = name_cache
    logger.info("Bot application data 初始化完成。")


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
    logger.info("--------------------")
    db_init()
    application = Application.builder().token(TELEGRAM_TOKEN).post_init(post_init).build()
    handlers = [
        CommandHandler("start", start_command), CommandHandler("help", help_command),
        CommandHandler("add", add_rule_command), CommandHandler("list", list_rules_command),
        CommandHandler("del", delete_rule_command), CommandHandler("on", toggle_rule_status_command),
        CommandHandler("off", toggle_rule_status_command), CommandHandler("add_w", add_whitelist_command),
        CommandHandler("del_w", del_whitelist_command), CommandHandler("list_w", list_whitelist_command)
    ]
    application.add_handlers(handlers)
    application.job_queue.run_repeating(check_rules_job, interval=CHECK_INTERVAL_SECONDS, first=10)
    logger.info("机器人正在启动...")
    application.run_polling()


if __name__ == '__main__':
    main()