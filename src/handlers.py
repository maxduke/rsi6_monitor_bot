# -*- coding: utf-8 -*-

import logging
import sqlite3
from collections import defaultdict
from functools import wraps

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from .config import (
    ADMIN_USER_ID,
    ENABLE_DAILY_BRIEFING,
    BRIEFING_TIMES_STR,
    KEY_CACHE_DATE,
    KEY_HIST_CACHE,
    MAX_NOTIFICATIONS_PER_TRIGGER,
    REQUEST_INTERVAL_SECONDS,
    RSI_PERIOD,
    USE_ADJUST,
)
from .database import db_execute, is_whitelisted, add_to_whitelist, remove_from_whitelist
from .data_fetcher import (
    _fetch_all_spot_data,
    _fetch_single_realtime_price,
    calculate_rsi,
    ensure_daily_history_cache,
    get_asset_name_with_cache,
    get_history_data,
    get_prices_for_rsi,
)

import asyncio

logger = logging.getLogger(__name__)


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


# --- 命令处理器 ---

@whitelisted_only
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    config_info = f"复权: {'是' if USE_ADJUST else '否'}"
    await update.message.reply_html(
        f"你好, {user.mention_html()}!\n\n"
        f"这是一个A股/ETF的RSI({RSI_PERIOD})监控机器人。\n"
        f"({config_info})\n"
        f"使用 /help 查看所有可用命令。"
    )


@whitelisted_only
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    briefing_status_row = db_execute(
        "SELECT daily_briefing_enabled FROM whitelist WHERE user_id = ?", (user_id,), fetchone=True
    )
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
    for rule in rules:
        rules_by_code[rule['asset_code']].append(rule)
    unique_codes = sorted(list(rules_by_code.keys()))

    rsi_results = {}

    spot_data, success = await _fetch_all_spot_data(context, unique_codes)
    if not success:
        await sent_message.edit_text("获取实时价格失败，请稍后重试。")
        return

    cache = context.bot_data.get(KEY_HIST_CACHE, {})

    for code in unique_codes:
        spot_price = spot_data.get(code)
        if spot_price is None:
            rsi_results[code] = "获取价格失败"
            continue

        hist_df = cache.get(code)
        if hist_df is None:
            logger.info(f"/check: 缓存未命中，为 {code} 单独获取历史数据。")
            await asyncio.sleep(REQUEST_INTERVAL_SECONDS)
            hist_df = await get_history_data(code, HIST_FETCH_DAYS)
            if hist_df is not None and not hist_df.empty:
                cache[code] = hist_df

        # Bug3: 统一检查 None 和 empty
        if hist_df is None or hist_df.empty:
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
        status_row = db_execute(
            "SELECT daily_briefing_enabled FROM whitelist WHERE user_id = ?", (user_id,), fetchone=True
        )
        status = "开启" if status_row and status_row['daily_briefing_enabled'] else "关闭"
        await update.message.reply_html(
            f"您的每日简报当前为 <b>{status}</b> 状态。\n\n"
            f"使用 <code>/briefing on</code> 或 <code>/briefing off</code> 来进行设置。"
        )
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
        rsi_min = float(rsi_min_str)
        rsi_max = float(rsi_max_str)
        if rsi_min >= rsi_max:
            await update.message.reply_text("错误：RSI最小值必须小于最大值。")
            return
        if rsi_min < 0 or rsi_max > 100:
            await update.message.reply_text("错误：RSI区间必须在 0 到 100 之间。")
            return
        sent_message = await update.message.reply_text(f"正在验证代码 {asset_code}...")

        # Bug1: 使用 is None 而非 not price
        price = await _fetch_single_realtime_price(asset_code)
        if price is None:
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
        if sent_message:
            await sent_message.edit_text(error_message)
        else:
            await update.message.reply_text(error_message)


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
            message += (
                f"{status_icon} <b>ID: {rule['id']}</b>\n"
                f"  - 名称: {rule['asset_name']} ({rule['asset_code']})\n"
                f"  - RSI 范围: {rule['rsi_min']} - {rule['rsi_max']}\n{notif_text}"
                f"  - 状态: {'开启' if rule['is_active'] else '关闭'}\n\n"
            )
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
        if new_status == 1:
            db_execute(
                "UPDATE rules SET is_active = 1, notification_count = 0, last_notified_rsi = 0 WHERE id = ? AND user_id = ?",
                (rule_id, user_id),
            )
        else:
            db_execute("UPDATE rules SET is_active = 0 WHERE id = ? AND user_id = ?", (rule_id, user_id))
        status_text = "开启" if new_status else "关闭"
        await update.message.reply_text(
            f"✅ 规则 ID: {rule_id} 已被设置为 **{status_text}** 状态。",
            parse_mode=ParseMode.MARKDOWN,
        )
    except (ValueError, IndexError):
        await update.message.reply_text(f"命令格式错误。\n正确格式: {command} <rule_id>")
    except Exception as e:
        logger.error(f"切换规则状态时出错: {e}")
        await update.message.reply_text("切换规则状态时发生错误。")


@admin_only
async def add_whitelist_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        _, user_id_str = update.message.text.split()
        user_id_to_add = int(user_id_str)
        add_to_whitelist(user_id_to_add)
        await update.message.reply_text(f"✅ 用户 {user_id_to_add} 已添加到白名单。")
    except (ValueError, IndexError):
        await update.message.reply_text("命令格式错误。\n正确格式: /add_w <user_id>")


@admin_only
async def del_whitelist_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        _, user_id_str = update.message.text.split()
        user_id_to_del = int(user_id_str)
        if user_id_to_del == ADMIN_USER_ID:
            await update.message.reply_text("❌ 不能将管理员从白名单中删除。")
            return
        remove_from_whitelist(user_id_to_del)
        await update.message.reply_text(f"✅ 用户 {user_id_to_del} 已从白名单中移除。")
    except (ValueError, IndexError):
        await update.message.reply_text("命令格式错误。\n正确格式: /del_w <user_id>")


@admin_only
async def list_whitelist_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    users = db_execute("SELECT * FROM whitelist", fetchall=True)
    if not users:
        await update.message.reply_text("白名单中没有任何用户。")
        return
    message = "<b>白名单用户列表:</b>\n\n"
    for user in users:
        is_admin_text = " (管理员)" if user['user_id'] == ADMIN_USER_ID else ""
        briefing_enabled_text = " (简报:开)" if user['daily_briefing_enabled'] else ""
        message += f"- <code>{user['user_id']}</code>{is_admin_text}{briefing_enabled_text}\n"
    await update.message.reply_html(message)


@admin_only
async def refresh_cache_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.bot_data[KEY_HIST_CACHE] = {}
    context.bot_data[KEY_CACHE_DATE] = None
    await update.message.reply_text("✅ 历史数据缓存已清空，下次检查时将重新获取。")
