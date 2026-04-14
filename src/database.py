# -*- coding: utf-8 -*-

import logging
import sqlite3
import threading
from typing import Optional, List

from .config import DB_FILE, ADMIN_USER_ID

logger = logging.getLogger(__name__)

# --- 持久连接 + 线程锁 ---
_conn: Optional[sqlite3.Connection] = None
_lock = threading.Lock()


def _get_connection() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        _conn.row_factory = sqlite3.Row
    return _conn


def db_init():
    with _lock:
        conn = _get_connection()
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
    with _lock:
        try:
            conn = _get_connection()
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            if fetchone:
                return cursor.fetchone()
            if fetchall:
                return cursor.fetchall()
            return None
        except sqlite3.Error as e:
            logger.error(f"数据库操作失败: {e} | query={query}")
            if not swallow_errors:
                raise
            return None


# --- 白名单操作 ---
def is_whitelisted(user_id: int) -> bool:
    return db_execute("SELECT 1 FROM whitelist WHERE user_id = ?", (user_id,), fetchone=True) is not None


def add_to_whitelist(user_id: int):
    db_execute("INSERT OR IGNORE INTO whitelist (user_id) VALUES (?)", (user_id,))


def remove_from_whitelist(user_id: int):
    db_execute("DELETE FROM whitelist WHERE user_id = ?", (user_id,))


def get_whitelist() -> Optional[List]:
    return db_execute("SELECT * FROM whitelist", fetchall=True)
