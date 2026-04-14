# -*- coding: utf-8 -*-

import pandas as pd


def normalize_hist_df(hist_df: pd.DataFrame) -> pd.DataFrame:
    """将英文列名的历史行情 DataFrame 统一为中文列名。"""
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


def get_sina_symbol(code: str) -> str:
    """转换代码为新浪接口格式。"""
    if code.startswith(('6', '5', '9')):
        return f"sh{code}"
    elif code.startswith(('0', '3', '1', '2')):
        return f"sz{code}"
    elif code.startswith(('4', '8')):
        return f"bj{code}"
    return code
