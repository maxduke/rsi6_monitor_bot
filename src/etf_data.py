# -*- coding: utf-8 -*-

import logging

import httpx
import pandas as pd

logger = logging.getLogger(__name__)

SINA_NAV_URL = "https://fund.sina.com.cn/fund/api/netWorthTable"


def fetch_etf_history_sina(
    fund_code: str,
    use_adjust: bool,
    max_pages: int = 200,
    page_size: int = 5000,
) -> pd.DataFrame:
    """使用新浪净值接口获取 ETF 全量历史净值数据。"""
    rows = []
    page = 1
    value_key = "UNITACCNAV" if use_adjust else "UNITNAV"

    while page <= max_pages:
        params = {"fundcode": fund_code, "page": page, "num": page_size}
        try:
            response = httpx.get(SINA_NAV_URL, params=params, timeout=10.0)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            logger.error(f"请求新浪ETF历史净值失败: {fund_code} page={page} error={exc}")
            break

        payload = response.json()
        if payload.get("code") != 0:
            logger.warning(f"新浪ETF历史净值返回错误: {fund_code} page={page} payload={payload}")
            break
        data = payload.get("data") or []
        if not data:
            break
        rows.extend(data)
        if len(data) < page_size:
            break
        page += 1

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    if value_key not in df.columns or "ENDDATE" not in df.columns:
        logger.error(f"新浪ETF历史净值字段缺失: {fund_code} columns={list(df.columns)}")
        return pd.DataFrame()

    df = df[["ENDDATE", value_key]].copy()
    df.rename(columns={"ENDDATE": "日期", value_key: "收盘"}, inplace=True)
    df["日期"] = pd.to_datetime(df["日期"], format="%Y%m%d", errors="coerce")
    df["收盘"] = pd.to_numeric(df["收盘"], errors="coerce")
    df.dropna(subset=["日期", "收盘"], inplace=True)
    df.sort_values("日期", inplace=True)
    df.set_index("日期", inplace=True)
    return df
