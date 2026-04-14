# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from unittest.mock import patch

import pandas as pd
import pytz
import pytest

from src.data_fetcher import get_prices_for_rsi, _adjust_spot_price


class TestAdjustSpotPrice:
    """测试复权价格调整。"""

    def test_no_adjust_mode(self):
        """USE_ADJUST=False 时直接返回原始价格。"""
        df = pd.DataFrame({'收盘': [10.0]})
        df.attrs['adjust_factor'] = 1.5
        with patch('src.data_fetcher.USE_ADJUST', False):
            result = _adjust_spot_price(df, 20.0)
        assert result == 20.0

    def test_adjust_with_factor(self):
        """有复权因子时正确相乘。"""
        df = pd.DataFrame({'收盘': [10.0]})
        df.attrs['adjust_factor'] = 1.5
        with patch('src.data_fetcher.USE_ADJUST', True):
            result = _adjust_spot_price(df, 20.0)
        assert result == 30.0  # 20.0 * 1.5

    def test_adjust_factor_none(self):
        """复权因子为 None 时返回原始价格。"""
        df = pd.DataFrame({'收盘': [10.0]})
        # 不设置 adjust_factor
        with patch('src.data_fetcher.USE_ADJUST', True):
            result = _adjust_spot_price(df, 20.0)
        assert result == 20.0

    def test_adjust_factor_zero(self):
        """复权因子为 0 时返回原始价格。"""
        df = pd.DataFrame({'收盘': [10.0]})
        df.attrs['adjust_factor'] = 0
        with patch('src.data_fetcher.USE_ADJUST', True):
            result = _adjust_spot_price(df, 20.0)
        assert result == 20.0

    def test_adjust_factor_very_small(self):
        """很小但非零的复权因子应正常计算。"""
        df = pd.DataFrame({'收盘': [10.0]})
        df.attrs['adjust_factor'] = 0.001
        with patch('src.data_fetcher.USE_ADJUST', True):
            result = _adjust_spot_price(df, 20.0)
        assert abs(result - 0.02) < 1e-10


class TestGetPricesForRsi:
    """测试 RSI 价格序列构建。"""

    def _make_hist_df(self, dates, closes, adjust_factor=None):
        """构造带日期索引的历史 DataFrame。"""
        df = pd.DataFrame({'收盘': closes}, index=pd.to_datetime(dates))
        if adjust_factor is not None:
            df.attrs['adjust_factor'] = adjust_factor
        return df

    def test_none_returns_none(self):
        assert get_prices_for_rsi(None, 10.0) is None

    def test_empty_df_returns_none(self):
        df = pd.DataFrame()
        assert get_prices_for_rsi(df, 10.0) is None

    def test_missing_close_column(self):
        df = pd.DataFrame({'开盘': [10.0]}, index=pd.to_datetime(['2024-01-01']))
        assert get_prices_for_rsi(df, 10.0) is None

    def test_appends_today_price(self):
        """历史数据最后日期早于今天时，应追加今日价格。"""
        tz = pytz.timezone('Asia/Shanghai')
        yesterday = (datetime.now(tz) - timedelta(days=1)).strftime('%Y-%m-%d')
        df = self._make_hist_df(
            [yesterday],
            [10.0],
        )
        with patch('src.data_fetcher.USE_ADJUST', False):
            result = get_prices_for_rsi(df, 15.0)
        assert result is not None
        assert len(result) == 2
        assert result.iloc[-1] == 15.0

    def test_replaces_today_price(self):
        """历史数据包含今日时，应替换最后一个值。"""
        tz = pytz.timezone('Asia/Shanghai')
        today = datetime.now(tz).strftime('%Y-%m-%d')
        df = self._make_hist_df(
            [today],
            [10.0],
        )
        with patch('src.data_fetcher.USE_ADJUST', False):
            result = get_prices_for_rsi(df, 15.0)
        assert result is not None
        assert len(result) == 1
        assert result.iloc[-1] == 15.0

    def test_preserves_history(self):
        """历史数据不应被修改。"""
        tz = pytz.timezone('Asia/Shanghai')
        yesterday = (datetime.now(tz) - timedelta(days=2)).strftime('%Y-%m-%d')
        day_before = (datetime.now(tz) - timedelta(days=3)).strftime('%Y-%m-%d')
        df = self._make_hist_df(
            [day_before, yesterday],
            [10.0, 12.0],
        )
        with patch('src.data_fetcher.USE_ADJUST', False):
            result = get_prices_for_rsi(df, 15.0)
        assert result is not None
        assert result.iloc[0] == 10.0
        assert result.iloc[1] == 12.0
