# -*- coding: utf-8 -*-

import pandas as pd
import pytest

from src.data_fetcher import calculate_rsi_exact


class TestCalculateRsiExact:
    """测试 RSI 精确计算函数。"""

    def test_normal_calculation(self):
        """正常价格序列应返回合理 RSI 值。"""
        prices = pd.Series([44.0, 44.34, 44.09, 43.61, 44.33, 44.83, 45.10, 45.42, 45.84])
        rsi = calculate_rsi_exact(prices, period=6)
        assert rsi is not None
        assert 0 <= rsi <= 100

    def test_uptrend_rsi_high(self):
        """持续上涨序列 RSI 应接近 100。"""
        prices = pd.Series([10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0])
        rsi = calculate_rsi_exact(prices, period=6)
        assert rsi is not None
        assert rsi > 90

    def test_all_gains_returns_100(self):
        """全部上涨、无下跌时 RSI 应为 100.0（除零保护）。"""
        prices = pd.Series([float(i) for i in range(1, 20)])
        rsi = calculate_rsi_exact(prices, period=6)
        assert rsi == 100.0

    def test_downtrend_rsi_low(self):
        """持续下跌序列 RSI 应较低。"""
        prices = pd.Series([20.0, 19.0, 18.0, 17.0, 16.0, 15.0, 14.0, 13.0, 12.0, 11.0])
        rsi = calculate_rsi_exact(prices, period=6)
        assert rsi is not None
        assert rsi < 15

    def test_flat_prices(self):
        """价格不变时 RSI 应为 None 或有限值（ewm 初始窗口可能导致 NaN）。"""
        prices = pd.Series([50.0] * 20)
        rsi = calculate_rsi_exact(prices, period=6)
        # 完全平盘：gain=0, loss=0, avg_loss=0 → 应返回 100.0（除零保护分支）
        assert rsi == 100.0

    def test_insufficient_data_returns_none(self):
        """数据量不足（< period+1）时应返回 None。"""
        prices = pd.Series([10.0, 11.0, 12.0])
        rsi = calculate_rsi_exact(prices, period=6)
        assert rsi is None

    def test_exact_minimum_data(self):
        """恰好 period+1 个数据点应能计算。"""
        prices = pd.Series([10.0, 11.0, 10.5, 11.5, 10.0, 12.0, 11.0])  # 7 points, period=6
        rsi = calculate_rsi_exact(prices, period=6)
        assert rsi is not None
        assert 0 <= rsi <= 100

    def test_different_periods(self):
        """不同周期参数应产生不同结果。"""
        prices = pd.Series([10, 11, 12, 11, 13, 14, 12, 15, 14, 16, 15, 17, 16, 18, 17, 19.0])
        rsi_6 = calculate_rsi_exact(prices, period=6)
        rsi_14 = calculate_rsi_exact(prices, period=14)
        assert rsi_6 is not None
        assert rsi_14 is not None
        # 两个周期计算结果应不同
        assert rsi_6 != rsi_14

    def test_result_is_rounded(self):
        """结果应保留 2 位小数。"""
        prices = pd.Series([10, 11, 12, 11, 13, 14, 12, 15, 14, 16.0])
        rsi = calculate_rsi_exact(prices, period=6)
        assert rsi is not None
        # 检查小数位数
        str_val = str(rsi)
        if '.' in str_val:
            decimals = len(str_val.split('.')[1])
            assert decimals <= 2

    def test_nan_in_prices(self):
        """包含 NaN 的价格序列不应崩溃。"""
        prices = pd.Series([10, 11, float('nan'), 13, 14, 15, 16, 17.0])
        rsi = calculate_rsi_exact(prices, period=6)
        # 不崩溃即可，结果可能是 None 或数值
        assert rsi is None or (0 <= rsi <= 100)

    def test_empty_series_returns_none(self):
        """空序列应返回 None。"""
        rsi = calculate_rsi_exact(pd.Series(dtype=float), period=6)
        assert rsi is None

    def test_single_value_returns_none(self):
        """单个值应返回 None。"""
        rsi = calculate_rsi_exact(pd.Series([42.0]), period=6)
        assert rsi is None
