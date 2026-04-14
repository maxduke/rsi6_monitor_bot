# -*- coding: utf-8 -*-

import math
import pytest

from src.jobs import _in_range


class TestInRange:
    """测试 RSI 区间判断函数。"""

    def test_value_in_range(self):
        assert _in_range(50.0, 30.0, 70.0) is True

    def test_value_at_min_boundary(self):
        assert _in_range(30.0, 30.0, 70.0) is True

    def test_value_at_max_boundary(self):
        assert _in_range(70.0, 30.0, 70.0) is True

    def test_value_below_range(self):
        assert _in_range(20.0, 30.0, 70.0) is False

    def test_value_above_range(self):
        assert _in_range(80.0, 30.0, 70.0) is False

    def test_none_returns_false(self):
        assert _in_range(None, 30.0, 70.0) is False

    def test_nan_returns_false(self):
        assert _in_range(float('nan'), 30.0, 70.0) is False

    def test_integer_value(self):
        assert _in_range(50, 30.0, 70.0) is True

    def test_numeric_string_coerced(self):
        """数值字符串会被 float() 转换，属于合法输入。"""
        assert _in_range("50", 30.0, 70.0) is True

    def test_non_numeric_string_returns_false(self):
        """非数值字符串应返回 False。"""
        assert _in_range("abc", 30.0, 70.0) is False

    def test_zero_value(self):
        assert _in_range(0.0, 0.0, 10.0) is True

    def test_zero_value_outside(self):
        assert _in_range(0.0, 1.0, 10.0) is False

    def test_rsi_100(self):
        assert _in_range(100.0, 90.0, 100.0) is True

    def test_tight_range(self):
        """极小区间。"""
        assert _in_range(50.01, 50.0, 50.02) is True
        assert _in_range(49.99, 50.0, 50.02) is False

    def test_inf_returns_false(self):
        """无穷大应被视为超出范围。"""
        assert _in_range(float('inf'), 0.0, 100.0) is False
        assert _in_range(float('-inf'), 0.0, 100.0) is False
