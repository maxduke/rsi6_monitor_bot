# -*- coding: utf-8 -*-

import pandas as pd
import pytest

from src.utils import normalize_hist_df, get_sina_symbol


class TestNormalizeHistDf:
    """测试历史数据列名标准化。"""

    def test_none_returns_none(self):
        assert normalize_hist_df(None) is None

    def test_empty_returns_empty(self):
        df = pd.DataFrame()
        result = normalize_hist_df(df)
        assert result is not None
        assert result.empty

    def test_english_to_chinese(self):
        """英文列名应被重命名为中文。"""
        df = pd.DataFrame({
            'date': ['2024-01-01'],
            'open': [10.0],
            'high': [11.0],
            'low': [9.0],
            'close': [10.5],
            'volume': [1000],
        })
        result = normalize_hist_df(df)
        assert '日期' in result.columns
        assert '开盘' in result.columns
        assert '最高' in result.columns
        assert '最低' in result.columns
        assert '收盘' in result.columns
        assert '成交量' in result.columns

    def test_chinese_columns_unchanged(self):
        """已经是中文列名的 DataFrame 不应被修改。"""
        df = pd.DataFrame({
            '日期': ['2024-01-01'],
            '收盘': [10.5],
        })
        result = normalize_hist_df(df)
        assert '日期' in result.columns
        assert '收盘' in result.columns

    def test_date_converted_to_datetime(self):
        """日期列应被转换为 datetime 类型。"""
        df = pd.DataFrame({
            'date': ['2024-01-01', '2024-01-02'],
            'close': [10.0, 11.0],
        })
        result = normalize_hist_df(df)
        assert pd.api.types.is_datetime64_any_dtype(result['日期'])

    def test_partial_columns(self):
        """只有部分列名匹配时，只重命名匹配的列。"""
        df = pd.DataFrame({
            'close': [10.0],
            'custom_field': [1],
        })
        result = normalize_hist_df(df)
        assert '收盘' in result.columns
        assert 'custom_field' in result.columns

    def test_data_integrity(self):
        """转换后数据值不变。"""
        df = pd.DataFrame({
            'close': [10.5, 11.0, 12.5],
        })
        result = normalize_hist_df(df)
        assert list(result['收盘']) == [10.5, 11.0, 12.5]


class TestGetSinaSymbol:
    """测试新浪代码格式转换。"""

    def test_shanghai_stock(self):
        assert get_sina_symbol('600519') == 'sh600519'

    def test_shenzhen_stock(self):
        assert get_sina_symbol('000001') == 'sz000001'

    def test_chinext_stock(self):
        """创业板 3 开头。"""
        assert get_sina_symbol('300750') == 'sz300750'

    def test_shanghai_etf(self):
        assert get_sina_symbol('510300') == 'sh510300'

    def test_shenzhen_etf(self):
        assert get_sina_symbol('159915') == 'sz159915'

    def test_beijing_stock_4(self):
        assert get_sina_symbol('430047') == 'bj430047'

    def test_beijing_stock_8(self):
        assert get_sina_symbol('830799') == 'bj830799'

    def test_sh_9_prefix(self):
        """9 开头的代码归属上海。"""
        assert get_sina_symbol('900901') == 'sh900901'

    def test_sz_2_prefix(self):
        """2 开头的代码归属深圳。"""
        assert get_sina_symbol('200001') == 'sz200001'

    def test_unknown_prefix(self):
        """未知前缀返回原值。"""
        assert get_sina_symbol('700001') == '700001'
