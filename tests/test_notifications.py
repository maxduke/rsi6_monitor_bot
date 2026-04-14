# -*- coding: utf-8 -*-

import sqlite3
import pytest

from src.jobs import _build_notification_chunks


def _make_mock_rule(asset_code, asset_name, rsi_min, rsi_max, notification_count=0):
    """创建一个模拟的 sqlite3.Row 对象。"""
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    conn.execute('''
        CREATE TABLE rules (
            asset_code TEXT, asset_name TEXT, rsi_min REAL, rsi_max REAL, notification_count INTEGER
        )
    ''')
    conn.execute(
        'INSERT INTO rules VALUES (?, ?, ?, ?, ?)',
        (asset_code, asset_name, rsi_min, rsi_max, notification_count),
    )
    row = conn.execute('SELECT * FROM rules').fetchone()
    conn.close()
    return row


class TestBuildNotificationChunks:
    """测试消息分块逻辑。"""

    def test_empty_rules(self):
        """空规则列表应返回空列表。"""
        chunks = _build_notification_chunks([])
        assert chunks == []

    def test_single_rule_one_chunk(self):
        """单条规则应在一个 chunk 中。"""
        rule = _make_mock_rule('600519', '贵州茅台', 20.0, 30.0, 0)
        chunks = _build_notification_chunks([(rule, 25.5)])
        assert len(chunks) == 1
        message, rules_in_chunk = chunks[0]
        assert '贵州茅台' in message
        assert '600519' in message
        assert '25.50' in message
        assert len(rules_in_chunk) == 1

    def test_multiple_rules_single_chunk(self):
        """多条较短规则应在同一个 chunk 中。"""
        rules = [
            (_make_mock_rule('600519', '贵州茅台', 20.0, 30.0), 25.5),
            (_make_mock_rule('000001', '平安银行', 15.0, 25.0), 18.3),
        ]
        chunks = _build_notification_chunks(rules)
        assert len(chunks) == 1
        message, rules_in_chunk = chunks[0]
        assert '贵州茅台' in message
        assert '平安银行' in message
        assert len(rules_in_chunk) == 2

    def test_chunking_on_length_limit(self):
        """超过 max_len 时应分块。"""
        rules = []
        for i in range(50):
            rules.append((
                _make_mock_rule(f'{600000+i}', f'测试资产名称很长很长很长_{i}', 10.0, 90.0),
                50.0,
            ))
        chunks = _build_notification_chunks(rules, max_len=500)
        assert len(chunks) > 1
        # 每个 chunk 的消息应不超过限制太多
        for message, _ in chunks:
            assert len(message) < 1000  # 允许一些余量

    def test_all_rules_accounted_for(self):
        """分块后所有规则应被包含。"""
        rules = []
        for i in range(20):
            rules.append((
                _make_mock_rule(f'{600000+i}', f'资产{i}', 10.0, 90.0),
                50.0,
            ))
        chunks = _build_notification_chunks(rules, max_len=500)
        total_rules = sum(len(r) for _, r in chunks)
        assert total_rules == 20

    def test_html_escaping(self):
        """资产名称中的 HTML 特殊字符应被转义。"""
        rule = _make_mock_rule('600519', '<script>alert(1)</script>', 20.0, 30.0)
        chunks = _build_notification_chunks([(rule, 25.0)])
        message, _ = chunks[0]
        assert '<script>' not in message
        assert '&lt;script&gt;' in message

    def test_none_asset_name(self):
        """asset_name 为 None 时应显示"未知资产"。"""
        rule = _make_mock_rule('600519', None, 20.0, 30.0)
        chunks = _build_notification_chunks([(rule, 25.0)])
        message, _ = chunks[0]
        assert '未知资产' in message

    def test_header_present(self):
        """每个 chunk 都应包含 header。"""
        rules = []
        for i in range(30):
            rules.append((
                _make_mock_rule(f'{600000+i}', f'长名称资产测试_{i}', 10.0, 90.0),
                50.0,
            ))
        chunks = _build_notification_chunks(rules, max_len=400)
        for message, _ in chunks:
            assert 'RSI 警报汇总' in message
