# -*- coding: utf-8 -*-

import sqlite3
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
        chunks = _build_notification_chunks([(rule, 25.5, True)])
        assert len(chunks) == 1
        message, rules_in_chunk = chunks[0]
        assert '贵州茅台' in message
        assert '600519' in message
        assert '25.50' in message
        assert len(rules_in_chunk) == 1

    def test_multiple_rules_single_chunk(self):
        """多条较短规则应在同一个 chunk 中。"""
        rules = [
            (_make_mock_rule('600519', '贵州茅台', 20.0, 30.0), 25.5, True),
            (_make_mock_rule('000001', '平安银行', 15.0, 25.0), 18.3, True),
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
                True,
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
                True,
            ))
        chunks = _build_notification_chunks(rules, max_len=500)
        total_rules = sum(len(r) for _, r in chunks)
        assert total_rules == 20

    def test_html_escaping(self):
        """资产名称中的 HTML 特殊字符应被转义。"""
        rule = _make_mock_rule('600519', '<script>alert(1)</script>', 20.0, 30.0)
        chunks = _build_notification_chunks([(rule, 25.0, True)])
        message, _ = chunks[0]
        assert '<script>' not in message
        assert '&lt;script&gt;' in message

    def test_none_asset_name(self):
        """asset_name 为 None 时应显示"未知资产"。"""
        rule = _make_mock_rule('600519', None, 20.0, 30.0)
        chunks = _build_notification_chunks([(rule, 25.0, True)])
        message, _ = chunks[0]
        assert '未知资产' in message

    def test_header_present(self):
        """每个 chunk 都应包含 header。"""
        rules = []
        for i in range(30):
            rules.append((
                _make_mock_rule(f'{600000+i}', f'长名称资产测试_{i}', 10.0, 90.0),
                50.0,
                True,
            ))
        chunks = _build_notification_chunks(rules, max_len=400)
        for message, _ in chunks:
            assert 'RSI 警报汇总' in message

    def test_capped_rule_is_shown_without_increment(self):
        """已达通知上限的规则应显示在汇总中，但标记为仅展示。"""
        rules = [
            (_make_mock_rule('600519', '贵州茅台', 20.0, 30.0, 1), 25.5, False),
            (_make_mock_rule('000001', '平安银行', 15.0, 25.0, 0), 18.3, True),
        ]
        chunks = _build_notification_chunks(rules)

        assert len(chunks) == 1
        message, rules_in_chunk = chunks[0]
        assert '贵州茅台' in message
        assert '平安银行' in message
        assert '已达上限，仅汇总展示' in message
        assert len(rules_in_chunk) == 2


def _reset_test_db(monkeypatch, tmp_path):
    """将数据库模块切换到临时 SQLite 文件。"""
    from src import database

    if database._conn is not None:
        database._conn.close()
        database._conn = None
    monkeypatch.setattr(database, "DB_FILE", str(tmp_path / "rules.db"))
    database.db_init()
    return database


class TestDailyNotificationReset:
    """测试通知计数按上海自然日重置。"""

    def test_reset_stale_notification_counts(self, monkeypatch, tmp_path):
        from src.jobs import _reset_stale_notification_counts

        database = _reset_test_db(monkeypatch, tmp_path)
        database.db_execute(
            """
            INSERT INTO rules (
                user_id, asset_code, asset_name, rsi_min, rsi_max,
                last_notified_rsi, notification_count, last_notification_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, '600519', '贵州茅台', 20.0, 30.0, 25.0, 1, '2026-05-17'),
        )
        database.db_execute(
            """
            INSERT INTO rules (
                user_id, asset_code, asset_name, rsi_min, rsi_max,
                last_notified_rsi, notification_count, last_notification_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, '000001', '平安银行', 20.0, 30.0, 25.0, 1, '2026-05-18'),
        )

        reset_count = _reset_stale_notification_counts('2026-05-18')

        assert reset_count == 1
        stale_rule = database.db_execute(
            "SELECT notification_count, last_notification_date FROM rules WHERE asset_code = ?",
            ('600519',),
            fetchone=True,
        )
        current_rule = database.db_execute(
            "SELECT notification_count, last_notification_date FROM rules WHERE asset_code = ?",
            ('000001',),
            fetchone=True,
        )
        assert stale_rule['notification_count'] == 0
        assert stale_rule['last_notification_date'] == '2026-05-17'
        assert current_rule['notification_count'] == 1
        assert current_rule['last_notification_date'] == '2026-05-18'

    def test_db_init_migrates_existing_rules_table(self, monkeypatch, tmp_path):
        import sqlite3
        from src import database

        db_file = tmp_path / "legacy.db"
        conn = sqlite3.connect(db_file)
        conn.execute(
            """
            CREATE TABLE rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                asset_code TEXT NOT NULL,
                asset_name TEXT,
                rsi_min REAL NOT NULL,
                rsi_max REAL NOT NULL,
                is_active INTEGER DEFAULT 1,
                last_notified_rsi REAL DEFAULT 0,
                notification_count INTEGER NOT NULL DEFAULT 0,
                UNIQUE(user_id, asset_code, rsi_min, rsi_max)
            )
            """
        )
        conn.commit()
        conn.close()

        if database._conn is not None:
            database._conn.close()
            database._conn = None
        monkeypatch.setattr(database, "DB_FILE", str(db_file))

        database.db_init()
        columns = database.db_execute("PRAGMA table_info(rules)", fetchall=True)

        assert 'last_notification_date' in {column['name'] for column in columns}
