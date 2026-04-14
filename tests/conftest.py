# -*- coding: utf-8 -*-

import os
import pytest

# 设置测试所需的最小环境变量，避免 config.py 导入时报错
os.environ.setdefault("TELEGRAM_TOKEN", "test-token")
os.environ.setdefault("ADMIN_USER_ID", "12345")
