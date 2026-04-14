# CLAUDE.md

## 项目概述

A 股/ETF RSI(6) 实时监控 Telegram 机器人。当指定资产的 RSI 值进入用户设定的目标区间时，通过 Telegram 发送通知。

**技术栈**: Python 3.12+ / python-telegram-bot / akshare / pandas / SQLite / Docker

## 模块结构

```
src/
├── main.py            # 入口：配置验证 → DB 初始化 → 注册 handlers/jobs → run_polling
├── config.py          # 环境变量加载、值范围验证、常量定义、日志配置
├── database.py        # SQLite 持久连接 + threading.Lock 保护，白名单 CRUD
├── data_fetcher.py    # 数据获取（东方财富/新浪双源容灾）、RSI 计算、缓存
├── market.py          # 交易日判断、交易时间检查、东方财富封禁检测（asyncio.Lock）
├── handlers.py        # 所有 Telegram 命令处理器（@whitelisted_only/@admin_only）
├── jobs.py            # 后台定时任务：check_rules_job、daily_briefing_job
├── utils.py           # 共享工具：normalize_hist_df()、get_sina_symbol()
└── etf_data.py        # ETF 新浪历史净值数据获取
```

## 开发环境

```bash
# 创建 venv（使用 uv）
uv venv .venv --python 3.13
source .venv/Scripts/activate   # Windows (Git Bash)

# 安装依赖
uv pip install -r requirements.txt

# 安装测试依赖
uv pip install pytest

# 运行
TELEGRAM_TOKEN=xxx ADMIN_USER_ID=123 python -m src.main

# 运行测试
pytest tests/ -v

# Docker 构建
docker-compose up -d --build
```

## 依赖管理

- `requirements.in` — 顶层依赖声明（未锁定版本）
- `requirements.txt` — pip-compile 生成的锁定文件
- 更新依赖：`./upgrade_requirments.sh`（Docker 内 pip-compile）
- CI 每周自动更新 akshare：`.github/workflows/update-akshare-pr.yml`

## 代码规范

- 所有用户可见文本使用中文
- 日志消息使用中文，log level：INFO 正常流程，WARNING 可恢复错误，ERROR 不可恢复
- 外部 API 调用必须经过 `_run_with_retries()` 包装（指数退避）
- DataFrame 空值检查统一使用 `if df is None or df.empty:`，不要用 `if not df:`
- 价格的 None 检查使用 `if price is None:`，不要用 `if not price:`（0.0 是合法价格）
- pandas NaN 检查使用 `pd.isna()`，不要用 `is None`
- 数据库操作通过 `database.db_execute()` 统一入口，自带锁保护
- 全局可变状态（缓存等）必须使用 `asyncio.Lock` 或 `threading.Lock` 保护

## 关键业务逻辑

- **RSI 算法**: Wilder 平滑（EWM alpha=1/N），复刻同花顺/东财口径，见 `data_fetcher.calculate_rsi_exact()`
- **复权处理**: 计算复权因子（复权收盘/未复权收盘），将实时价格转换到复权尺度
- **数据源容灾**: 东方财富为主 → 检测到封禁时自动切换新浪
- **通知去重**: 进入区间后发送 N 次通知（可配），离开区间自动重置计数器

## 测试

```bash
pytest tests/ -v
```

测试覆盖核心纯函数：RSI 计算、区间判断、价格序列构建、工具函数、消息分块。
不测试外部 API 调用和 Telegram 处理器（依赖 mock 成本高，ROI 低）。
