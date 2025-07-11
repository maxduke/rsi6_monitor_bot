# --- Stage 1: Builder ---
FROM python:3.9-slim-buster AS builder

WORKDIR /app

# 安装构建依赖并清理缓存
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip wheel --no-cache-dir --wheel-dir /wheels -r requirements.txt

# --- Stage 2: Final Image ---
FROM python:3.9-slim-buster

# 创建非root用户
RUN groupadd -r appuser && \
    useradd -r -s /bin/false -g appuser appuser

WORKDIR /app

# 从构建阶段复制依赖
COPY --from=builder --chown=appuser:appuser /wheels /wheels
RUN pip install --no-cache /wheels/* && \
    rm -rf /wheels  # 立即清理

# 创建数据目录并设置权限
RUN mkdir -p /app/data && \
    chown appuser:appuser /app/data
VOLUME /app/data

# 复制应用代码（使用非root用户权限）
COPY --chown=appuser:appuser src/ ./src/

# 环境变量
ENV DB_FILE=/app/data/rules.db \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# 切换到非特权用户
USER appuser

# 入口点
CMD ["python", "src/rsi_monitor_bot.py"]