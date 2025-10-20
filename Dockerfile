# --- Stage 1: Builder ---
# 使用一个更新的、受支持的基础镜像
FROM python:3.12-slim-bookworm AS builder

WORKDIR /app

# 安装构建依赖并清理缓存
# 对于 Debian Bookworm, build-essential 通常是足够的
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential && \
    rm -rf /var/lib/apt/lists/*

# 首先只复制 requirements.txt 以利用 Docker 缓存
COPY requirements.txt .

# 使用 pip wheel 构建依赖
RUN pip wheel --no-cache-dir --wheel-dir /wheels -r requirements.txt

# --- Stage 2: Final Image ---
# 确保最终镜像和构建镜像使用相同的基础
FROM python:3.12-slim-bookworm

# 创建非 root 用户
RUN groupadd -r appuser && \
    useradd -r -s /bin/false -g appuser appuser

WORKDIR /app

# 从构建阶段复制 wheels 并安装
COPY --from=builder --chown=appuser:appuser /wheels /wheels
RUN pip install --no-cache /wheels/* && \
    rm -rf /wheels

# 创建数据目录并设置权限
RUN mkdir -p /app/data && \
    chown appuser:appuser /app/data
VOLUME /app/data

# 复制应用代码
COPY --chown=appuser:appuser src/ ./src/

# 环境变量
ENV DB_FILE=/app/data/rules.db \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# 切换到非特权用户
USER appuser

# 入口点
CMD ["python", "src/rsi_monitor_bot.py"]
