# A股/ETF RSI 监控 Telegram 机器人

这是一个功能丰富的 Telegram 机器人，用于实时监控中国A股或场内基金的 RSI(6) 指标。当指定的股票或ETF的RSI值进入您设定的目标区间时，它会立即发送通知。

## ✨ 功能特性

- **规则管理**: 支持添加、删除、查看、开启和关闭监控规则。
- **白名单**: 只有授权的 Telegram 用户才能与机器人交互。
- **开盘监控**: 任务仅在中国 A 股交易时段 (09:30-11:30, 13:00-15:00) 运行。
- **实时 RSI(6)**: 结合历史和实时价格计算 RSI，精准响应市场动态。
- **数据持久化**: 所有规则和用户数据都通过 SQLite 存储在挂载卷中，确保容器重启后数据不丢失。
- **环境变量配置**: 所有关键配置均通过环境变量设置，便于部署和管理。
- **Docker化**: 提供优化的 `Dockerfile` 和 `docker-compose.yml`，实现一键启动。
- **CI/CD**: 集成 GitHub Actions，在代码推送到 `main` 分支后自动执行以下操作：
  - **构建多架构镜像**: 支持 `linux/amd64` (标准PC, 服务器) 和 `linux/arm64` (树莓派, Apple M系列芯片等)。
  - **推送到多仓库**: 同时将镜像推送到 **Docker Hub** 和 **GitHub Container Registry (GHCR)**。

## ⚙️ 配置

机器人通过环境变量进行配置。对于 Docker 部署，建议创建一个 `.env` 文件。

| 变量名                        | 描述                                                       | 默认值         |
| ----------------------------- | ---------------------------------------------------------- | -------------- |
| `TELEGRAM_TOKEN`              | **必需**: 你的 Telegram 机器人 Token。                     | -              |
| `ADMIN_USER_ID`               | **必需**: 你的 Telegram 用户 ID，作为管理员。              | -              |
| `DB_FILE`                     | 数据库文件的路径。推荐使用容器内路径以实现数据持久化。     | `rules.db`     |
| `CHECK_INTERVAL_SECONDS`      | 每次检查规则的间隔时间（秒）。                             | `60`           |
| `RSI_PERIOD`                  | 计算RSI的周期。                                            | `6`            |
| `HIST_FETCH_DAYS`             | 计算RSI时获取历史数据的天数。                              | `30`           |
| `MAX_NOTIFICATIONS_PER_TRIGGER` | 在一个触发周期内（从进入到离开区间），最多发送多少次通知。 | `1`            |
| `RANDOM_DELAY_MAX_SECONDS`    | 在每次获取实时数据前，增加一个0到该秒数之间的随机延迟，以避免被服务器限制。 | `0`    |
| `REQUEST_INTERVAL_SECONDS`    | **每个API请求之间**的固定间隔时间（秒），用于防止接口限制。 | `1.0`  |
| `FETCH_FAILURE_THRESHOLD`     | 连续获取数据失败多少次后，向管理员发送一条警报通知。         | `5`    |

## 🤖 如何使用

启动机器人后，在 Telegram 中与你的机器人对话即可。

| 命令                | 示例                             | 描述                           |
| ------------------- | -------------------------------- | ------------------------------ |
| `/start`            | `/start`                         | 开始使用机器人并显示欢迎信息。 |
| `/help`             | `/help`                          | 显示帮助信息和所有可用命令。   |
| `/add`              | `/add 510300 20 30`              | 添加一条新的监控规则。         |
| `/del`              | `/del 3`                         | 根据ID删除一条规则。           |
| `/list`             | `/list`                          | 查看你设置的所有监控规则。     |
| `/on`               | `/on 3`                          | 根据ID开启一条规则。           |
| `/off`              | `/off 3`                         | 根据ID关闭一条规则。           |
| **管理员命令**      |                                  |                                |
| `/add_w`            | `/add_w 123456789`               | 添加用户到白名单。             |
| `/del_w`            | `/del_w 123456789`               | 从白名单移除用户。             |
| `/list_w`           | `/list_w`                        | 查看白名单列表。               |

## 🚀 部署与运行

推荐使用 Docker 和 Docker Compose 进行部署。

### 准备工作

1.  安装 [Docker](https://docs.docker.com/get-docker/) 和 [Docker Compose](https://docs.docker.com/compose/install/)。
2.  克隆本仓库:
    ```bash
    git clone https://github.com/maxduke/rsi6_monitor_bot.git
    cd rsi6_monitor_bot
    ```
3.  创建 `.env` 文件:
    复制下面的内容到项目根目录下的一个新文件 `.env` 中，并填入你自己的信息。
    ```env
    # .env 文件
    TELEGRAM_TOKEN=123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11
    ADMIN_USER_ID=123456789
    CHECK_INTERVAL_SECONDS=60
    ```

### 启动机器人 (两种方案)

#### 方案 A: 使用预构建的镜像 (生产环境推荐)

此方案直接从 Docker Hub 或 GHCR 拉取已经构建好的镜像，无需在本地编译。

1.  **修改 `docker-compose.yml`**:
    打开 `docker-compose.yml` 文件，注释掉 `build: .` 这一行，并取消 `image:` 这一行的注释。将其中的镜像地址修改为你自己的 (推荐使用 GHCR)。

    ```yaml
    # ...
    services:
      rsi-bot:
        # 方案1: 从 Dockerfile 本地构建 (用于开发)
        # build: .
        
        # 方案2: 从镜像仓库拉取预构建的多架构镜像 (用于生产)
        image: ghcr.io/your-github-username/your-repo-name:latest
    # ...
    ```

2.  **启动服务**:
    ```bash
    docker-compose up -d
    ```

#### 方案 B: 本地构建镜像 (开发环境推荐)

此方案会在你的机器上根据 `Dockerfile` 构建镜像。

1.  **使用默认的 `docker-compose.yml`** (确保 `build: .` 生效，`image:` 被注释)。
2.  **启动服务**:
    ```bash
    docker-compose up -d --build
    ```

### 查看与管理

- **查看日志**: `docker-compose logs -f`
- **停止服务**: `docker-compose down`

## ⚠️ 注意事项与故障排除

### NumPy 版本依赖

本项目依赖的某些库（如 `pandas-ta`）可能与最新版的 `NumPy 2.0` 存在兼容性问题。为了确保稳定运行，`requirements.txt` 文件已将 NumPy 版本固定在 `2.0` 以下。如果您在自行构建或修改时遇到 `ImportError: cannot import name 'NaN' from 'numpy'` 相关的错误，请确保您的环境中安装的 NumPy 版本低于 `2.0`。