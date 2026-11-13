# Build Stage
# 固定工具链版本：rustc 1.100（stable 2026-11-09）起 allocator API 变化，
# 依赖 turso_core 0.8.3 编译失败（E0308, DynVec::new_in）。请勿改回 rust:latest，
# 移动标签会让 CI 镜像构建随上游工具链漂移而失败。
FROM rust:1.96-bookworm AS builder

WORKDIR /usr/src/app
COPY . .

# 注入架构特定的 CPU 优化参数
# 注意：这将生成针对特定 CPU (Zen4 / Kunpeng 920) 优化的二进制文件
# 如果运行环境不支持这些指令集，程序将无法启动 (Illegal Instruction)
    ARG TARGETARCH
    RUN case "$TARGETARCH" in \
          "amd64") export RUSTFLAGS="-C target-cpu=x86-64-v3" ;; \
          "arm64") export RUSTFLAGS="-C target-cpu=tsv110" ;; \
        esac && \
        cargo build --release
# Runtime Stage
FROM debian:bookworm-slim

# 设置时区为 Asia/Shanghai
ENV TZ=Asia/Shanghai

# 安装必要的系统依赖
# reqwest (使用 rustls) 通常不需要 OpenSSL 开发库，但 ca-certificates 是必须的
# 同时安装 tzdata 以支持时区设置
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 从 builder 阶段复制二进制文件
COPY --from=builder /usr/src/app/target/release/queqiao-router ./queqiao-router

# 复制配置目录（如果运行时需要）
# 注意：Config map 可能会在部署时挂载，但这里提供默认结构
COPY config ./config
# 确保日志目录存在
RUN mkdir -p logs

# 设置数据库路径环境变量
ENV RECD_PATH="sqlite:./logs/record.db"

# 暴露端口
EXPOSE 8000

# 启动命令
CMD ["./queqiao-router"]
