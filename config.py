# ═══════════════════════════════════════════════════════════════
#  NAS 烤机 & 压测工具 — 配置文件
#  by 童儿制作 仅供学习参考 请尊重每个人的知识产权
# ═══════════════════════════════════════════════════════════════

# ── NAS 连接信息 ──────────────────────────────────────────
NAS_HOST = "192.168.3.101"       # NAS IP 地址
NAS_HTTP_PORT = 5666             # NAS Web 管理界面端口 (飞牛 OS)

# ── SSH ───────────────────────────────────────────────────
SSH_PORT = 22                    # SSH 端口
SSH_USER = "admin"               # SSH 用户名
SSH_PASSWORD = ""                # SSH 密码 (留空则使用 ~/.ssh/id_ed25519 密钥)

# ── 压测默认值 ────────────────────────────────────────────
UDP_PORT = 9998                  # UDP 发包目标端口
HTTP_CONCURRENCY = 20            # HTTP 默认并发数
HTTP_REQUESTS_PER_CONN = 50      # HTTP keep-alive 每连接请求数
UDP_CONCURRENCY = 4              # UDP 默认线程数
UDP_PAYLOAD_SIZE = 1400          # UDP 包大小 (字节)
FILE_THREADS = 4                 # 文件 I/O 默认线程数
FILE_SIZE_MB = 512               # 测试文件大小 (MB)
FILE_BLOCK_SIZE = "1m"           # I/O 块: 4k / 64k / 1m / 4m / 16m
CPU_WORKERS = 0                  # CPU 满载核心数 (0=全部)

# ── Web 服务器 ────────────────────────────────────────────
WEB_HOST = "127.0.0.1"           # 监听地址
WEB_PORT = 8888                  # 监听端口
