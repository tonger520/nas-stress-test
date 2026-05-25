# 🔥 NAS 烤机 & 压测 — Web 控制台

> by 童儿 制作 · 仅供学习参考 · 请尊重每个人的知识产权

一款基于 **Python + Flask** 的轻量级 NAS 压力测试工具，内置 Web 控制台，支持 HTTP 压测、UDP 发包、文件 IO、SSH CPU 满载、Web 终端、电源管理等功能，开箱即用。

## ✨ 功能一览

| 模块 | 说明 |
|------|------|
| **HTTP 压测** | 连接保持、指数退避重试、优雅降级、实时统计 |
| **UDP 发包** | 多线程突发压测、流量控制、自动限速保护 |
| **文件 IO** | 顺序写入/读取、实时校验、进度条显示 |
| **SSH CPU 满载** | 远程拉起多进程 CPU 压力、单进程模式 |
| **Web 终端** | 基于 xterm.js + WebSocket 的完整 SSH 终端 |
| **电源管理** | 远程关机 / 重启 |
| **自定义组合** | 自由勾选多个测试模块同时运行 |

## 🚀 快速开始

### 安装依赖

```bash
pip install flask paramiko flask-sock
```

### 编辑配置

打开 `config.py`，填入你的 NAS 信息：

```python
NAS_HOST = "192.168.3.101"       # NAS IP 地址
NAS_HTTP_PORT = 5666             # NAS Web 管理界面端口
SSH_PORT = 22                    # SSH 端口
SSH_USER = "admin"               # SSH 用户名
SSH_PASSWORD = ""                # 留空则使用密钥
```

### 启动

```bash
python nas_stress_web.py
```

浏览器打开 `http://localhost:8888` 即可使用。

## 📂 项目结构

```
nas-stress-test/
├── nas_stress_web.py   # 主程序（含内嵌前端页面）
├── config.py           # 配置文件
└── README.md
```

## ⚠️ 免责声明

本工具仅供学习交流使用，使用者需自行承担压测带来的风险，包括但不限于硬盘损坏、数据丢失、设备故障等。请在充分了解风险后使用，切勿在生产环境中运行。
