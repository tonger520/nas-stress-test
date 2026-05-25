#!/usr/bin/env python3
"""
NAS 烤机 & 压测 — Web 控制台
by 童儿制作 仅供学习参考 请尊重每个人的知识产权

功能: HTTP压测 | UDP发包 | 文件IO | SSH CPU满载 | Web终端 | 电源管理
配置: 编辑 config.py
依赖: pip install flask paramiko flask-sock
"""

import hashlib, json, os, re, random, signal, socket, ssl, statistics
import sys, threading, time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlparse

# ─── 配置 ──────────────────────────────────────────────
try:
    from config import (
        NAS_HOST, NAS_HTTP_PORT, SSH_PORT, SSH_USER, SSH_PASSWORD,
        UDP_PORT, HTTP_CONCURRENCY, HTTP_REQUESTS_PER_CONN,
        UDP_CONCURRENCY, UDP_PAYLOAD_SIZE,
        FILE_THREADS, FILE_SIZE_MB, FILE_BLOCK_SIZE, CPU_WORKERS,
        WEB_HOST, WEB_PORT,
    )
except ImportError:
    NAS_HOST = "192.168.3.101"; NAS_HTTP_PORT = 5666
    SSH_PORT = 22; SSH_USER = "admin"; SSH_PASSWORD = ""
    UDP_PORT = 9998; HTTP_CONCURRENCY = 20; HTTP_REQUESTS_PER_CONN = 50
    UDP_CONCURRENCY = 4; UDP_PAYLOAD_SIZE = 1400
    FILE_THREADS = 4; FILE_SIZE_MB = 512; FILE_BLOCK_SIZE = "1m"
    CPU_WORKERS = 0; WEB_HOST = "127.0.0.1"; WEB_PORT = 8888

# ─── 依赖 ──────────────────────────────────────────────
try:
    import paramiko
    HAS_SSH = True
except ImportError:
    HAS_SSH = False

try:
    from flask_sock import Sock
    HAS_SOCK = True
except ImportError:
    HAS_SOCK = False

from flask import Flask, jsonify, render_template_string, request

app = Flask(__name__)
sock = Sock(app) if HAS_SOCK else None

BLOCK_SIZES = {"4k": 4096, "64k": 65536, "1m": 1048576, "4m": 4194304, "16m": 16777216}

# ─── 远程 CPU 脚本 ─────────────────────────────────────
CPU_SCRIPT = r"""
import multiprocessing as mp, os, sys, time, signal
running = True
def h(s,f):
    global running; running = False
for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
    try: signal.signal(sig, h)
    except: pass
def w():
    n = 1
    while running:
        x = n; _ = x**0.5 * 3.1415926535 + 2.718281828
        for i in range(2, int(x**0.5)+1):
            if x % i == 0: break
        n = n+1 if n < 99999999 else 1
nw = int(sys.argv[1]) if len(sys.argv)>1 else (os.cpu_count() or 4)
print(f"CPU:{nw}:START", flush=True)
ps = [mp.Process(target=w, daemon=True) for _ in range(nw)]
for p in ps: p.start()
while running: time.sleep(0.5)
for p in ps:
    try: p.terminate()
    except: pass
for p in ps:
    try: p.join(timeout=3)
    except: pass
print("CPU:DONE", flush=True)
"""


# ═══════════════════════════════════════════════════════════
#  Stats
# ═══════════════════════════════════════════════════════════
class Stats:
    def __init__(self):
        self._lk = threading.Lock()
        self.start_time = time.time()
        self.http_requests = 0; self.http_success = 0; self.http_errors = 0
        self.http_latencies = []; self.http_bytes = 0
        self.udp_packets = 0; self.udp_bytes = 0; self.udp_errors = 0
        self.bytes_written = 0; self.bytes_read = 0
        self.write_ops = 0; self.read_ops = 0
        self.write_lats = []; self.read_lats = []
        self.cpu_active = False; self.cpu_workers = 0
        self.messages = []

    def set_cpu(self, active, workers=0):
        with self._lk: self.cpu_active = active; self.cpu_workers = workers

    def http_ok(self, lat, sz, status):
        with self._lk:
            self.http_requests += 1; self.http_latencies.append(lat); self.http_bytes += sz
            if 200 <= status < 400: self.http_success += 1
            else: self.http_errors += 1

    def http_fail(self, msg=""):
        with self._lk: self.http_requests += 1; self.http_errors += 1
        if msg: self.messages.append(msg)

    def udp_ok(self, sz):
        with self._lk: self.udp_packets += 1; self.udp_bytes += sz

    def udp_fail(self):
        with self._lk: self.udp_errors += 1

    def write_ok(self, sz, lat):
        with self._lk: self.bytes_written += sz; self.write_ops += 1; self.write_lats.append(lat)

    def read_ok(self, sz, lat):
        with self._lk: self.bytes_read += sz; self.read_ops += 1; self.read_lats.append(lat)

    def log(self, msg):
        with self._lk: self.messages.append(msg)

    def snapshot(self):
        with self._lk:
            e = max(time.time() - self.start_time, 0.001)
            def p(arr, q):
                if not arr: return 0
                s = sorted(arr); return s[min(int(len(s)*q), len(s)-1)]
            return {
                "elapsed": round(e,1),
                "http_rps": round(self.http_requests/e,1),
                "http_total": self.http_requests,
                "http_success": self.http_success,
                "http_errors": self.http_errors,
                "http_mb": round(self.http_bytes/1048576,2),
                "http_lat_avg": round(statistics.mean(self.http_latencies or [0]),2),
                "http_lat_p50": round(p(self.http_latencies,0.5),2),
                "http_lat_p95": round(p(self.http_latencies,0.95),2),
                "udp_pps": round(self.udp_packets/e,0),
                "udp_total": self.udp_packets,
                "udp_mb": round(self.udp_bytes/1048576,2),
                "udp_mbps": round((self.udp_bytes/1048576)/e,2),
                "udp_errors": self.udp_errors,
                "write_mb": round(self.bytes_written/1048576,2),
                "read_mb": round(self.bytes_read/1048576,2),
                "write_mbps": round((self.bytes_written/1048576)/e,2),
                "read_mbps": round((self.bytes_read/1048576)/e,2),
                "write_iops": round(self.write_ops/e,1),
                "read_iops": round(self.read_ops/e,1),
                "write_lat_avg": round(statistics.mean(self.write_lats or [0]),2),
                "write_lat_p95": round(p(self.write_lats,0.95),2),
                "read_lat_avg": round(statistics.mean(self.read_lats or [0]),2),
                "read_lat_p95": round(p(self.read_lats,0.95),2),
                "cpu_active": self.cpu_active,
                "cpu_workers": self.cpu_workers,
                "messages": self.messages[-20:],
            }


# ═══════════════════════════════════════════════════════════
#  SSH 工具
# ═══════════════════════════════════════════════════════════
def ssh_connect(host, port, user, password):
    if not HAS_SSH:
        return None, "缺少 paramiko: pip install paramiko"
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        kw = {"hostname": host, "port": port, "username": user,
              "timeout": 10, "banner_timeout": 8, "auth_timeout": 8}
        if password:
            kw["password"] = password; kw["look_for_keys"] = False
        else:
            kw["look_for_keys"] = True; kw["allow_agent"] = True
        c.connect(**kw)
        return c, None
    except paramiko.AuthenticationException:
        return None, "认证失败: 用户名或密码错误"
    except socket.timeout:
        return None, f"连接超时: {host}:{port}"
    except Exception as e:
        return None, str(e)


def ssh_quick_cmd(client, cmd, get_pty=False, stdin_input=None):
    stdin, stdout, stderr = client.exec_command(cmd, get_pty=get_pty)
    if stdin_input and get_pty:
        stdin.write(stdin_input + "\n"); stdin.flush()
    return stdout.read().decode(errors="replace"), stderr.read().decode(errors="replace")


# ═══════════════════════════════════════════════════════════
#  Workers
# ═══════════════════════════════════════════════════════════
def http_worker(running, stats, host, port, path, ssl_flag, wid, rpc=50, delay=0):
    targets = [path, path+"/", "/favicon.ico", "/login", "/api/status",
               "/index.html", "/admin", "/dashboard", "/settings",
               "/api/info", "/api/list", "/static/app.js"]
    backoff = 0.2
    while running():
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.settimeout(5)
            if ssl_flag:
                ctx = ssl.create_default_context()
                ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
                sock = ctx.wrap_socket(sock, server_hostname=host)
            sock.connect((host, port))
            backoff = 0.2
            for _ in range(rpc):
                if not running(): break
                p = random.choice(targets); t0 = time.time()
                req = (f"GET {p} HTTP/1.1\r\nHost: {host}:{port}\r\n"
                       f"User-Agent: Mozilla/5.0\r\n"
                       f"Accept: */*\r\nConnection: keep-alive\r\n\r\n")
                sock.sendall(req.encode())
                hd = b""; body = b""; cl = 0; done = False
                while True:
                    try:
                        ck = sock.recv(4096)
                        if not ck: break
                        if not done:
                            hd += ck
                            if b"\r\n\r\n" in hd:
                                hd, body = hd.split(b"\r\n\r\n", 1); done = True
                                m = re.search(rb"Content-Length:\s*(\d+)", hd, re.I)
                                if m: cl = int(m.group(1))
                                if cl == 0: break
                        else: body += ck
                        if done and cl > 0 and len(body) >= cl: break
                    except socket.timeout: break
                status = 200
                try:
                    sl = hd.split(b"\r\n")[0].decode()
                    if sl.startswith("HTTP/"): status = int(sl.split(" ")[1])
                except: pass
                stats.http_ok((time.time()-t0)*1000, len(hd)+len(body)+len(req), status)
                if delay: time.sleep(delay/1000)
        except Exception as e:
            stats.http_fail(f"[W{wid}] {type(e).__name__}")
            time.sleep(backoff); backoff = min(backoff*1.8, 15)
        finally:
            if sock:
                try: sock.close()
                except: pass


def udp_worker(running, stats, host, port, psize, wid):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(0.1); t = (host, port)
    while running():
        try:
            sz = random.randint(psize//2, psize)
            s.sendto(os.urandom(sz), t); stats.udp_ok(sz)
        except BlockingIOError: pass
        except Exception: stats.udp_fail(); time.sleep(0.05)
    s.close()


def file_worker(running, stats, path, bs, fsb, verify, wid):
    fp = Path(path) / f".nas_w{wid}.tmp"
    def rand_blk(sz):
        if random.random() < 0.5: return os.urandom(sz)
        return (random.choice("ABC01\x00")*sz).encode()[:sz]
    def cs(d): return hashlib.sha256(d).hexdigest()[:16]
    while running():
        try:
            t0 = time.time(); cks = {}; w = 0
            with open(fp, "wb", buffering=bs) as f:
                while w < fsb and running():
                    blk = rand_blk(min(bs, fsb-w)); f.write(blk)
                    if verify: cks[w] = cs(blk)
                    w += len(blk)
                f.flush(); os.fsync(f.fileno())
            stats.write_ok(w, (time.time()-t0)*1000)
        except Exception as e:
            stats.log(str(e)); time.sleep(1); continue
        try:
            t0 = time.time(); r = 0
            with open(fp, "rb", buffering=bs) as f:
                off = 0
                while running():
                    blk = f.read(bs)
                    if not blk: break
                    if verify and off in cks and cs(blk) != cks[off]:
                        stats.log(f"校验失败 @{off}")
                    r += len(blk); off += len(blk)
            stats.read_ok(r, (time.time()-t0)*1000)
        except Exception as e:
            stats.log(str(e)); time.sleep(1)
        time.sleep(0.01)
    if fp.exists():
        try: fp.unlink()
        except: pass


# ═══════════════════════════════════════════════════════════
#  Engine
# ═══════════════════════════════════════════════════════════
class Engine:
    def __init__(self):
        self.running = False
        self.mode = None
        self.stats = Stats()
        self._pool = None
        self._futures = []
        self._cleanup = None
        self._ssh_thread = None
        self._ssh_info = {}

    def _begin(self, mode):
        self.running = True; self.mode = mode; self.stats = Stats()

    def stop(self):
        if not self.running: return False, "没有运行中的测试"
        prev = self.mode; self.running = False
        self._kill_remote()
        if self._pool: self._pool.shutdown(wait=False, cancel_futures=True); self._pool = None
        if self._cleanup:
            for f in self._cleanup.glob(".nas_w*.tmp"):
                try: f.unlink()
                except: pass
            self._cleanup = None
        self.mode = None; self._futures = []
        return True, f"已停止 · {prev}"

    def status(self):
        return {"running": self.running, "mode": self.mode, "stats": self.stats.snapshot()}

    # ── SSH CPU ──────────────────────────────────────────
    def _cpu_run(self, host, port, user, pw, workers):
        tw = workers if workers > 0 else (os.cpu_count() or 4)
        self.stats.set_cpu(True, tw)
        ok = False; client = None
        try:
            client, err = ssh_connect(host, port, user, pw)
            if err: self.stats.log(f"SSH: {err}"); return
            self.stats.log(f"已连接 {user}@{host}:{port}")
            sftp = client.open_sftp()
            with sftp.file("/tmp/.nas_cpu.py", "w") as f: f.write(CPU_SCRIPT)
            sftp.chmod("/tmp/.nas_cpu.py", 0o700); sftp.close()
            ch = client.get_transport().open_session()
            ch.exec_command(f"python3 /tmp/.nas_cpu.py {tw}")
            buf = ""
            while self.running:
                if ch.recv_ready():
                    buf += ch.recv(1024).decode(errors="replace")
                    while "\n" in buf:
                        line, buf = buf.split("\n", 1); line = line.strip()
                        if "CPU:" in line and ":START" in line: ok = True; self.stats.log(f"CPU 满载 ({tw}核)")
                        elif "CPU:DONE" in line: self.stats.log("CPU 进程退出")
                        elif line: self.stats.log(f"[NAS] {line[:120]}")
                else: time.sleep(0.05)
                if ch.exit_status_ready(): break
            ch.recv_exit_status()
        except Exception as e: self.stats.log(f"CPU 错误: {e}")
        finally:
            if client:
                try: client.close()
                except: pass
            self.stats.set_cpu(False)
            if not ok and self.running: self.stats.log("CPU 启动失败"); self.running = False; self.mode = None

    def cpu_start(self, host, port, user, pw, workers):
        if self.running: return False, "已有测试运行中"
        self._ssh_info = {"host": host, "port": port, "user": user, "password": pw}
        self._begin("ssh_cpu")
        self._ssh_thread = threading.Thread(target=self._cpu_run, args=(host, port, user, pw, workers), daemon=True)
        self._ssh_thread.start(); time.sleep(1.2)
        if self._ssh_thread.is_alive(): return True, f"CPU 满载 ({workers if workers>0 else '全部'}核)"
        msgs = self.stats.messages[-2:]; self.running = False; self.mode = None
        return False, "; ".join(msgs) if msgs else "连接失败"

    def _kill_remote(self):
        p = self._ssh_info
        if not p: return
        try:
            c, err = ssh_connect(p["host"], p["port"], p["user"], p["password"])
            if not err:
                c.exec_command("pkill -9 -f nas_cpu 2>/dev/null; rm -f /tmp/.nas_cpu.py; echo ok")
                c.close()
        except: pass

    # ── SSH 测试 ─────────────────────────────────────────
    def ssh_test(self, host, port, user, pw):
        c, err = ssh_connect(host, port, user, pw)
        if err: return False, err
        try:
            out, _ = ssh_quick_cmd(c, "python3 --version 2>&1 && echo PY_OK")
            c.close()
            if "PY_OK" in out: return True, f"连接成功! {out.strip().split(chr(10))[0]}"
            return False, "NAS 未安装 python3"
        except Exception as e:
            try: c.close()
            except: pass
            return False, str(e)

    # ── 电源 ─────────────────────────────────────────────
    def power(self, action, host, port, user, pw):
        if self.running: self.stop()
        c, err = ssh_connect(host, port, user, pw)
        if err: return False, f"SSH: {err}"
        try:
            cmd = "sudo shutdown -h now" if action == "shutdown" else "sudo reboot"
            lb = "关机" if action == "shutdown" else "重启"
            out, err_out = ssh_quick_cmd(c, cmd, get_pty=True, stdin_input=pw)
            c.close()
            if err_out and "sudo" not in err_out.lower(): return False, f"{lb}失败: {err_out[:100]}"
            return True, f"NAS {lb}指令已发送"
        except Exception as e:
            try: c.close()
            except: pass
            return False, str(e)

    # ── HTTP ─────────────────────────────────────────────
    def http_start(self, url, conc, rpc=50, delay=0):
        if self.running: return False, "已有测试运行中"
        p = urlparse(url if "://" in url else f"http://{url}")
        host = p.hostname or NAS_HOST; port = p.port or NAS_HTTP_PORT
        path = p.path or "/"; ssl_flag = p.scheme == "https"
        self._begin("http")
        self._pool = ThreadPoolExecutor(max_workers=conc)
        for i in range(conc):
            self._futures.append(self._pool.submit(
                http_worker, lambda: self.running, self.stats,
                host, port, path, ssl_flag, i, rpc, delay))
        return True, f"HTTP 压测 · {conc}并发 → {host}:{port}"

    # ── UDP ──────────────────────────────────────────────
    def udp_start(self, host, port, conc, psize):
        if self.running: return False, "已有测试运行中"
        self._begin("udp")
        self._pool = ThreadPoolExecutor(max_workers=conc)
        for i in range(conc):
            self._futures.append(self._pool.submit(
                udp_worker, lambda: self.running, self.stats,
                host, port, psize, i))
        return True, f"UDP 发包 · {conc}线程 → {host}:{port}"

    # ── 文件 I/O ─────────────────────────────────────────
    def file_start(self, path, threads, fsm, bsk, verify):
        if self.running: return False, "已有测试运行中"
        pp = Path(path); pp.mkdir(parents=True, exist_ok=True)
        self._cleanup = pp
        bs = BLOCK_SIZES.get(bsk, 1048576); fsb = fsm * 1048576
        self._begin("file")
        self._pool = ThreadPoolExecutor(max_workers=threads)
        for i in range(threads):
            self._futures.append(self._pool.submit(
                file_worker, lambda: self.running, self.stats,
                path, bs, fsb, verify, i))
        return True, f"文件 I/O · {threads}线程 → {path}"

    # ── 自定义 ───────────────────────────────────────────
    def custom_start(self, cfg):
        if self.running: return False, "已有测试运行中"
        en = []
        tw = 0
        if cfg.get("enable_http"): en.append("http"); tw += int(cfg.get("http_concurrency", 10))
        if cfg.get("enable_udp"): en.append("udp"); tw += int(cfg.get("udp_conc", 4))
        if cfg.get("enable_file"): en.append("file"); tw += int(cfg.get("file_threads", 4))
        if cfg.get("enable_cpu"): en.append("cpu"); tw += 1
        if not en: return False, "至少选一种"
        if cfg.get("enable_file"):
            pp = Path(cfg.get("path", "")); pp.mkdir(parents=True, exist_ok=True)
            self._cleanup = pp
        self._begin("custom")
        self._pool = ThreadPoolExecutor(max_workers=max(tw, 1))
        wid = 0
        if cfg.get("enable_http"):
            p = urlparse(cfg.get("http_url","") if "://" in str(cfg.get("http_url","")) else f"http://{cfg.get('http_url','')}")
            host = p.hostname or NAS_HOST; port = p.port or NAS_HTTP_PORT
            path = p.path or "/"; ssl_flag = p.scheme == "https"
            for _ in range(int(cfg.get("http_concurrency", 10))):
                self._futures.append(self._pool.submit(http_worker, lambda: self.running, self.stats, host, port, path, ssl_flag, wid, int(cfg.get("http_reqs_per_conn", 50))))
                wid += 1
        if cfg.get("enable_udp"):
            for _ in range(int(cfg.get("udp_conc", 4))):
                self._futures.append(self._pool.submit(udp_worker, lambda: self.running, self.stats, cfg.get("udp_host", NAS_HOST), int(cfg.get("udp_port", UDP_PORT)), int(cfg.get("udp_payload", 1400)), wid))
                wid += 1
        if cfg.get("enable_file"):
            bs = BLOCK_SIZES.get(cfg.get("block_size", "1m"), 1048576)
            fsb = int(cfg.get("file_size", 512)) * 1048576
            for _ in range(int(cfg.get("file_threads", 4))):
                self._futures.append(self._pool.submit(file_worker, lambda: self.running, self.stats, cfg.get("path",""), bs, fsb, not cfg.get("no_verify", False), wid))
                wid += 1
        if cfg.get("enable_cpu"):
            host = cfg.get("cpu_host", NAS_HOST); port = int(cfg.get("cpu_port", SSH_PORT))
            user = cfg.get("cpu_user", SSH_USER); pw = cfg.get("cpu_password", SSH_PASSWORD)
            workers = int(cfg.get("cpu_workers", 0))
            self._ssh_info = {"host": host, "port": port, "user": user, "password": pw}
            self._ssh_thread = threading.Thread(target=self._cpu_run, args=(host, port, user, pw, workers), daemon=True)
            self._ssh_thread.start()
        return True, f"组合 · {', '.join(en)}"


engine = Engine()


# ═══════════════════════════════════════════════════════════
#  Routes
# ═══════════════════════════════════════════════════════════
@app.route("/")
def index():
    return render_template_string(HTML)

@app.route("/api/status")
def api_status():
    return jsonify(engine.status())

@app.route("/api/start", methods=["POST"])
def api_start():
    d = request.get_json() or {}; m = d.get("mode", "")
    try:
        if m == "ssh_cpu":
            ok, msg = engine.cpu_start(d.get("host", NAS_HOST), int(d.get("port", SSH_PORT)), d.get("user", SSH_USER), d.get("password", SSH_PASSWORD), int(d.get("workers", 0)))
        elif m == "http":
            ok, msg = engine.http_start(d.get("url", f"http://{NAS_HOST}:{NAS_HTTP_PORT}/"), int(d.get("concurrency", HTTP_CONCURRENCY)), int(d.get("reqs_per_conn", HTTP_REQUESTS_PER_CONN)), int(d.get("delay_ms", 0)))
        elif m == "udp":
            ok, msg = engine.udp_start(d.get("host", NAS_HOST), int(d.get("port", UDP_PORT)), int(d.get("concurrency", UDP_CONCURRENCY)), int(d.get("payload_size", UDP_PAYLOAD_SIZE)))
        elif m == "file":
            ok, msg = engine.file_start(d.get("path", ""), int(d.get("threads", FILE_THREADS)), int(d.get("file_size", FILE_SIZE_MB)), d.get("block_size", FILE_BLOCK_SIZE), not d.get("no_verify", False))
        elif m == "custom":
            ok, msg = engine.custom_start(d)
        else:
            return jsonify({"ok": False, "message": f"Unknown mode: {m}"})
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)})
    return jsonify({"ok": ok, "message": msg})

@app.route("/api/stop", methods=["POST"])
def api_stop():
    ok, msg = engine.stop(); return jsonify({"ok": ok, "message": msg})

@app.route("/api/ssh_test", methods=["POST"])
def api_ssh_test():
    d = request.get_json() or {}
    ok, msg = engine.ssh_test(d.get("host", NAS_HOST), int(d.get("port", SSH_PORT)), d.get("user", SSH_USER), d.get("password", SSH_PASSWORD))
    return jsonify({"ok": ok, "message": msg})

@app.route("/api/power", methods=["POST"])
def api_power():
    d = request.get_json() or {}; a = d.get("action", "")
    if a not in ("shutdown", "reboot"): return jsonify({"ok": False, "message": "action=shutdown|reboot"})
    ok, msg = engine.power(a, d.get("host", NAS_HOST), int(d.get("port", SSH_PORT)), d.get("user", SSH_USER), d.get("password", SSH_PASSWORD))
    return jsonify({"ok": ok, "message": msg})


# ═══════════════════════════════════════════════════════════
#  WebSocket Terminal
# ═══════════════════════════════════════════════════════════
if HAS_SOCK and HAS_SSH:
    @sock.route("/ws/terminal")
    def ws_terminal(ws):
        client = None; chan = None
        try:
            raw = ws.receive()
            if not raw: return
            cfg = json.loads(raw)
            if cfg.get("type") != "connect": return
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            kw = {"hostname": cfg["host"], "port": cfg["port"], "username": cfg["user"],
                  "timeout": 10, "banner_timeout": 8, "auth_timeout": 8}
            if cfg.get("password", ""): kw["password"] = cfg["password"]; kw["look_for_keys"] = False
            else: kw["look_for_keys"] = True; kw["allow_agent"] = True
            client.connect(**kw)
            chan = client.invoke_shell(term="xterm-256color", width=120, height=40)
            chan.settimeout(0.1)
            ws.send(json.dumps({"type": "connected"}))
            while True:
                if chan.recv_ready():
                    data = chan.recv(8192)
                    if data: ws.send(data.decode("latin-1", errors="replace"))
                wsd = ws.receive(timeout=0.03)
                if wsd:
                    try:
                        ctl = json.loads(wsd)
                        if ctl.get("type") == "resize":
                            try: chan.resize_pty(ctl.get("w", 120), ctl.get("h", 40))
                            except: pass
                        elif ctl.get("type") == "disconnect": break
                    except json.JSONDecodeError:
                        chan.send(wsd.encode("utf-8"))
                time.sleep(0.02)
                if chan.closed: break
        except Exception as e:
            try: ws.send(json.dumps({"type": "error", "msg": str(e)}))
            except: pass
        finally:
            if chan:
                try: chan.close()
                except: pass
            if client:
                try: client.close()
                except: pass


# ═══════════════════════════════════════════════════════════
#  HTML — Glass UI
# ═══════════════════════════════════════════════════════════
HTML = r'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>NAS 烤机 — by 童儿</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300..800&display=swap" rel="stylesheet">
<link href="https://cdn.jsdelivr.net/npm/xterm@5.3.0/css/xterm.min.css" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/xterm@5.3.0/lib/xterm.min.js"></script>
<style>
:root{
 --bg:#f0f2f5;--bg2:#e8ecf1;
 --glass:rgba(255,255,255,.55);--glass2:rgba(255,255,255,.72);
 --bd:rgba(0,0,0,.06);--bd2:rgba(0,0,0,.1);
 --t:#1a1a2e;--t2:#334155;--t3:#64748b;--tm:#94a3b8;
 --ac:#06b6d4;--ac2:#10b981;--gn:#10b981;--rd:#ef4444;--or:#f59e0b;
 --gnb:rgba(16,185,129,.1);--rdb:rgba(239,68,68,.1);--orb:rgba(245,158,11,.1);--acb:rgba(6,182,212,.1);
 --r:20px;--rs:14px;--rx:10px;
 --sh:0 4px 24px rgba(0,0,0,.06),0 1px 3px rgba(0,0,0,.04);
 --sh2:0 12px 40px rgba(0,0,0,.08),0 2px 8px rgba(0,0,0,.04);
}
@media(prefers-color-scheme:dark){
 :root{
  --bg:#0a0a1a;--bg2:#111128;
  --glass:rgba(255,255,255,.04);--glass2:rgba(255,255,255,.07);
  --bd:rgba(255,255,255,.05);--bd2:rgba(255,255,255,.08);
  --t:#e2e8f0;--t2:#cbd5e1;--t3:#94a3b8;--tm:#64748b;
  --gn:#34d399;--rd:#f87171;--or:#fbbf24;
  --gnb:rgba(52,211,153,.1);--rdb:rgba(248,113,113,.1);--orb:rgba(251,191,36,.1);
  --sh:0 4px 24px rgba(0,0,0,.3);--sh2:0 12px 40px rgba(0,0,0,.4);
 }
}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:"Inter","Segoe UI","PingFang SC","Microsoft YaHei",sans-serif;font-size:15px;color:var(--t);background:var(--bg);background-image:radial-gradient(ellipse 70% 50% at 50% -10%,rgba(6,182,212,.06),transparent),radial-gradient(ellipse 50% 40% at 80% 90%,rgba(16,185,129,.04),transparent);min-height:100vh;-webkit-font-smoothing:antialiased;line-height:1.5}
::-webkit-scrollbar{width:5px}::-webkit-scrollbar-track{background:transparent}::-webkit-scrollbar-thumb{background:var(--bd2);border-radius:3px}
@keyframes fi{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}
@keyframes su{from{opacity:0;transform:translateY(14px)}to{opacity:1;transform:translateY(0)}}
@keyframes pd{0%,100%{box-shadow:0 0 8px rgba(239,68,68,.5)}50%{box-shadow:0 0 18px rgba(239,68,68,.85)}}
.anim{animation:fi .35s ease}

.topbar{position:sticky;top:0;z-index:100;display:flex;align-items:center;justify-content:space-between;padding:10px 22px;background:var(--glass2);backdrop-filter:blur(48px)saturate(200%);-webkit-backdrop-filter:blur(48px)saturate(200%);border-bottom:1px solid var(--bd)}
.topbar h2{font-size:1.15rem;font-weight:700}
.grad{background:linear-gradient(135deg,var(--ac),var(--ac2));-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.ctr{max-width:1100px;margin:0 auto;padding:20px 22px 40px}

.pnl{background:var(--glass);border:1px solid var(--bd);backdrop-filter:blur(32px)saturate(180%);-webkit-backdrop-filter:blur(32px)saturate(180%);border-radius:var(--r);padding:22px 24px;margin:10px 0;box-shadow:var(--sh);transition:all .3s ease}
.pnl:hover{background:var(--glass2);box-shadow:var(--sh2)}
.pnl-sm{background:var(--glass);border:1px solid var(--bd);backdrop-filter:blur(32px)saturate(180%);-webkit-backdrop-filter:blur(32px)saturate(180%);border-radius:var(--rs);padding:16px 18px;margin:6px 0;box-shadow:var(--sh);transition:all .3s ease}
.pnl-sm:hover{background:var(--glass2)}

.btn{display:inline-flex;align-items:center;justify-content:center;gap:6px;padding:10px 18px;font-size:.85rem;font-weight:600;color:#fff;border:1px solid transparent;border-radius:var(--rx);cursor:pointer;transition:all .25s cubic-bezier(.4,0,.2,1);font-family:inherit;position:relative;overflow:hidden}
.btn::after{content:"";position:absolute;inset:0;background:linear-gradient(180deg,rgba(255,255,255,.15),transparent 60%);pointer-events:none}
.btn:hover{transform:translateY(-1px)}.btn:active{transform:scale(.97)}.btn:disabled{opacity:.4;pointer-events:none;transform:none}
.btn-p{background:linear-gradient(135deg,var(--ac),var(--ac2));box-shadow:0 2px 12px rgba(6,182,212,.25)}.btn-p:hover{box-shadow:0 4px 20px rgba(6,182,212,.35)}
.btn-s{background:var(--glass);border-color:var(--bd2);color:var(--t);font-weight:500}.btn-s:hover{background:var(--glass2)}
.btn-r{background:var(--rdb);border-color:rgba(239,68,68,.3);color:var(--rd)}.btn-r:hover{background:rgba(239,68,68,.15)}
.btn-g{background:var(--gnb);border-color:rgba(16,185,129,.3);color:var(--gn)}.btn-g:hover{background:rgba(16,185,129,.15)}
.btn-o{background:var(--orb);border-color:rgba(245,158,11,.3);color:var(--or)}.btn-o:hover{background:rgba(245,158,11,.15)}
.btn-sm{padding:6px 12px;font-size:.76rem;border-radius:8px}
.btn-blk{display:flex;width:100%}

.lbl{display:block;font-size:.7rem;font-weight:600;color:var(--tm);text-transform:uppercase;letter-spacing:.06em;margin-bottom:4px}
.inp{width:100%;padding:9px 13px;font-size:.85rem;font-family:inherit;color:var(--t);background:var(--glass);border:1px solid var(--bd);border-radius:var(--rx);outline:none;transition:all .2s}
.inp:focus{border-color:var(--ac);box-shadow:0 0 0 4px rgba(6,182,212,.15)}

.badge{display:inline-flex;align-items:center;padding:4px 12px;border-radius:20px;font-size:.72rem;font-weight:600;letter-spacing:.02em}
.badge-ac{background:var(--acb);color:var(--ac)}.badge-gn{background:var(--gnb);color:var(--gn)}.badge-rd{background:var(--rdb);color:var(--rd)}.badge-n{background:rgba(0,0,0,.04);color:var(--tm)}
@media(prefers-color-scheme:dark){.badge-n{background:rgba(255,255,255,.06)}}
.stat{display:inline-flex;align-items:center;padding:5px 14px;border:1px solid var(--bd);border-radius:24px;font-size:.73rem;font-weight:600;background:var(--glass);backdrop-filter:blur(32px);-webkit-backdrop-filter:blur(32px)}
.stat-r{border-color:rgba(239,68,68,.25);background:var(--rdb);color:var(--rd)}
.stat-g{border-color:rgba(16,185,129,.25);background:var(--gnb);color:var(--gn)}

.tgl{position:relative;width:42px;height:24px;flex-shrink:0}
.tgl input{opacity:0;width:0;height:0}
.tgl .sl{position:absolute;cursor:pointer;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,.1);border-radius:24px;transition:all .3s;border:1px solid var(--bd2)}
.tgl input:checked+.sl{background:var(--ac);border-color:var(--ac)}
.tgl .sl::before{content:"";position:absolute;height:18px;width:18px;left:2px;bottom:2px;background:#fff;border-radius:50%;transition:all .3s cubic-bezier(.2,.9,.4,1);box-shadow:0 1px 3px rgba(0,0,0,.2)}
.tgl input:checked+.sl::before{transform:translateX(18px)}
@media(prefers-color-scheme:dark){.tgl .sl{background:rgba(255,255,255,.1)}}

.g2{display:grid;grid-template-columns:1fr 1fr;gap:14px}
.g3{display:grid;grid-template-columns:repeat(3,1fr);gap:14px}
.g5{display:grid;grid-template-columns:repeat(5,1fr);gap:10px}
@media(max-width:900px){.g3,.g5{grid-template-columns:1fr 1fr}.g2{grid-template-columns:1fr}}
@media(max-width:560px){.g2,.g3,.g5{grid-template-columns:1fr}}
.row{display:flex;gap:12px}.row>*{flex:1}
.between{display:flex;align-items:center;justify-content:space-between}
.center{text-align:center}.hidden{display:none!important}.xs{font-size:.7rem}.muted{color:var(--tm)}

.dot{width:8px;height:8px;border-radius:50%;display:inline-block;margin-right:6px}
.dot-on{background:var(--gn);box-shadow:0 0 8px rgba(16,185,129,.5)}
.dot-off{background:var(--tm)}
.dot-busy{background:var(--rd);box-shadow:0 0 8px rgba(239,68,68,.5);animation:pd 1.5s ease-in-out infinite}

.term-bar{display:flex;align-items:center;justify-content:space-between;padding:8px 14px;background:rgba(0,0,0,.04);border:1px solid var(--bd);border-radius:var(--rs) var(--rs) 0 0;border-bottom:none}
.term-dots{display:flex;gap:6px}.term-dots span{width:10px;height:10px;border-radius:50%}
.term-dots .d1{background:#ff5f57}.term-dots .d2{background:#febc2e}.term-dots .d3{background:#28c840}
#terminal{height:300px;border-radius:0 0 var(--rs) var(--rs);overflow:hidden;border:1px solid var(--bd);border-top:none}
@media(prefers-color-scheme:dark){.term-bar{background:rgba(255,255,255,.03)}}

.footer{text-align:center;font-size:.72rem;color:var(--tm);opacity:.5;line-height:1.8;margin-top:24px}
.toast{position:fixed;top:16px;right:16px;z-index:9999;padding:10px 18px;border-radius:var(--rs);font-size:.8rem;font-weight:500;background:var(--glass);border:1px solid var(--bd2);backdrop-filter:blur(48px);-webkit-backdrop-filter:blur(48px);box-shadow:var(--sh2);color:var(--t);animation:su .35s ease}
.log-box{max-height:140px;overflow-y:auto;font-family:'SF Mono','Cascadia Code',monospace;font-size:.68rem;color:var(--t3);line-height:1.7}
</style>
</head>
<body>

<div class="topbar">
 <div class="row" style="align-items:center;gap:10px"><h2><span class="grad">NAS</span> 烤机 & 压测</h2><span class="badge badge-ac">v5</span></div>
 <span id="badge" class="badge badge-n"><span class="dot dot-off"></span>待机中</span>
</div>

<div class="ctr anim">

 <!-- SSH -->
 <div class="pnl anim">
  <div class="between" style="margin-bottom:14px;flex-wrap:wrap;gap:10px"><h3 style="font-size:.95rem;font-weight:600">🔑 SSH 连接</h3><span id="sshStatus" class="xs muted"></span></div>
  <div class="g2">
   <div><label class="lbl">主机</label><input class="inp" id="sshHost" value="''' + NAS_HOST + '''"></div>
   <div><label class="lbl">端口</label><input class="inp" type="number" id="sshPort" value="''' + str(SSH_PORT) + '''"></div>
   <div><label class="lbl">用户名</label><input class="inp" id="sshUser" value="''' + SSH_USER + '''"></div>
   <div><label class="lbl">密码 (空=密钥)</label><input class="inp" type="password" id="sshPass" value="''' + (SSH_PASSWORD or "") + '''" placeholder="留空使用 ~/.ssh/id_ed25519"></div>
  </div>
  <div class="row" style="margin-top:12px"><button class="btn btn-g btn-sm" onclick="sshTest()">测试连接</button><button class="btn btn-s btn-sm" onclick="toggleTerm()">💻 终端</button></div>
 </div>

 <!-- 测试卡片 -->
 <div class="g3 anim">
  <div class="pnl-sm"><h3 style="font-size:.88rem;font-weight:600;margin-bottom:6px">🌐 HTTP 模拟</h3><p class="xs muted" style="margin-bottom:12px">keep-alive · 指数退避</p><label class="lbl">URL</label><input class="inp" id="sHttpUrl" value="http://''' + NAS_HOST + ":" + str(NAS_HTTP_PORT) + '''/" style="margin-bottom:8px"><div class="row" style="margin-bottom:8px"><div><label class="lbl">并发</label><input class="inp" type="number" id="sHttpConc" value="''' + str(HTTP_CONCURRENCY) + '''"></div><div><label class="lbl">请求/连接</label><input class="inp" type="number" id="sHttpRpc" value="''' + str(HTTP_REQUESTS_PER_CONN) + '''"></div></div><button class="btn btn-p btn-sm btn-blk" onclick="startOne('http')">启动 HTTP</button></div>
  <div class="pnl-sm"><h3 style="font-size:.88rem;font-weight:600;margin-bottom:6px">📡 UDP 发包</h3><p class="xs muted" style="margin-bottom:12px">无连接 · 不受连接限制</p><div class="row" style="margin-bottom:8px"><div><label class="lbl">IP</label><input class="inp" id="sUdpHost" value="''' + NAS_HOST + '''"></div><div><label class="lbl">端口</label><input class="inp" type="number" id="sUdpPort" value="''' + str(UDP_PORT) + '''"></div></div><div class="row" style="margin-bottom:8px"><div><label class="lbl">线程</label><input class="inp" type="number" id="sUdpConc" value="''' + str(UDP_CONCURRENCY) + '''"></div><div><label class="lbl">包大小(B)</label><input class="inp" type="number" id="sUdpPayload" value="''' + str(UDP_PAYLOAD_SIZE) + '''"></div></div><button class="btn btn-p btn-sm btn-blk" onclick="startOne('udp')">启动 UDP</button></div>
  <div class="pnl-sm"><h3 style="font-size:.88rem;font-weight:600;margin-bottom:6px">📁 文件 I/O</h3><p class="xs muted" style="margin-bottom:12px">SMB 挂载 · 循环读写</p><label class="lbl">挂载路径</label><input class="inp" id="sFilePath" placeholder="Z:\\share 或 /mnt/nas" style="margin-bottom:8px"><div class="row" style="margin-bottom:8px"><div><label class="lbl">线程</label><input class="inp" type="number" id="sFileThreads" value="''' + str(FILE_THREADS) + '''"></div><div><label class="lbl">MB</label><input class="inp" type="number" id="sFileSize" value="''' + str(FILE_SIZE_MB) + '''"></div></div><button class="btn btn-p btn-sm btn-blk" onclick="startOne('file')">启动文件</button></div>
 </div>

 <!-- CPU + 电源 + 组合 -->
 <div class="g2" style="margin-top:4px">
  <div>
   <div class="pnl-sm anim"><h3 style="font-size:.88rem;font-weight:600;margin-bottom:10px">🔥 SSH CPU 满载</h3><p class="xs muted" style="margin-bottom:10px">SSH 远程多进程满载 NAS CPU</p><label class="lbl">核心数 (0=全部)</label><input class="inp" type="number" id="cpuWorkers" value="''' + str(CPU_WORKERS) + '''" min="0" max="64" style="margin-bottom:10px"><button class="btn btn-r btn-sm btn-blk" onclick="startOne('ssh_cpu')">🔥 SSH CPU 满载</button></div>
   <div class="pnl-sm anim"><div class="between"><h3 style="font-size:.88rem;font-weight:600">🔌 NAS 电源</h3><div class="row"><button class="btn btn-o btn-sm" onclick="doPower('reboot')">↻ 重启</button><button class="btn btn-r btn-sm" onclick="doPower('shutdown')">⏻ 关机</button></div></div></div>
  </div>
  <div class="pnl-sm anim"><h3 style="font-size:.88rem;font-weight:600;margin-bottom:12px">⚡ 自由组合</h3>
   <div class="between" style="padding:6px 0;border-bottom:1px solid var(--bd)"><span>🌐 HTTP</span><label class="tgl"><input type="checkbox" id="cHttp" checked onchange="tgl('cHttpS')"><span class="sl"></span></label></div>
   <div id="cHttpS" class="row" style="margin:8px 0"><div><label class="lbl">并发</label><input class="inp" type="number" id="cHttpConc" value="10"></div><div><label class="lbl">请求/连接</label><input class="inp" type="number" id="cHttpRpc" value="50"></div></div>
   <div class="between" style="padding:6px 0;border-bottom:1px solid var(--bd)"><span>📡 UDP</span><label class="tgl"><input type="checkbox" id="cUdp" checked onchange="tgl('cUdpS')"><span class="sl"></span></label></div>
   <div id="cUdpS" class="row" style="margin:8px 0"><div><label class="lbl">线程</label><input class="inp" type="number" id="cUdpConc" value="2"></div><div><label class="lbl">包大小</label><input class="inp" type="number" id="cUdpPayload" value="1400"></div></div>
   <div class="between" style="padding:6px 0;border-bottom:1px solid var(--bd)"><span>📁 文件 I/O</span><label class="tgl"><input type="checkbox" id="cFile" onchange="tgl('cFileS')"><span class="sl"></span></label></div>
   <div id="cFileS" class="row hidden" style="margin:8px 0"><div><label class="lbl">路径</label><input class="inp" id="cFilePath" placeholder="Z:\\share"></div><div><label class="lbl">线程</label><input class="inp" type="number" id="cFileThreads" value="3"></div></div>
   <div class="between" style="padding:6px 0"><span>🔥 SSH CPU</span><label class="tgl"><input type="checkbox" id="cCpu" checked onchange="tgl('cCpuS')"><span class="sl"></span></label></div>
   <div id="cCpuS" style="margin:8px 0"><label class="lbl">核心数</label><input class="inp" type="number" id="cCpuWorkers" value="0"></div>
   <button class="btn btn-p btn-sm btn-blk" onclick="startCustom()" style="margin-top:10px">⚡ 启动组合压测</button>
  </div>
 </div>

 <div class="center" style="margin:16px 0"><button class="btn btn-r" id="stopBtn" onclick="doStop()" disabled style="font-size:.9rem;padding:10px 32px;border-radius:22px">■ 停止测试</button></div>

 <!-- 统计 -->
 <div class="pnl-sm anim"><h3 style="font-size:.78rem;font-weight:600;color:var(--t3);text-transform:uppercase;letter-spacing:.08em;margin-bottom:10px">📊 实时统计</h3>
  <div class="g5">
   <div class="stat"><span id="sT">0s</span></div>
   <div class="stat stat-r" id="sCpuW" style="display:none"><span id="sCpu">—</span></div>
   <div class="stat"><span id="sRps">0 rps</span></div>
   <div class="stat"><span id="sLat">— ms</span></div>
   <div class="stat stat-g" id="sUdpW" style="display:none"><span id="sPps">0 pps</span></div>
   <div class="stat"><span id="sUmb">— MB/s</span></div>
   <div class="stat"><span id="sWr">— MB/s</span></div>
   <div class="stat"><span id="sRd">— MB/s</span></div>
   <div class="stat"><span id="sWl">— ms</span></div>
   <div class="stat"><span id="sRl">— ms</span></div>
  </div>
  <div class="xs muted center" style="margin-top:10px" id="sDetail"></div>
 </div>

 <div class="pnl-sm log-box" style="margin-top:12px" id="logBox"><div>NAS 烤机控制台就绪 — ''' + NAS_HOST + ":" + str(NAS_HTTP_PORT) + ''' (飞牛 OS)</div></div>

 <!-- 终端 -->
 <div style="margin-top:16px"><div class="term-bar"><div class="term-dots"><span class="d1"></span><span class="d2"></span><span class="d3"></span></div><span class="xs muted" id="termTitle">终端 — 未连接</span><button class="btn btn-s btn-sm" onclick="toggleTerm()" id="termBtn">打开终端</button></div><div id="terminal"></div></div>

 <div class="footer"><p>by 童儿制作 仅供学习参考 请尊重每个人的知识产权</p><p>Glass UI Design System · NAS Stress Test Tool v5</p></div>
</div>

<div id="toastCtr"></div>

<script>
let pollTimer=null,logs=[],term=null,termWs=null,termOpen=false;

function tgl(id){document.getElementById(id).style.display=document.getElementById(id.replace('S','')).checked?'':'none'}
function addLog(msg,err){const t=new Date().toLocaleTimeString();logs.push({time:t,msg,err});if(logs.length>80)logs.shift();const b=document.getElementById('logBox');b.innerHTML=logs.map(l=>`<div${l.err?' style="color:var(--rd)"':''}>[${l.time}] ${l.msg}</div>`).join('');b.scrollTop=b.scrollHeight}
function toast(msg){const c=document.getElementById('toastCtr'),e=document.createElement('div');e.className='toast';e.textContent=msg;c.appendChild(e);setTimeout(()=>{e.style.opacity='0';e.style.transition='opacity .4s'},2200);setTimeout(()=>e.remove(),2600)}
function getSsh(){return{host:document.getElementById('sshHost').value||'192.168.3.101',port:parseInt(document.getElementById('sshPort').value)||22,user:document.getElementById('sshUser').value||'admin',password:document.getElementById('sshPass').value}}

async function sshTest(){const btn=document.getElementById('sshTestBtn'),st=document.getElementById('sshStatus');btn.disabled=true;st.textContent='连接中...';st.style.color='var(--tm)';try{const r=await fetch('/api/ssh_test',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(getSsh())});const j=await r.json();st.textContent=j.message;st.style.color=j.ok?'var(--gn)':'var(--rd)';addLog(j.message,!j.ok);toast(j.message)}catch(e){st.textContent='请求失败';st.style.color='var(--rd)'}btn.disabled=false}

async function doPower(a){const lb=a==='shutdown'?'关机':'重启';if(!confirm('确定要'+lb+' NAS？'))return;try{const r=await fetch('/api/power',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({...getSsh(),action:a})});const j=await r.json();toast(j.message);addLog(j.message,!j.ok)}catch(e){toast('失败: '+e.message)}}

function fmtN(n){if(n<1000)return ''+n;if(n<1e6)return (n/1e3).toFixed(1)+'K';if(n<1e9)return (n/1e6).toFixed(1)+'M';return (n/1e9).toFixed(2)+'B'}

async function poll(){try{const r=await fetch('/api/status');const s=(await r.json()).stats||{},st=s,m=(await(await fetch('/api/status')).json()).mode||'';updateUI((await(await fetch('/api/status')).json()))}catch(e){}}

async function _poll(){try{const r=await fetch('/api/status');updateUI(await r.json())}catch(e){}}

function updateUI(d){const s=d.stats||{},r=d.running,m=d.mode||'',b=document.getElementById('badge');if(r){b.innerHTML='<span class="dot dot-busy"></span>运行中 — '+m;b.className='badge badge-rd'}else{b.innerHTML='<span class="dot dot-off"></span>待机中';b.className='badge badge-n'}document.getElementById('stopBtn').disabled=!r;document.getElementById('sT').textContent=Math.floor(s.elapsed||0)+'s';document.getElementById('sRps').textContent=(s.http_rps||0).toFixed(0)+' rps';document.getElementById('sLat').textContent='HTTP '+(s.http_lat_avg||0).toFixed(1)+'ms';document.getElementById('sUmb').textContent='UDP '+(s.udp_mbps||0).toFixed(1)+' MB/s';document.getElementById('sWr').textContent='写 '+(s.write_mbps||0).toFixed(1)+' MB/s';document.getElementById('sRd').textContent='读 '+(s.read_mbps||0).toFixed(1)+' MB/s';document.getElementById('sWl').textContent='写延 '+(s.write_lat_avg||0).toFixed(1)+'ms';document.getElementById('sRl').textContent='读延 '+(s.read_lat_avg||0).toFixed(1)+'ms';
 const cw=document.getElementById('sCpuW');if(s.cpu_active){cw.style.display='';document.getElementById('sCpu').textContent='CPU '+(s.cpu_workers||'?')+'核 满载'}else cw.style.display='none';
 const uw=document.getElementById('sUdpW');if((s.udp_pps||0)>0||m==='udp'||(m==='custom'&&(s.udp_total||0)>0)){uw.style.display='';document.getElementById('sPps').textContent=(s.udp_pps||0)+' pps'}else uw.style.display='none';
 const parts=[];if(s.http_total>0)parts.push('HTTP: '+s.http_total+'请求/'+s.http_success+'成功/'+s.http_errors+'错');if(s.udp_total>0)parts.push('UDP: '+fmtN(s.udp_total)+'包');if(s.write_mb>0)parts.push('写:'+(s.write_mb||0).toFixed(0)+'MB 读:'+(s.read_mb||0).toFixed(0)+'MB');document.getElementById('sDetail').textContent=parts.join(' · ')||'等待数据...';
 if(s.messages&&s.messages.length)s.messages.slice(-3).forEach(m=>addLog(m,true))}

function startOne(mode){let d={};if(mode==='ssh_cpu')d={mode:'ssh_cpu',...getSsh(),workers:parseInt(document.getElementById('cpuWorkers').value)||0};else if(mode==='http')d={mode:'http',url:document.getElementById('sHttpUrl').value,concurrency:parseInt(document.getElementById('sHttpConc').value)||10,reqs_per_conn:parseInt(document.getElementById('sHttpRpc').value)||50};else if(mode==='udp')d={mode:'udp',host:document.getElementById('sUdpHost').value,port:parseInt(document.getElementById('sUdpPort').value)||9998,concurrency:parseInt(document.getElementById('sUdpConc').value)||4,payload_size:parseInt(document.getElementById('sUdpPayload').value)||1400};else if(mode==='file'){const p=document.getElementById('sFilePath').value.trim();if(!p){toast('请填写挂载路径');return}d={mode:'file',path:p,threads:parseInt(document.getElementById('sFileThreads').value)||4,file_size:parseInt(document.getElementById('sFileSize').value)||512,block_size:'1m',no_verify:false}};doStart(d)}

function startCustom(){const h=document.getElementById('cHttp').checked,u=document.getElementById('cUdp').checked,f=document.getElementById('cFile').checked,c=document.getElementById('cCpu').checked;if(!h&&!u&&!f&&!c){toast('至少选一种');return}if(f&&!document.getElementById('cFilePath').value.trim()){toast('请填写挂载路径');return};const sc=getSsh();doStart({mode:'custom',enable_http:h,http_url:'http://''' + NAS_HOST + ":" + str(NAS_HTTP_PORT) + '''/',http_concurrency:parseInt(document.getElementById('cHttpConc').value)||10,http_reqs_per_conn:parseInt(document.getElementById('cHttpRpc').value)||50,enable_udp:u,udp_host:' ''' + NAS_HOST + '''',udp_port:''' + str(UDP_PORT) + ''',udp_conc:parseInt(document.getElementById('cUdpConc').value)||2,udp_payload:parseInt(document.getElementById('cUdpPayload').value)||1400,enable_file:f,path:document.getElementById('cFilePath').value,file_threads:parseInt(document.getElementById('cFileThreads').value)||3,file_size:256,block_size:'1m',no_verify:false,enable_cpu:c,cpu_host:sc.host,cpu_port:sc.port,cpu_user:sc.user,cpu_password:sc.password,cpu_workers:parseInt(document.getElementById('cCpuWorkers').value)||0})}

async function doStart(d){try{const r=await fetch('/api/start',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(d)});const j=await r.json();toast(j.message);addLog(j.message,!j.ok);if(j.ok){if(!pollTimer)pollTimer=setInterval(_poll,1500);_poll()}}catch(e){toast('失败: '+e.message)}}

async function doStop(){try{const r=await fetch('/api/stop',{method:'POST'});const j=await r.json();toast(j.message);addLog(j.message,false);_poll()}catch(e){toast('失败: '+e.message)}}

function toggleTerm(){termOpen?closeTerm():openTerm()}
function openTerm(){if(termOpen)return;const el=document.getElementById('terminal');el.style.display='';if(!term){const bg=getComputedStyle(document.body).getPropertyValue('--bg').trim()||'#0a0a1a';const fg=getComputedStyle(document.body).getPropertyValue('--t').trim()||'#e2e8f0';const ac=getComputedStyle(document.body).getPropertyValue('--ac').trim()||'#06b6d4';term=new Terminal({cursorBlink:true,fontSize:13,fontFamily:'SF Mono,Cascadia Code,Consolas,monospace',theme:{background:bg,foreground:fg,cursor:ac},cols:120,rows:20});term.open(el);term.onData(d=>{if(termWs&&termWs.readyState===WebSocket.OPEN)termWs.send(d)});term.onResize(({cols,rows})=>{if(termWs&&termWs.readyState===WebSocket.OPEN)termWs.send(JSON.stringify({type:'resize',w:cols,h:rows}))})}connectTerm();termOpen=true;document.getElementById('termBtn').textContent='关闭终端';document.getElementById('termTitle').textContent='终端 — 连接中...'}
function closeTerm(){if(termWs){try{termWs.send(JSON.stringify({type:'disconnect'}));termWs.close()}catch(e){};termWs=null}termOpen=false;document.getElementById('termBtn').textContent='打开终端';document.getElementById('termTitle').textContent='终端 — 未连接'}
function connectTerm(){if(termWs){try{termWs.close()}catch(e){}}const sc=getSsh(),proto=location.protocol==='https:'?'wss':'ws';termWs=new WebSocket(proto+'://'+location.host+'/ws/terminal');termWs.onopen=()=>{termWs.send(JSON.stringify({type:'connect',...sc}))};termWs.onmessage=e=>{try{const d=JSON.parse(e.data);if(d.type==='connected'){document.getElementById('termTitle').textContent='终端 — '+sc.user+'@'+sc.host;addLog('终端已连接',false)}else if(d.type==='error'){addLog('终端: '+d.msg,true)}}catch(_){if(term)term.write(e.data)}};termWs.onclose=()=>{document.getElementById('termTitle').textContent='终端 — 已断开';termWs=null};termWs.onerror=()=>{document.getElementById('termTitle').textContent='终端 — 连接失败';addLog('WebSocket 连接失败',true)}}

addLog('NAS 烤机 v5 就绪 — ''' + NAS_HOST + ":" + str(NAS_HTTP_PORT) + '''',false);_poll();if(!pollTimer)pollTimer=setInterval(_poll,1500);
</script>
</body>
</html>'''


def main():
    import webbrowser
    print("\n" + "=" * 54)
    print("  NAS 烤机 & 压测 v5")
    print("  by 童儿制作 仅供学习参考")
    print("=" * 54)
    print(f"  目标: {NAS_HOST}:{NAS_HTTP_PORT}")
    print(f"  控制台: http://{WEB_HOST}:{WEB_PORT}")
    print(f"  SSH: {SSH_USER}@{NAS_HOST}:{SSH_PORT}")
    if not HAS_SSH: print("  [!] paramiko 未安装，SSH/CPU/终端不可用: pip install paramiko")
    if not HAS_SOCK: print("  [!] flask-sock 未安装，终端不可用: pip install flask-sock")
    print()
    try: webbrowser.open(f"http://{WEB_HOST}:{WEB_PORT}")
    except: pass
    app.run(host=WEB_HOST, port=WEB_PORT, debug=False, threaded=True)


if __name__ == "__main__":
    main()
