"""CDP 代理注入器 — 通过 SIGUSR1 + Node Inspector 向 Electron 进程注入 CDP 代理。

新版 Electron 应用（如 SeaTalk 2.9.3+）封堵了 --remote-debugging-port 启动参数，
本模块通过以下步骤恢复 CDP 访问：

1. 向 Electron 主进程发送 SIGUSR1，触发 Node.js Inspector 在 9229 端口监听
2. 通过 Inspector WebSocket 在主进程中注入一段 JS，该 JS 利用 Electron 的
   webContents.debugger API 创建一个 HTTP+WebSocket 代理服务器
3. 代理服务器在 9222 端口暴露标准 CDP 协议，外部工具可正常连接
"""

from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import time
import urllib.request
from typing import Optional

from logger import get_logger

logger = get_logger()

try:
    import websocket

    _WS_AVAILABLE = True
except ImportError:
    _WS_AVAILABLE = False


def _build_cdp_proxy_js(cdp_port: int) -> str:
    """生成注入到 Electron 主进程的 CDP 代理 JS 代码。

    代理在 Electron 进程内部运行，将标准 CDP 协议请求桥接到
    webContents.debugger.sendCommand，同时转发 debugger 事件给客户端。
    """
    return r"""
(function() {
    var CDP_PORT = """ + str(cdp_port) + r""";
    if (globalThis.__cdpProxyRunning) return 'CDP proxy already running on port ' + CDP_PORT;

    var http = require('http');
    var crypto = require('crypto');
    var electron = require('electron');
    var wins = electron.BrowserWindow.getAllWindows();
    var mainWin = wins.find(function(w) {
        return w.webContents.getURL().indexOf('about:blank') === -1;
    }) || wins[0];
    var wc = mainWin.webContents;

    if (!wc.debugger.isAttached()) {
        wc.debugger.attach('1.3');
    }

    var wsClients = [];

    wc.debugger.on('message', function(event, method, params) {
        var msg = JSON.stringify({ method: method, params: params });
        wsClients.forEach(function(c) {
            try { c.send(msg); } catch(e) {}
        });
    });

    ['Page', 'Network', 'Runtime', 'DOM', 'CSS', 'Log', 'Console'].forEach(function(domain) {
        wc.debugger.sendCommand(domain + '.enable', {}).catch(function(){});
    });

    function parseWsFrames(buffer, onMessage) {
        while (buffer.length >= 2) {
            var firstByte = buffer[0];
            var masked = (buffer[1] & 0x80) !== 0;
            var len = buffer[1] & 0x7f;
            var offset = 2;
            if (len === 126) {
                if (buffer.length < 4) break;
                len = buffer.readUInt16BE(2);
                offset = 4;
            } else if (len === 127) {
                if (buffer.length < 10) break;
                len = Number(buffer.readBigUInt64BE(2));
                offset = 10;
            }
            var maskOffset = masked ? 4 : 0;
            var totalLen = offset + maskOffset + len;
            if (buffer.length < totalLen) break;

            var payload;
            if (masked) {
                var mask = buffer.slice(offset, offset + 4);
                payload = Buffer.from(buffer.slice(offset + 4, offset + 4 + len));
                for (var i = 0; i < payload.length; i++) payload[i] ^= mask[i % 4];
            } else {
                payload = buffer.slice(offset, offset + len);
            }
            buffer = buffer.slice(totalLen);

            var opcode = firstByte & 0x0f;
            if (opcode === 0x01) {
                onMessage(payload.toString());
            } else if (opcode === 0x08) {
                return { buffer: buffer, closed: true };
            }
        }
        return { buffer: buffer, closed: false };
    }

    function makeWsFrame(data) {
        var buf = Buffer.from(data);
        var header;
        if (buf.length < 126) {
            header = Buffer.alloc(2);
            header[0] = 0x81;
            header[1] = buf.length;
        } else if (buf.length < 65536) {
            header = Buffer.alloc(4);
            header[0] = 0x81;
            header[1] = 126;
            header.writeUInt16BE(buf.length, 2);
        } else {
            header = Buffer.alloc(10);
            header[0] = 0x81;
            header[1] = 127;
            header.writeBigUInt64BE(BigInt(buf.length), 2);
        }
        return Buffer.concat([header, buf]);
    }

    var wsUrl = 'ws://localhost:' + CDP_PORT + '/devtools/page/1';

    var server = http.createServer(function(req, res) {
        res.setHeader('Content-Type', 'application/json');
        res.setHeader('Access-Control-Allow-Origin', '*');
        if (req.url === '/json/version') {
            res.end(JSON.stringify({
                Browser: 'Electron/' + process.versions.electron,
                'Protocol-Version': '1.3',
                'User-Agent': wc.getUserAgent(),
                'V8-Version': process.versions.v8,
                webSocketDebuggerUrl: wsUrl
            }));
        } else if (req.url === '/json' || req.url === '/json/list') {
            res.end(JSON.stringify([{
                description: '',
                devtoolsFrontendUrl: 'devtools://devtools/bundled/inspector.html?experiments=true&ws=localhost:' + CDP_PORT + '/devtools/page/1',
                id: '1',
                title: wc.getTitle(),
                type: 'page',
                url: wc.getURL(),
                webSocketDebuggerUrl: wsUrl
            }]));
        } else {
            res.statusCode = 404;
            res.end('{}');
        }
    });

    server.on('upgrade', function(req, socket, head) {
        var key = req.headers['sec-websocket-key'];
        var magic = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
        var accept = crypto.createHash('sha1').update(key + magic).digest('base64');
        socket.write(
            'HTTP/1.1 101 Switching Protocols\r\n' +
            'Upgrade: websocket\r\n' +
            'Connection: Upgrade\r\n' +
            'Sec-WebSocket-Accept: ' + accept + '\r\n\r\n'
        );

        var client = {
            send: function(data) {
                try { socket.write(makeWsFrame(data)); } catch(e) {}
            }
        };
        wsClients.push(client);

        var buf = Buffer.alloc(0);
        if (head && head.length > 0) buf = head;

        socket.on('data', function(chunk) {
            buf = Buffer.concat([buf, chunk]);
            var result = parseWsFrames(buf, function(text) {
                try {
                    var msg = JSON.parse(text);
                    wc.debugger.sendCommand(msg.method, msg.params || {}).then(function(res) {
                        client.send(JSON.stringify({ id: msg.id, result: res }));
                    }).catch(function(err) {
                        client.send(JSON.stringify({ id: msg.id, error: { message: err.message, code: -32000 } }));
                    });
                } catch(e) {}
            });
            buf = result.buffer;
            if (result.closed) socket.end();
        });

        socket.on('close', function() {
            var idx = wsClients.indexOf(client);
            if (idx !== -1) wsClients.splice(idx, 1);
        });
        socket.on('error', function() {
            var idx = wsClients.indexOf(client);
            if (idx !== -1) wsClients.splice(idx, 1);
        });
    });

    server.listen(CDP_PORT, '127.0.0.1', function() {
        globalThis.__cdpProxyRunning = true;
    });
    return 'CDP proxy started on ' + wsUrl;
})()
"""


class CDPInjector:
    """通过 Node Inspector 向 Electron 进程注入 CDP 代理。

    典型用法::

        injector = CDPInjector(inspector_port=9229, cdp_port=9222)
        if injector.ensure_cdp(process_name="SeaTalk"):
            # CDP 已在 localhost:9222 可用
            ...
    """

    def __init__(self, inspector_port: int = 9229, cdp_port: int = 9222):
        self._inspector_port = inspector_port
        self._cdp_port = cdp_port

    @property
    def inspector_url(self) -> str:
        return f"http://localhost:{self._inspector_port}"

    @property
    def cdp_url(self) -> str:
        return f"http://localhost:{self._cdp_port}"

    # ------------------------------------------------------------------
    # Step 1: SIGUSR1
    # ------------------------------------------------------------------

    def send_sigusr1(self, process_name: str = "SeaTalk") -> bool:
        """向目标进程发送 SIGUSR1 信号以开启 Node Inspector。"""
        pid = self._find_pid(process_name)
        if not pid:
            logger.warning("未找到进程: %s", process_name)
            return False
        try:
            os.kill(pid, signal.SIGUSR1)
            logger.info("已发送 SIGUSR1 至 %s (PID %d)", process_name, pid)
            return True
        except OSError as e:
            logger.warning("发送 SIGUSR1 失败 (PID %d): %s", pid, e)
            return False

    @staticmethod
    def _find_pid(process_name: str) -> Optional[int]:
        """通过 pgrep 获取主进程 PID。"""
        try:
            result = subprocess.run(
                ["pgrep", "-x", process_name],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0 and result.stdout.strip():
                return int(result.stdout.strip().splitlines()[0])
        except (subprocess.TimeoutExpired, ValueError):
            pass
        return None

    # ------------------------------------------------------------------
    # Step 2: Node Inspector 检测
    # ------------------------------------------------------------------

    def is_inspector_available(self) -> bool:
        """检查 Node Inspector 端口是否可用。"""
        try:
            urllib.request.urlopen(f"{self.inspector_url}/json/list", timeout=2)
            return True
        except Exception:
            return False

    def _wait_for_inspector(self, timeout: float = 5) -> bool:
        """等待 Node Inspector 端口就绪。"""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.is_inspector_available():
                return True
            time.sleep(0.3)
        return False

    # ------------------------------------------------------------------
    # Step 3: 注入 CDP 代理
    # ------------------------------------------------------------------

    def inject_cdp_proxy(self) -> bool:
        """连接 Node Inspector WebSocket 并注入 CDP 代理 JS。

        注入成功后，标准 CDP 协议将在 cdp_port 上可用。
        """
        if not _WS_AVAILABLE:
            logger.warning("websocket-client 未安装，无法注入 CDP 代理")
            return False

        ws_url = self._get_inspector_ws_url()
        if not ws_url:
            logger.warning("无法获取 Node Inspector WebSocket URL")
            return False

        try:
            ws = websocket.create_connection(ws_url, timeout=10)
        except Exception as e:
            logger.warning("Node Inspector WebSocket 连接失败: %s", e)
            return False

        try:
            return self._do_inject(ws)
        finally:
            try:
                ws.close()
            except Exception:
                pass

    def _get_inspector_ws_url(self) -> Optional[str]:
        try:
            data = urllib.request.urlopen(
                f"{self.inspector_url}/json/list", timeout=5,
            ).read()
            targets = json.loads(data)
            if targets:
                return targets[0].get("webSocketDebuggerUrl")
        except Exception as e:
            logger.debug("获取 Inspector targets 失败: %s", e)
        return None

    def _do_inject(self, ws) -> bool:
        """通过已连接的 Inspector WebSocket 执行注入。"""
        ws.send(json.dumps({"id": 0, "method": "Runtime.enable"}))
        self._recv_until_id(ws, 0)

        js_code = _build_cdp_proxy_js(self._cdp_port)
        ws.send(json.dumps({
            "id": 1,
            "method": "Runtime.evaluate",
            "params": {
                "expression": js_code,
                "returnByValue": True,
                "includeCommandLineAPI": True,
            },
        }))

        resp = self._recv_until_id(ws, 1)
        if resp is None:
            logger.warning("注入 CDP 代理无响应")
            return False

        result = resp.get("result", {}).get("result", {})
        if "exceptionDetails" in resp.get("result", {}):
            exc = resp["result"]["exceptionDetails"]
            logger.warning("注入 CDP 代理 JS 异常: %s", exc.get("text", ""))
            return False

        value = result.get("value", "")
        logger.info("CDP 代理注入结果: %s", value)
        return "CDP proxy" in str(value)

    @staticmethod
    def _recv_until_id(ws, target_id: int, timeout: float = 10) -> Optional[dict]:
        """从 Inspector WebSocket 接收消息直到匹配指定 id。"""
        deadline = time.monotonic() + timeout
        ws.settimeout(1.0)
        while time.monotonic() < deadline:
            try:
                raw = ws.recv()
                msg = json.loads(raw)
                if msg.get("id") == target_id:
                    return msg
            except websocket.WebSocketTimeoutException:
                continue
            except Exception:
                break
        return None

    # ------------------------------------------------------------------
    # Step 4: 验证 CDP 可用
    # ------------------------------------------------------------------

    def is_cdp_available(self) -> bool:
        """检查 CDP 代理端口是否可用。"""
        try:
            urllib.request.urlopen(f"{self.cdp_url}/json", timeout=2)
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------
    # 一站式方法
    # ------------------------------------------------------------------

    def ensure_cdp(self, process_name: str = "SeaTalk") -> bool:
        """一站式方法：SIGUSR1 → 等待 Inspector → 注入代理 → 验证 CDP。

        如果 CDP 端口已可用则直接返回 True（幂等）。
        """
        if self.is_cdp_available():
            logger.debug("CDP 端口 %d 已可用，跳过注入", self._cdp_port)
            return True

        if not self.send_sigusr1(process_name):
            return False

        if not self._wait_for_inspector(timeout=5):
            logger.warning(
                "SIGUSR1 已发送但 Node Inspector 端口 %d 未就绪",
                self._inspector_port,
            )
            return False

        if not self.inject_cdp_proxy():
            return False

        time.sleep(0.5)
        if self.is_cdp_available():
            logger.info("CDP 代理已就绪: %s", self.cdp_url)
            return True

        logger.warning("CDP 代理注入后端口 %d 仍不可用", self._cdp_port)
        return False
