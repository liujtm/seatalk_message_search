"""
launcher.py — SeaTalk 进程管理
1. 关闭已运行的 SeaTalk
2. 以调试模式重新启动
3. 等待用户确认登录完成
4. 验证 CDP 连接可用
"""

import subprocess
import sys
import termios
import time
import tty

import requests

from logger import get_logger

log = get_logger()


def _quit_seatalk_gracefully() -> None:
    """用 osascript 优雅退出 SeaTalk"""
    try:
        subprocess.run(
            ["osascript", "-e", 'quit app "SeaTalk"'],
            capture_output=True,
            timeout=5,
        )
        log.debug("osascript quit sent")
    except Exception as e:
        log.debug(f"osascript quit failed (may not be running): {e}")


def _kill_seatalk_forcefully() -> None:
    """强杀残留 SeaTalk 进程"""
    try:
        result = subprocess.run(
            ["pkill", "-f", "SeaTalk"],
            capture_output=True,
            timeout=5,
        )
        if result.returncode == 0:
            log.debug("pkill SeaTalk OK")
    except Exception as e:
        log.debug(f"pkill failed: {e}")


def _launch_seatalk(port: int) -> None:
    """以调试模式启动 SeaTalk"""
    subprocess.Popen(
        [
            "open",
            "-a",
            "SeaTalk",
            "--args",
            f"--remote-debugging-port={port}",
            "--remote-allow-origins=*",
        ]
    )
    log.debug(f"SeaTalk launched with CDP port {port}")


def _wait_for_cdp(port: int, timeout: int = 30) -> bool:
    """轮询 CDP /json 接口，直到 SeaTalk 渲染进程就绪"""
    url = f"http://localhost:{port}/json"
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = requests.get(url, timeout=2)
            targets = resp.json()
            seatalk_pages = [
                t for t in targets
                if t.get("type") == "page" and "SeaTalk" in t.get("title", "")
            ]
            if seatalk_pages:
                log.debug(f"CDP ready, found {len(seatalk_pages)} SeaTalk page target(s)")
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def _wait_for_keypress() -> bool:
    """
    等待单个按键：
      Enter → 返回 True（继续）
      Esc / Ctrl+C → 返回 False（退出）
    非交互模式（stdin 非 tty）时自动继续。
    """
    if not sys.stdin.isatty():
        log.debug("stdin 非 tty，自动继续采集")
        return True
    fd = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        ch = sys.stdin.read(1)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)
    return ch not in ("\x1b", "\x03")  # ESC=0x1b  Ctrl+C=0x03


def _seatalk_cdp_ready(port: int) -> bool:
    """
    用 lsof 检查指定端口是否已被 SeaTalk 进程监听，
    同时确认 CDP /json 接口可正常访问。
    两者同时满足才认为 CDP 已就绪，无需重启。
    """
    try:
        result = subprocess.run(
            ["lsof", "-i", f":{port}", "-sTCP:LISTEN", "-n", "-P"],
            capture_output=True, text=True, timeout=5,
        )
        if "SeaTalk" not in result.stdout:
            return False
        # 端口被 SeaTalk 占用，再验证 CDP HTTP 接口是否可用
        resp = requests.get(f"http://localhost:{port}/json", timeout=3)
        targets = resp.json()
        return any(
            t.get("type") == "page" and "SeaTalk" in t.get("title", "")
            for t in targets
        )
    except Exception:
        return False


def restart_seatalk(port: int) -> None:
    """关闭→启动→等待登录→验证流程；若 SeaTalk 已以 CDP 模式运行则跳过重启。"""
    if _seatalk_cdp_ready(port):
        log.info(f"检测到 SeaTalk 已在 CDP 端口 {port} 运行，跳过重启")
        return

    log.info("SeaTalk 正在关闭（如已运行）...")
    _quit_seatalk_gracefully()
    time.sleep(2)
    _kill_seatalk_forcefully()
    time.sleep(1)

    log.info(f"正在以调试模式启动 SeaTalk（CDP 端口 {port}）...")
    _launch_seatalk(port)

    print()
    print("=" * 60)
    print("  SeaTalk 已启动，请完成以下步骤：")
    print("  1. 等待 SeaTalk 界面加载完成")
    print("  2. 登录您的账号（如已自动登录请确认界面正常）")
    print("  3. 确认聊天列表已加载")
    print("     按 Enter 继续采集 / 按 Esc 退出程序")
    print("=" * 60)
    if not _wait_for_keypress():
        log.info("用户按 Esc，程序退出")
        sys.exit(0)

    log.info("正在验证 CDP 连接...")
    if not _wait_for_cdp(port, timeout=15):
        raise RuntimeError(
            f"无法连接到 SeaTalk CDP 调试接口（端口 {port}），"
            "请确认 SeaTalk 已正常启动并已登录。"
        )
    log.info("CDP 连接验证成功")
