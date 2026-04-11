"""
launcher.py — SeaTalk 进程管理
1. 若 SeaTalk 未运行则启动
2. 通过 SIGUSR1 + Node Inspector 注入 CDP 代理
3. 等待用户确认登录完成
4. 验证 CDP 连接可用
"""

import subprocess
import sys
import termios
import time
import tty

import requests

from cdp_injector import CDPInjector
from logger import get_logger

log = get_logger()


# ---------------------------------------------------------------------------
# 进程管理
# ---------------------------------------------------------------------------

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


def _wait_process_exit(timeout: int = 10) -> None:
    """等待所有 SeaTalk 进程完全退出"""
    for i in range(timeout):
        result = subprocess.run(
            ["pgrep", "-f", "SeaTalk"],
            capture_output=True,
            timeout=5,
        )
        if result.returncode != 0:
            log.debug(f"SeaTalk 进程已全部退出（等待 {i + 1}s）")
            return
        time.sleep(1)
    log.warning(f"等待 {timeout}s 后仍有 SeaTalk 进程残留，继续启动")


def _is_seatalk_running() -> bool:
    """检查 SeaTalk 是否正在运行"""
    result = subprocess.run(
        ["pgrep", "-x", "SeaTalk"],
        capture_output=True,
        timeout=5,
    )
    return result.returncode == 0


def _launch_seatalk() -> None:
    """通过 macOS open 命令正常启动 SeaTalk（无需任何特殊参数）"""
    subprocess.Popen(
        ["open", "-a", "/Applications/SeaTalk.app"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    log.debug("SeaTalk launched via 'open -a'")


# ---------------------------------------------------------------------------
# CDP 检测
# ---------------------------------------------------------------------------

def _is_seatalk_page(target: dict) -> bool:
    """判断 CDP target 是否为 SeaTalk 主页面"""
    if target.get("type") != "page":
        return False
    url = target.get("url", "")
    title = target.get("title", "")
    return "haiserve.com" in url or "SeaTalk" in title


def _wait_for_cdp(port: int, timeout: int = 15) -> bool:
    """轮询 CDP /json 接口，直到 SeaTalk 渲染进程就绪"""
    url = f"http://localhost:{port}/json"
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = requests.get(url, timeout=2)
            targets = resp.json()
            pages = [t for t in targets if _is_seatalk_page(t)]
            if pages:
                log.debug(
                    f"CDP ready, found {len(pages)} SeaTalk page target(s)"
                )
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def _seatalk_cdp_ready(port: int) -> bool:
    """检查 SeaTalk 是否已在 CDP 模式运行"""
    try:
        resp = requests.get(f"http://localhost:{port}/json", timeout=3)
        targets = resp.json()
        return any(_is_seatalk_page(t) for t in targets)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# 用户交互
# ---------------------------------------------------------------------------

def _wait_for_keypress() -> bool:
    """
    等待单个按键：
      Enter → 返回 True（继续）
      Esc / Ctrl+C → 返回 False（退出）
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
    return ch not in ("\x1b", "\x03")


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------

def restart_seatalk(port: int) -> None:
    """启动 SeaTalk → 等待登录 → SIGUSR1 注入 CDP 代理 → 验证。

    如果 CDP 已可用则直接跳过。
    """
    if _seatalk_cdp_ready(port):
        log.info(f"检测到 CDP 端口 {port} 已可用，跳过重启")
        return

    if not _is_seatalk_running():
        log.info("SeaTalk 未运行，正在启动...")
        _launch_seatalk()
    else:
        log.info("SeaTalk 已在运行")

    print()
    print("=" * 60)
    print("  请完成以下步骤：")
    print("  1. 等待 SeaTalk 界面加载完成")
    print("  2. 登录您的账号（如已自动登录请确认界面正常）")
    print("  3. 确认聊天列表已加载")
    print("     按 Enter 继续采集 / 按 Esc 退出程序")
    print("=" * 60)
    if not _wait_for_keypress():
        log.info("用户按 Esc，程序退出")
        sys.exit(0)

    log.info("正在通过 SIGUSR1 注入 CDP 代理...")
    injector = CDPInjector(inspector_port=9229, cdp_port=port)
    if not injector.ensure_cdp(process_name="SeaTalk"):
        raise RuntimeError(
            f"CDP 代理注入失败（端口 {port}），"
            "请确认 SeaTalk 已正常启动并已登录。"
        )
    log.info(f"CDP 代理已就绪: localhost:{port}")
