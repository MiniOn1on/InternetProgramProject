"""1.2 Sleep 版：心跳服务端。

不用 selectors，也不用为每台设备单开定时线程：
    处理线程   一个连接一个，recv 设 1 s 超时，超时返回就转一圈（顺便看停止标志）
    巡检线程   全局唯一的“定时器”，每秒扫一遍在线表，超过 3T 没心跳就摘掉

运行：
    python server.py            # 先起它
    python client.py            # 再起客户端
"""
from __future__ import annotations

import argparse
import json
import socket
import threading
import time

import protocol

ONLINE = "ONLINE"
UNSTABLE = "UNSTABLE"
OFFLINE = "OFFLINE"
UNKNOWN = "UNKNOWN"
GOOD = "GOOD"
BAD = "BAD"

devices: dict[str, "Device"] = {}
devices_lock = threading.Lock()
stop_evt = threading.Event()
threads: list[threading.Thread] = []

PERIOD = 5.0        # 客户端约定的心跳周期
MAX_MISS = 3        # 连续 N 帧才判离线
CHECK_INTERVAL = 1.0   # 巡检间隔（也就是超时判定精度）
MUTE_AFTER = 0      # >0 时：每台设备回了这么多帧 ACK 之后就装死，用来演示降级


def stamp() -> str:
    return time.strftime("%H:%M:%S", time.localtime())


class Device:
    """一台设备的在线记录，在线表是唯一的事实来源。"""

    def __init__(self, device_id: str, now: float) -> None:
        self.device_id = device_id
        self.conn: socket.socket | None = None
        self.addr = None
        self.first_seen = now
        self.last_seen = now
        self.state = UNSTABLE
        self.unstable_from = OFFLINE
        self.good_streak = 0
        self.bad_streak = 0
        self.last_report = UNKNOWN
        self.last_rtt_ms: int | None = None
        self.heartbeats = 0


# ---------------------------------------------------------------- 协议层
def classify(prev_seq, prev_rtt, seq: int) -> tuple[str, int | None]:
    """判断客户端上报的“上一个周期”是否可达。"""
    if not isinstance(prev_seq, int) or prev_seq != seq - 1:
        return UNKNOWN, None          # 重连后首帧 / 跳号：不下结论
    if not isinstance(prev_rtt, int):
        return UNKNOWN, None
    if prev_rtt < 0:
        return BAD, None              # -1：那个周期没收到 ACK
    return GOOD, prev_rtt


def build_ack(seq: int) -> bytes:
    return protocol.encode({"type": "ack", "seq": seq,
                            "ts": int(time.time() * 1000)})


# ---------------------------------------------------------------- 状态层
def on_heartbeat(dev: Device, result: str, rtt: int | None, now: float) -> None:
    dev.last_seen = now
    dev.heartbeats += 1
    dev.last_report = result
    if rtt is not None:
        dev.last_rtt_ms = rtt

    old = dev.state
    if result == GOOD:
        dev.bad_streak = 0
        dev.good_streak += 1
        if dev.state == OFFLINE:
            dev.state, dev.unstable_from = UNSTABLE, OFFLINE
        elif dev.state == UNSTABLE:
            if dev.unstable_from == ONLINE or dev.good_streak >= MAX_MISS:
                dev.state, dev.unstable_from = ONLINE, OFFLINE
    elif result == BAD:
        dev.good_streak = 0
        dev.bad_streak += 1
        if dev.state == ONLINE:
            dev.state, dev.unstable_from = UNSTABLE, ONLINE
        elif dev.state == UNSTABLE and dev.bad_streak >= MAX_MISS:
            dev.state = OFFLINE
    # UNKNOWN：只刷新 last_seen，不改状态

    if old != dev.state:
        print(f"{stamp()} [dev {dev.device_id}] {old} -> {dev.state}"
              f"（{result}，rtt={dev.last_rtt_ms}，心跳 {dev.heartbeats} 次）")


def check_timeout(dev: Device, now: float, period: float) -> bool:
    if dev.state == OFFLINE:
        return False
    if now - dev.last_seen > 3 * period:
        dev.state = OFFLINE
        return True
    return False


# ---------------------------------------------------------------- 传输层
def handle_client(conn: socket.socket, addr) -> None:
    conn.settimeout(CHECK_INTERVAL)       # 处理线程的计时精度就是这 1 s
    buf = bytearray()
    dev: Device | None = None
    try:
        while not stop_evt.is_set():
            try:
                chunk = conn.recv(4096)
            except socket.timeout:
                continue                  # 超时不是错误：回去看 stop_evt
            except OSError:
                break                     # 连接已坏
            if not chunk:
                break                     # 对端关闭

            buf.extend(chunk)
            try:
                frames = protocol.decode(buf)
            except (ValueError, json.JSONDecodeError) as exc:
                print(f"{stamp()} [srv] {addr} 协议错误：{exc}")
                break

            for frame in frames:
                if frame.get("type") != "hb":
                    continue
                dev = dispatch_heartbeat(conn, addr, frame)
    finally:
        drop_device(dev.device_id if dev else None, conn)


def dispatch_heartbeat(conn: socket.socket, addr, msg: dict) -> Device | None:
    device_id = msg.get("device_id") or msg.get("ecu_id")
    if device_id is None:
        return None
    device_id = str(device_id)
    now = time.monotonic()

    with devices_lock:
        dev = devices.get(device_id)
        if dev is None:
            dev = Device(device_id, now)
            devices[device_id] = dev
            print(f"{stamp()} [srv] 新设备 {device_id} 来自 {addr}")
        elif dev.conn is not conn:                    # 同一设备重连：关旧连接
            try:
                dev.conn.close()
            except OSError:
                pass
        dev.conn = conn
        dev.addr = addr
        result, rtt = classify(msg.get("prev_seq"), msg.get("prev_rtt"), msg.get("seq"))
        on_heartbeat(dev, result, rtt, now)

    if MUTE_AFTER and dev.heartbeats > MUTE_AFTER:
        print(f"{stamp()} [srv] 演示模式：不再回设备 {device_id} 的 ACK（心跳 {dev.heartbeats}）")
        return dev

    try:
        conn.sendall(build_ack(msg["seq"]))            # 锁外发送
    except OSError:
        pass
    return dev


def drop_device(device_id: str | None, conn: socket.socket) -> None:
    if device_id is None:
        return
    with devices_lock:
        dev = devices.get(device_id)
        if dev is None or dev.conn is not conn:        # 旧连接不动新连接
            return
        dev.conn = None
        dev.state = OFFLINE
        devices.pop(device_id, None)
    print(f"{stamp()} [srv] 设备 {device_id} 已移出在线表")
    try:
        conn.close()
    except OSError:
        pass


# ---------------------------------------------------------------- 并发层
def sweeper() -> None:
    """服务端唯一的“定时器”。"""
    while not stop_evt.is_set():
        now = time.monotonic()
        with devices_lock:
            for dev in list(devices.values()):
                if check_timeout(dev, now, PERIOD):
                    print(f"{stamp()} [srv] 设备 {dev.device_id} 超过 {3 * PERIOD:.0f}s 无心跳，判离线")
                    drop_device(dev.device_id, dev.conn)
        time.sleep(CHECK_INTERVAL)


def accept_loop(listener: socket.socket) -> None:
    while not stop_evt.is_set():
        try:
            conn, addr = listener.accept()
        except socket.timeout:
            continue
        except OSError:
            break
        print(f"{stamp()} [srv] 连接来自 {addr}")
        t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
        t.start()
        threads.append(t)


def main() -> None:
    global PERIOD, MAX_MISS, MUTE_AFTER
    ap = argparse.ArgumentParser(description="1.2 Sleep 版心跳服务端")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--period", type=float, default=5.0, help="客户端心跳周期，用来算 3T")
    ap.add_argument("--miss", type=int, default=3)
    ap.add_argument("--mute-after", type=int, default=0,
                    help="演示用：每台设备回这么多帧 ACK 之后就不再回，观察客户端降级")
    args = ap.parse_args()
    PERIOD, MAX_MISS, MUTE_AFTER = args.period, args.miss, args.mute_after

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((args.host, args.port))
    listener.listen(16)
    listener.settimeout(0.5)                  # 让 accept 能定期回来看 stop_evt

    print(f"心跳服务端启动：{args.host}:{args.port}  T={PERIOD}s  3T={3 * PERIOD:.0f}s"
          + (f"  演示：回 {MUTE_AFTER} 帧后装死" if MUTE_AFTER else ""))
    threading.Thread(target=sweeper, name="sweeper", daemon=True).start()
    try:
        accept_loop(listener)
    except KeyboardInterrupt:
        print("\n收到 Ctrl+C，退出")
    finally:
        stop_evt.set()
        listener.close()
        for t in threads:
            t.join(timeout=1.0)


if __name__ == "__main__":
    main()
