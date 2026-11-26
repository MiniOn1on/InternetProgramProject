"""服务端：二态（client 在 server 眼中的状态）+ ACK + 看门狗。

规则
----
ONLINE  ：距最后一次收到 DATA/HEARTBEAT 不超过 3×T
OFFLINE ：超过 3×T 没收到包
          再超过 3×T 仍无包 -> 关闭连接（设备记录保留）
OFFLINE + 任一合法包 -> 立即 ONLINE

T 从客户端每帧的 period_ms 取；每个合法包都回 ACK。
"""

from __future__ import annotations

import argparse
import json
import socket
import threading
import time
from dataclasses import dataclass

from websockets.exceptions import ConnectionClosed
from websockets.sync.server import serve

ONLINE = "ONLINE"
OFFLINE = "OFFLINE"


@dataclass
class Device:
    device_id: str
    period: float = 5.0
    state: str = OFFLINE
    last_seen: float = 0.0
    offline_since: float | None = None
    conn: object | None = None
    packets: int = 0
    data_packets: int = 0
    heartbeat_packets: int = 0
    last_payload: dict | None = None
    last_reason: str = "首次"
    closed: bool = False   # 离线满 6T 后已关过连接，避免重复检查


class WSServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 0, *,
                 default_period: float = 5.0,
                 log=print) -> None:
        self.host = host
        self.default_period = default_period
        self.log = log

        self.stats = {"connections": 0, "packets": 0,
                      "data_packets": 0, "heartbeat_packets": 0,
                      "acks": 0, "muted": 0,
                      "online_events": 0, "offline_events": 0, "closes": 0}

        self._stop = threading.Event()
        self._mute = threading.Event()
        self._lock = threading.Lock()
        self._devices: dict[str, Device] = {}
        self._conns: set = set()

        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((host, port))
        listener.listen(8)
        self.port = listener.getsockname()[1]
        self._listener = listener
        self._server = serve(self._handle, sock=listener,
                             ping_interval=None, close_timeout=1)
        self._threads: list[threading.Thread] = []

    # ------------------------------------------------------------ 生命周期

    def start(self) -> int:
        for target, name in ((self._server.serve_forever, "ws-accept"),
                             (self._watchdog, "ws-watchdog")):
            thread = threading.Thread(target=target, name=name, daemon=True)
            thread.start()
            self._threads.append(thread)
        self.log(f"[server] listening ws://{self.host}:{self.port}/")
        return self.port

    def stop(self) -> None:
        self._stop.set()
        try:
            self._server.shutdown(close_connections=True)
        except Exception:
            pass
        for thread in self._threads:
            thread.join(timeout=2.0)
        self._threads.clear()

    def set_mute(self, muted: bool) -> None:
        if muted:
            self._mute.set()
        else:
            self._mute.clear()
        self.log(f"[server] mute={'ON' if muted else 'OFF'}")

    def devices(self) -> list[dict]:
        now = time.monotonic()
        with self._lock:
            return [{
                "device_id": d.device_id,
                "state": d.state,
                "silent_s": round(now - d.last_seen, 2),
                "period_s": d.period,
                "timeout_s": round(3 * d.period, 2),
                "offline_for_s": (round(now - d.offline_since, 2)
                                  if d.offline_since is not None else None),
                "packets": d.packets,
                "data_packets": d.data_packets,
                "heartbeat_packets": d.heartbeat_packets,
                "connected": d.conn is not None,
                "reason": d.last_reason,
            } for d in sorted(self._devices.values(),
                              key=lambda x: x.device_id)]

    # ------------------------------------------------------------ 收包

    def _handle(self, conn) -> None:
        with self._lock:
            self._conns.add(conn)
            self.stats["connections"] += 1
        try:
            while not self._stop.is_set():
                try:
                    raw = conn.recv(timeout=0.3)
                except TimeoutError:
                    continue
                except ConnectionClosed:
                    break
                try:
                    msg = json.loads(raw)
                except (ValueError, TypeError):
                    continue
                if isinstance(msg, dict) and msg.get("type") in ("data", "heartbeat"):
                    self._on_packet(conn, msg)
        finally:
            self._drop_connection(conn)

    def _drop_connection(self, conn) -> None:
        with self._lock:
            self._conns.discard(conn)
            for device in self._devices.values():
                if device.conn is conn:
                    device.conn = None

    def _on_packet(self, conn, msg: dict) -> None:
        device_id = msg.get("device_id")
        seq = msg.get("seq")
        if not isinstance(device_id, (str, int)) or not isinstance(seq, int):
            return
        device_id = str(device_id)
        now = time.monotonic()

        became_online = False
        with self._lock:
            device = self._devices.get(device_id)
            if device is None:
                device = Device(device_id=device_id,
                                period=self.default_period)
                self._devices[device_id] = device

            device.conn = conn
            device.last_seen = now
            device.packets += 1
            if msg.get("type") == "data":
                device.data_packets += 1
                payload = msg.get("payload")
                device.last_payload = payload if isinstance(payload, dict) else None
                self.stats["data_packets"] += 1
            else:
                device.heartbeat_packets += 1
                self.stats["heartbeat_packets"] += 1

            period_ms = msg.get("period_ms")
            if (isinstance(period_ms, (int, float))
                    and 100 <= period_ms <= 3_600_000):
                device.period = float(period_ms) / 1000.0

            if device.state != ONLINE:
                device.state = ONLINE
                device.offline_since = None
                device.closed = False
                device.last_reason = "收到心跳/数据"
                self.stats["online_events"] += 1
                became_online = True

            self.stats["packets"] += 1

        if became_online:
            self.log(f"[server] {device_id} -> ONLINE "
                     f"(T={device.period:.2f}s)")

        if self._mute.is_set():
            with self._lock:
                self.stats["muted"] += 1
            return

        try:
            conn.send(json.dumps({"type": "ack", "seq": seq}))
        except ConnectionClosed:
            return
        with self._lock:
            self.stats["acks"] += 1

    # ------------------------------------------------------------ 看门狗

    def _watchdog(self) -> None:
        while not self._stop.wait(0.05):
            now = time.monotonic()
            to_close: list[Device] = []

            with self._lock:
                for device in self._devices.values():
                    # 在线 -> 3T 无包 -> 离线
                    if (device.state == ONLINE
                            and now - device.last_seen > 3 * device.period):
                        device.state = OFFLINE
                        device.offline_since = now
                        device.closed = False
                        device.last_reason = (
                            f"静默 {now - device.last_seen:.2f}s")
                        self.stats["offline_events"] += 1
                        self.log(f"[server] {device.device_id} -> OFFLINE "
                                 f"(静默 {now - device.last_seen:.2f}s)")
                    # 离线 -> 再 3T 无包 -> 关闭连接
                    elif (device.state == OFFLINE
                          and not device.closed
                          and device.offline_since is not None
                          and now - device.offline_since > 3 * device.period):
                        to_close.append(device)

            for device in to_close:
                with self._lock:
                    # 这一小段时间里可能又收到包，重新确认
                    if (device.state != OFFLINE
                            or device.offline_since is None
                            or now - device.offline_since <= 3 * device.period):
                        continue
                    conn = device.conn
                    device.conn = None
                    device.closed = True
                if conn is not None:
                    try:
                        conn.close_socket()
                    except Exception:
                        pass
                    with self._lock:
                        self.stats["closes"] += 1
                    self.log(f"[server] {device.device_id} 离线超时，关闭连接")


def main() -> None:
    parser = argparse.ArgumentParser(description="二态心跳服务端")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9001)
    parser.add_argument("--default-period", type=float, default=5.0)
    parser.add_argument("--mute-after", type=float, default=0.0,
                        help="运行 N 秒后开始只收不回 ACK")
    parser.add_argument("--dump-every", type=float, default=0.0,
                        help="每隔 N 秒打印一次设备状态")
    args = parser.parse_args()

    server = WSServer(args.host, args.port,
                      default_period=args.default_period)
    server.start()

    if args.dump_every > 0:
        def _dump() -> None:
            while True:
                time.sleep(args.dump_every)
                for d in server.devices():
                    print(f"[dump] {d}")
        threading.Thread(target=_dump, name="dump", daemon=True).start()

    if args.mute_after > 0:
        def _mute() -> None:
            time.sleep(args.mute_after)
            server.set_mute(True)
        threading.Thread(target=_mute, name="mute", daemon=True).start()

    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n退出")
    finally:
        server.stop()


if __name__ == "__main__":
    main()
