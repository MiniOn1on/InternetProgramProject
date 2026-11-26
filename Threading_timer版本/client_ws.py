"""客户端：三线程 + 事件驱动 + 三态（server 在 client 眼中的状态）。

线程模型
--------
主线程   ：业务循环 + 周期定时器；每 T 把“这一拍要发的数据”放进单槽，
           然后 set tick_event。
T1（sender）：等 tick_event；有 data 发 data，没有就发 heartbeat；
           发完等 ack_event 或超时；更新状态机；必要时重连。
T2（receiver）：收 server 的 ACK，写 acked_seq 并 set ack_event；
             连接断开时 set link_down_event / ack_event / tick_event。

状态机（fail = 自上次有效 ACK 以来连续超时的次数）
--------------------------------------------------
ONLINE   --1 次超时--> UNSTABLE(fail=1)
UNSTABLE --再超时到 fail>=3--> OFFLINE
UNSTABLE --1 次 ACK--> ONLINE
OFFLINE  --1 次 ACK--> UNSTABLE
OFFLINE  --离线满 3 个周期的那一拍发完仍超时--> 暂停发送
任意     --硬故障--> OFFLINE，立即重连
"""

from __future__ import annotations

import argparse
import json
import threading
import time

from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect

ONLINE = "ONLINE"
UNSTABLE = "UNSTABLE"
OFFLINE = "OFFLINE"


class ClientState:
    """纯状态机：不碰网络、不碰线程，方便单独测试。"""

    def __init__(self, period: float, max_fail: int = 3,
                 stop_cycles: int = 3) -> None:
        self.period = period
        self.max_fail = max_fail
        self.stop_cycles = stop_cycles

        self._lock = threading.Lock()
        self.state = OFFLINE       # 初始：还没收到过 ACK
        self.fail = 0
        self.src = None            # UNSTABLE 的来源：ONLINE / OFFLINE
        self.offline_since = None  # 进入 OFFLINE 的时刻
        self.paused = False        # 离线满 3 个周期后停发

    def on_success(self, now: float) -> None:
        with self._lock:
            self.fail = 0
            if self.state == OFFLINE:
                self.state = UNSTABLE
                self.src = OFFLINE
                self.offline_since = None
                self.paused = False
            elif self.state == UNSTABLE:
                self.state = ONLINE
                self.src = None
            # ONLINE 保持

    def on_timeout(self, now: float, send_at: float) -> None:
        with self._lock:
            self.fail += 1

            if self.state == ONLINE:
                self.state = UNSTABLE
                self.src = ONLINE
            elif self.state == UNSTABLE and self.fail >= self.max_fail:
                self.state = OFFLINE
                self.src = None
                self.offline_since = now
                self.paused = False

            if self.state == OFFLINE:
                if self.offline_since is None:
                    self.offline_since = now
                # 离线满 stop_cycles 个周期的那一拍：发完仍超时 -> 暂停
                if send_at >= self.offline_since + self.stop_cycles * self.period:
                    self.paused = True

    def on_hard_failure(self) -> None:
        with self._lock:
            self.state = OFFLINE
            self.src = None
            self.fail = 0
            self.offline_since = None
            self.paused = False

    def on_reconnect(self) -> None:
        with self._lock:
            self.state = OFFLINE
            self.src = None
            self.fail = 0
            self.offline_since = None
            self.paused = False

    def snapshot(self) -> dict:
        with self._lock:
            return {"state": self.state, "fail": self.fail, "src": self.src,
                    "paused": self.paused}


class WSHeartbeatClient:
    def __init__(self, url: str, device_id: str = "ecu-1", *,
                 period: float = 1.0,
                 reply_timeout: float = 0.5,
                 max_fail: int = 3,
                 stop_cycles: int = 3,
                 data_every: int = 2,
                 reconnect_base: float = 0.2,
                 reconnect_cap: float = 2.0,
                 log=print) -> None:
        if reply_timeout >= period:
            raise ValueError("reply_timeout 必须小于 period")
        self.url = url
        self.device_id = device_id
        self.period = period
        self.reply_timeout = reply_timeout
        self.max_fail = max_fail
        self.stop_cycles = stop_cycles
        self.data_every = max(1, data_every)
        self.reconnect_base = reconnect_base
        self.reconnect_cap = reconnect_cap
        self.log = log

        self.state_machine = ClientState(period, max_fail, stop_cycles)

        # 事件：只负责“叫醒”
        self.tick_event = threading.Event()       # 主线程 -> T1
        self.ack_event = threading.Event()        # T2 -> T1
        self.link_down_event = threading.Event()  # T2 -> T1
        self.stop_event = threading.Event()       # 主线程 -> 全部

        # 共享值：用一把小锁保护（Event 本身不能传值）
        self._shared_lock = threading.Lock()
        self._latest_data = None                  # 主线程写，T1 取走
        self._acked_seq = 0                       # T2 写，T1 读

        self._conn_lock = threading.Lock()
        self._conn = None                         # T1 写，T2 读

        self._seq = 0                             # 只有 T1 用
        self._threads: list[threading.Thread] = []

    # ------------------------------------------------------------ 生命周期

    def start(self) -> "WSHeartbeatClient":
        if self._threads:
            return self
        for target, name in ((self._sender_loop, "cli-sender"),
                             (self._receiver_loop, "cli-receiver")):
            thread = threading.Thread(target=target, name=name, daemon=True)
            thread.start()
            self._threads.append(thread)
        return self

    def run(self) -> None:
        self.start()
        try:
            self._business_loop()
        finally:
            self.stop()

    def stop(self) -> None:
        self.stop_event.set()
        self._close_conn()
        self.tick_event.set()
        self.ack_event.set()
        self.link_down_event.set()
        for thread in self._threads:
            thread.join(timeout=2.0)
        self._threads.clear()

    def status(self) -> dict:
        info = self.state_machine.snapshot()
        info.update({"device_id": self.device_id,
                     "seq": self._seq,
                     "connected": self._get_conn() is not None})
        return info

    # ------------------------------------------------------------ 主线程

    def _business_loop(self) -> None:
        next_at = time.monotonic()
        tick = 0

        while not self.stop_event.is_set():
            tick += 1

            # 简单业务数据：每 data_every 拍产生一次，其余拍为 None
            # None 时 T1 会发 heartbeat，正好演示两种包。
            data = None
            if tick % self.data_every == 1:
                data = {"counter": tick,
                        "value": round(20.0 + (tick % 5) * 0.1, 2),
                        "ts_ms": int(time.time() * 1000)}

            with self._shared_lock:
                self._latest_data = data
            self.tick_event.set()

            next_at += self.period
            remain = next_at - time.monotonic()
            if remain > 0:
                if self.stop_event.wait(remain):
                    break
            else:
                next_at = time.monotonic() + self.period

    # ------------------------------------------------------------ 发送线程 T1

    def _sender_loop(self) -> None:
        while not self.stop_event.is_set():
            # 停发状态：只等连接关闭（server 会主动关）或退出
            if self.state_machine.snapshot()["paused"]:
                self.tick_event.wait(0.2)
                self.tick_event.clear()
                if self.stop_event.is_set():
                    return
                if self.link_down_event.is_set():
                    self.link_down_event.clear()
                    if self._reconnect():
                        self.state_machine.on_reconnect()
                        self.tick_event.set()
                continue

            # 等周期节拍；超时醒来看一眼有没有连接断开
            if not self.tick_event.wait(0.2):
                if self.link_down_event.is_set():
                    self.link_down_event.clear()
                    if self._reconnect():
                        self.state_machine.on_reconnect()
                        self.tick_event.set()
                continue
            self.tick_event.clear()
            if self.stop_event.is_set():
                return

            # 连接断了先重连
            if self.link_down_event.is_set():
                self.link_down_event.clear()
                if not self._reconnect():
                    continue
                self.state_machine.on_reconnect()
                self.tick_event.set()
                continue

            conn = self._get_conn()
            if conn is None:
                if not self._reconnect():
                    continue
                self.state_machine.on_reconnect()
                self.tick_event.set()
                continue

            data = self._take_data()
            self._seq += 1
            seq = self._seq
            msg = self._build_message(seq, data)
            sent_at = time.monotonic()

            self.ack_event.clear()
            try:
                conn.send(json.dumps(msg, separators=(",", ":")))
            except (ConnectionClosed, OSError):
                self._hard_failure()
                continue

            result = self._wait_ack(seq, sent_at)
            if result is None:              # 等 ACK 期间连接断开
                self._hard_failure()
                continue

            before = self.state_machine.snapshot()
            if result:
                self.state_machine.on_success(time.monotonic())
            else:
                self.state_machine.on_timeout(time.monotonic(), sent_at)
            after = self.state_machine.snapshot()
            self._log_transition(before, after)

    def _build_message(self, seq: int, data) -> dict:
        msg = {"device_id": self.device_id,
               "seq": seq,
               "period_ms": int(self.period * 1000),
               "ts_ms": int(time.time() * 1000)}
        if data is None:
            msg["type"] = "heartbeat"
        else:
            msg["type"] = "data"
            msg["payload"] = data
        return msg

    def _wait_ack(self, seq: int, sent_at: float):
        """True=成功，False=超时，None=连接断开。"""
        deadline = sent_at + self.reply_timeout
        while True:
            if self.stop_event.is_set():
                return None
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            if self.ack_event.wait(remaining):
                with self._shared_lock:
                    acked = self._acked_seq
                if acked == seq:
                    return True
                self.ack_event.clear()      # 迟到/旧的 ACK
            if self.link_down_event.is_set():
                return None

    def _take_data(self):
        with self._shared_lock:
            data = self._latest_data
            self._latest_data = None
            return data

    def _log_transition(self, before: dict, after: dict) -> None:
        if after["state"] != before["state"]:
            self.log(f"[state] {before['state']} -> {after['state']} "
                     f"(fail={after['fail']}, src={after['src']})")
        if after["paused"] and not before["paused"]:
            self.log("[state] 离线满 3 个周期，暂停发送，等 server 关闭连接")

    # ------------------------------------------------------------ 连接管理

    def _get_conn(self):
        with self._conn_lock:
            return self._conn

    def _close_conn(self) -> None:
        with self._conn_lock:
            conn = self._conn
            self._conn = None
        if conn is not None:
            try:
                conn.close_socket()
            except Exception:
                pass

    def _reconnect(self) -> bool:
        self._close_conn()
        # 只要开始重连，说明当前连接已经不可用：状态先落 OFFLINE，
        # 避免“server 已经关了、client 还显示 ONLINE”。
        self.state_machine.on_hard_failure()
        delay = 0.0
        while not self.stop_event.is_set():
            if delay > 0 and self.stop_event.wait(delay):
                return False
            try:
                conn = connect(self.url, ping_interval=None,
                               open_timeout=2.0, close_timeout=1.0, legacy=True)
            except OSError:
                delay = min(self.reconnect_cap,
                            max(self.reconnect_base,
                                delay * 2 if delay else self.reconnect_base))
                continue
            with self._conn_lock:
                self._conn = conn
            self.link_down_event.clear()
            self.log(f"[link] connected {self.url}")
            return True
        return False

    def _hard_failure(self) -> None:
        self.log("[link] hard failure -> OFFLINE, reconnect")
        self._close_conn()
        self.state_machine.on_hard_failure()
        if self._reconnect():
            self.state_machine.on_reconnect()
            self.tick_event.set()

    # ------------------------------------------------------------ 接收线程 T2

    def _receiver_loop(self) -> None:
        while not self.stop_event.is_set():
            conn = self._get_conn()
            if conn is None:
                self.stop_event.wait(0.1)
                continue

            try:
                raw = conn.recv(timeout=0.2)
            except TimeoutError:
                continue
            except (ConnectionClosed, OSError):
                lost = False
                with self._conn_lock:
                    if self._conn is conn:
                        self._conn = None
                        lost = True
                if lost and not self.stop_event.is_set():
                    # 同时叫醒：等 ACK 的 T1、停发等 tick 的 T1
                    self.link_down_event.set()
                    self.ack_event.set()
                    self.tick_event.set()
                continue

            try:
                msg = json.loads(raw)
            except (ValueError, TypeError):
                continue
            if isinstance(msg, dict) and msg.get("type") == "ack":
                seq = msg.get("seq")
                if isinstance(seq, int):
                    with self._shared_lock:
                        self._acked_seq = seq
                    self.ack_event.set()


def main() -> None:
    parser = argparse.ArgumentParser(description="三线程心跳客户端")
    parser.add_argument("--url", default="ws://127.0.0.1:9001/")
    parser.add_argument("--device-id", default="ecu-1")
    parser.add_argument("--period", type=float, default=2.0)
    parser.add_argument("--timeout", type=float, default=1.0)
    parser.add_argument("--miss", type=int, default=3)
    parser.add_argument("--data-every", type=int, default=2)
    args = parser.parse_args()

    client = WSHeartbeatClient(args.url, args.device_id,
                               period=args.period,
                               reply_timeout=args.timeout,
                               max_fail=args.miss,
                               data_every=args.data_every)
    try:
        client.run()
    except KeyboardInterrupt:
        print("\n退出")
    finally:
        client.stop()


if __name__ == "__main__":
    main()
