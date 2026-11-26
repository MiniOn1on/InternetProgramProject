"""1.2 Sleep 版：心跳 + ECU 上报客户端。

三个线程，全部用 sleep 定时，不依赖 Event / Condition 唤醒：
    主线程      采集 ECU 快照，写进“只保留最新值”的盒子
    发送线程    自己算 next_at，到点组包发送，然后用 50 ms 轮询等 ACK
    接收线程    阻塞收包（0.3 s 超时），收到 ACK 只改共享字段，不做通知

运行：
    python server.py            # 另开一个窗口
    python client.py            # --verbose 可以看到每一帧
"""
from __future__ import annotations

import argparse
import json
import math
import random
import socket
import threading
import time

import protocol

ONLINE = "ONLINE"
UNSTABLE = "UNSTABLE"
OFFLINE = "OFFLINE"


def stamp() -> str:
    return time.strftime("%H:%M:%S", time.localtime())


class LatestBox:
    """单槽、最新值覆盖：ECU 状态是“当前值”语义，断链期间攒的旧帧没有意义。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._value: dict | None = None

    def publish(self, value: dict) -> None:
        with self._lock:
            self._value = value

    def get(self) -> dict | None:
        with self._lock:
            return self._value


class Link:
    """共享链路状态：一把锁保护所有字段。"""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.conn: socket.socket | None = None
        self.seq = 1              # 单调递增，重连也不清零
        self.acked_seq = 0        # 收到过的最大的 ACK 序号
        self.miss = 0             # 累计未应答帧数
        self.ok_streak = 0        # 连续成功帧数
        self.fail_streak = 0      # 连续失败帧数
        self.state = OFFLINE
        self.unstable_from = OFFLINE   # 从哪个状态掉下来的，决定“多快回 ONLINE”
        self.ever_up = False


class HeartbeatClient:
    def __init__(self, host: str = "127.0.0.1", port: int = 9000, *,
                 period: float = 5.0,
                 reply_timeout: float = 2.0,
                 max_miss: int = 3,
                 poll: float = 0.05,
                 ecu_id: int = 1,
                 verbose: bool = False) -> None:
        if reply_timeout >= period:
            raise ValueError("reply_timeout 必须小于 period，否则节拍会堆积")
        self.host, self.port = host, port
        self.period = period
        self.reply_timeout = reply_timeout
        self.max_miss = max_miss
        self.poll = poll
        self.ecu_id = ecu_id
        self.verbose = verbose

        self.link = Link()
        self.ecu = LatestBox()
        self.stop_evt = threading.Event()
        self.last_outcome: tuple[int, int] | None = None   # (seq, rtt_ms) 或 (seq, -1)
        self._first_attempt = True
        self._backoff_step = 0
        self._conn_failures = 0
        self._threads: list[threading.Thread] = []

    # ------------------------------------------------------------ 生命周期
    def run(self) -> None:
        self._start_threads()
        try:
            self._business_loop()
        finally:
            self.stop()

    def _start_threads(self) -> None:
        for target, name in ((self._sender_loop, "hb-sender"),
                             (self._receiver_loop, "hb-receiver")):
            t = threading.Thread(target=target, name=name, daemon=True)
            t.start()
            self._threads.append(t)

    def stop(self) -> None:
        self.stop_evt.set()
        conn = self.link.conn
        self.link.conn = None
        _close_quietly(conn)
        for t in self._threads:
            t.join(timeout=2.0)
        self._threads.clear()

    # ------------------------------------------------------------ 业务层
    def _business_loop(self) -> None:
        """主线程只做采集：谁要用时间谁自己睡，节拍由发送线程负责。"""
        while not self.stop_evt.is_set():
            self.ecu.publish(self._read_ecu())
            time.sleep(0.5)

    def _read_ecu(self) -> dict:
        now = time.time()
        return {
            "sample_ts_ms": int(now * 1000),
            "ecu_id": self.ecu_id,
            "voltage_mv": 12000 + int(150 * math.sin(now)),
            "rpm": 800 + int(300 * (1 + math.sin(now / 3))),
            "speed_x10": int(500 + 300 * math.sin(now / 5)),
        }

    # ------------------------------------------------------------ 并发层
    def _sender_loop(self) -> None:
        next_at = time.monotonic()
        while not self.stop_evt.is_set():
            if self.link.conn is None:
                if not self._reconnect():
                    time.sleep(1.0)
                    next_at = time.monotonic()
                    continue

            seq = self._send_one()
            if seq is not None:
                rtt = self._wait_ack(seq, self.reply_timeout)
                if rtt is None:
                    self._on_timeout(seq)
                else:
                    self._on_ack(seq, rtt)

            next_at += self.period               # 按绝对时刻推进，吃掉发送耗时
            remain = next_at - time.monotonic()
            if remain > 0:
                time.sleep(remain)
            else:
                next_at = time.monotonic()       # 落后了就不补发

    def _send_one(self) -> int | None:
        sample = self.ecu.get()
        if sample is None:
            return None
        conn = self.link.conn
        if conn is None:
            return None

        with self.link.lock:
            seq = self.link.seq
            self.link.seq += 1

        msg = {
            "type": "hb",
            "seq": seq,
            "ts": int(time.time() * 1000),
            "ecu_id": self.ecu_id,
            "period_ms": int(self.period * 1000),
            "ecu": sample,
        }
        prev = self.last_outcome
        if prev is not None:
            msg["prev_seq"], msg["prev_rtt"] = prev

        try:
            conn.sendall(protocol.encode(msg))
        except OSError as exc:
            self._mark_down(conn, f"send: {exc}")
            return None
        if self.verbose:
            print(f"{stamp()} [tx] seq={seq} vol={sample['voltage_mv']}mV rpm={sample['rpm']}")
        return seq

    def _wait_ack(self, seq: int, timeout: float) -> int | None:
        """轮询等应答：返回 RTT(ms)，超时返回 None。这就是 sleep 版的“等”。"""
        sent_at = time.monotonic()
        deadline = sent_at + timeout
        while time.monotonic() < deadline:
            with self.link.lock:
                acked = self.link.acked_seq
            if acked >= seq:
                return int((time.monotonic() - sent_at) * 1000)
            if self.stop_evt.is_set() or self.link.conn is None:
                return None
            time.sleep(self.poll)
        return None

    def _receiver_loop(self) -> None:
        buf = bytearray()
        cur: socket.socket | None = None
        while not self.stop_evt.is_set():
            conn = self.link.conn
            if conn is None:
                time.sleep(0.1)
                continue
            if conn is not cur:
                buf.clear()                       # 换连接：丢掉上一连接的半截缓冲
                cur = conn
            try:
                chunk = conn.recv(4096)
            except socket.timeout:
                continue                          # 超时不是错误：转一圈看 stop_evt
            except OSError as exc:
                self._mark_down(conn, f"recv: {exc}")
                continue
            if not chunk:
                self._mark_down(conn, "对端关闭连接")
                continue

            buf.extend(chunk)
            try:
                frames = protocol.decode(buf)
            except (ValueError, json.JSONDecodeError) as exc:
                self._mark_down(conn, f"协议错误: {exc}")
                continue

            for frame in frames:
                if frame.get("type") != "ack":
                    continue
                with self.link.lock:
                    if self.link.conn is not conn:
                        continue
                    if frame["seq"] > self.link.acked_seq:
                        self.link.acked_seq = frame["seq"]

    # ------------------------------------------------------------ 状态层
    def _on_ack(self, seq: int, rtt_ms: int) -> None:
        self.last_outcome = (seq, rtt_ms)        # 下一帧捎带给服务端
        with self.link.lock:
            link = self.link
            link.miss = 0
            link.fail_streak = 0
            link.ok_streak += 1
            old = link.state
            if link.state == ONLINE:
                new = ONLINE
            elif link.state == OFFLINE:          # 掉线后第一次可达：先 UNSTABLE
                link.state = UNSTABLE
                link.unstable_from = OFFLINE
                new = UNSTABLE
            elif link.unstable_from == ONLINE or link.ok_streak >= self.max_miss:
                link.state = ONLINE                  # 从 ONLINE 掉下来的一帧即回；否则要连续 N 帧
                link.ever_up = True
                new = ONLINE
            else:
                new = link.state
        if old != new:
            print(f"{stamp()} [link] {old} -> {new}（收到 ACK，rtt={rtt_ms}ms）")
        elif self.verbose:
            print(f"{stamp()} [rx] seq={seq} ack rtt={rtt_ms}ms")

    def _on_timeout(self, seq: int) -> None:
        self.last_outcome = (seq, -1)
        with self.link.lock:
            link = self.link
            link.ok_streak = 0
            link.miss += 1
            old = link.state
            if link.state == ONLINE:
                link.state = UNSTABLE
                link.unstable_from = ONLINE
                link.fail_streak = 1
            else:
                link.fail_streak += 1
                if link.fail_streak >= self.max_miss or link.unstable_from == OFFLINE:
                    link.state = OFFLINE
            new, streak = link.state, link.fail_streak
        if old != new:
            print(f"{stamp()} [link] {old} -> {new}（seq={seq} 未应答，连续 {streak} 次）")

    def _mark_down(self, conn: socket.socket, reason: str) -> None:
        with self.link.lock:
            if self.link.conn is not conn:
                return                            # 旧连接退出，不动新连接
            self.link.conn = None
            self.link.state = OFFLINE
            self.link.unstable_from = OFFLINE
            self.link.fail_streak = 0
        _close_quietly(conn)
        print(f"{stamp()} [link] -> OFFLINE（{reason}）")

    # ------------------------------------------------------------ 传输层
    def _reconnect(self) -> bool:
        delay = 0.0 if self._first_attempt else self._backoff_delay()
        if delay > 0:
            print(f"{stamp()} [link] {delay:.2f}s 后重连（连续失败 {self._conn_failures} 次）")
            time.sleep(delay)                     # 不可打断：stop 要等它睡完
        self._first_attempt = False

        try:
            conn = socket.create_connection((self.host, self.port), timeout=2.0)
            conn.settimeout(0.3)                  # 接收线程的计时精度
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError as exc:
            self._conn_failures += 1
            self._backoff_step = min(self._backoff_step + 1, 8)
            print(f"{stamp()} [link] 连接失败：{exc}（连续失败 {self._conn_failures} 次）")
            return False

        with self.link.lock:
            self.link.conn = conn
            self.link.state = UNSTABLE
            self.link.unstable_from = OFFLINE
            self.link.miss = 0
            self.link.ok_streak = 0
            self.link.fail_streak = 0
        self.last_outcome = None                  # 新连接上旧结果没有意义
        self._conn_failures = 0
        self._backoff_step = 0
        print(f"{stamp()} [link] 已连接 {self.host}:{self.port}，发心跳等 ACK")
        return True

    def _backoff_delay(self) -> float:
        base = 1.0 * (2 ** self._backoff_step)
        return min(30.0, base) * random.uniform(0.85, 1.15)


def _close_quietly(sock: socket.socket | None) -> None:
    if sock is None:
        return
    for fn in (lambda: sock.shutdown(socket.SHUT_RDWR), sock.close):
        try:
            fn()
        except OSError:
            pass


def main() -> None:
    ap = argparse.ArgumentParser(description="1.2 Sleep 版心跳客户端")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--period", type=float, default=5.0, help="心跳周期（秒）")
    ap.add_argument("--timeout", type=float, default=2.0, help="单帧等 ACK 的超时")
    ap.add_argument("--miss", type=int, default=3, help="连续 N 次无应答判离线")
    ap.add_argument("--ecu-id", type=int, default=1)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    client = HeartbeatClient(args.host, args.port, period=args.period,
                             reply_timeout=args.timeout, max_miss=args.miss,
                             ecu_id=args.ecu_id, verbose=args.verbose)
    print(f"连接 {args.host}:{args.port}  ECU={args.ecu_id}  "
          f"T={args.period}s  Tout={args.timeout}s  N={args.miss}")
    try:
        client.run()
    except KeyboardInterrupt:
        print("\n收到 Ctrl+C，退出")


if __name__ == "__main__":
    main()
