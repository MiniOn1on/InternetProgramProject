import asyncio
import json
import time
import uuid
import platform

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed


SERVER_URI = "ws://127.0.0.1:8888"

DEVICE_ID = "ECU_001"

HEARTBEAT_INTERVAL = 5
HEARTBEAT_TIMEOUT = 15

STATUS_INTERVAL = 10
BUSINESS_INTERVAL = 30

ACK_TIMEOUT = 5
MAX_RETRIES = 3

# 等待数据 ACK 的消息
pending_acks = {}

# 统一发送锁
send_lock = asyncio.Lock()

# 最近一次收到心跳 ACK 的时间
last_heartbeat_ack = time.monotonic()

# 客户端状态
client_status = "ONLINE"

# 正在执行的远程命令任务
command_tasks = set()


def create_msg_id():
    return uuid.uuid4().hex


async def send_message(websocket, message):
    """统一发送 JSON 消息"""

    data = json.dumps(message, ensure_ascii=False)

    async with send_lock:
        await websocket.send(data)


# =====================================
# 1. 可靠消息发送与 ACK 匹配
# =====================================

async def send_reliable_message(websocket, message):

    msg_id = message["msg_id"]

    future = asyncio.get_running_loop().create_future()
    pending_acks[msg_id] = future

    try:

        for attempt in range(1, MAX_RETRIES + 1):

            await send_message(websocket, message)

            print(
                f"发送 {message['type']}，"
                f"msg_id={msg_id}，第 {attempt} 次"
            )

            try:

                await asyncio.wait_for(
                    asyncio.shield(future),
                    timeout=ACK_TIMEOUT
                )

                print(f"收到数据 ACK：{msg_id}")
                return

            except asyncio.TimeoutError:

                print(f"等待数据 ACK 超时：{msg_id}")

        raise ConnectionError(
            f"消息 {msg_id} 连续 {MAX_RETRIES} 次未收到 ACK"
        )

    finally:
        pending_acks.pop(msg_id, None)


# =====================================
# 2. 心跳发送
# =====================================

async def heartbeat_sender(websocket):

    while True:

        message = {
            "type": "heartbeat",
            "msg_id": create_msg_id(),
            "device_id": DEVICE_ID
        }

        await send_message(websocket, message)

        print("发送心跳包")

        await asyncio.sleep(HEARTBEAT_INTERVAL)


# =====================================
# 3. 心跳状态监测
# =====================================

async def heartbeat_monitor():

    global client_status

    while True:

        elapsed = time.monotonic() - last_heartbeat_ack

        if elapsed >= HEARTBEAT_TIMEOUT:

            client_status = "OFFLINE"

            print("服务端状态：OFFLINE")

            raise ConnectionError("等待心跳 ACK 超时")

        elif elapsed >= HEARTBEAT_INTERVAL * 2:

            client_status = "UNSTABLE"

            print("服务端状态：UNSTABLE")

        else:

            client_status = "ONLINE"

        await asyncio.sleep(1)


# =====================================
# 4. 收集框架内部状态
# =====================================

def collect_framework_status():

    cpu_percent = None
    memory_percent = None

    try:
        import psutil

        cpu_percent = psutil.cpu_percent(interval=None)
        memory_percent = psutil.virtual_memory().percent

    except ImportError:
        pass

    return {
        "device_id": DEVICE_ID,
        "cpu_percent": cpu_percent,
        "memory_percent": memory_percent,
        "device_status": "RUNNING",
        "system": platform.system(),
        "python_version": platform.python_version(),
        "timestamp": time.time()
    }


async def status_sender(websocket):

    while True:

        status_data = collect_framework_status()

        message = {
            "type": "status_report",
            "msg_id": create_msg_id(),
            "device_id": DEVICE_ID,
            "data": status_data
        }

        await send_reliable_message(websocket, message)

        await asyncio.sleep(STATUS_INTERVAL)


# =====================================
# 5. 框架外业务数据透传
# =====================================

async def business_data_sender(websocket):

    while True:

        # 这里只是周期性演示数据。
        # 实际项目中可以替换为外部业务模块产生的数据。

        business_data = {
            "system": "campus_access",
            "event": "vehicle_detected",
            "vehicle_id": "CAR_001",
            "gate_id": "GATE_01",
            "timestamp": time.time()
        }

        message = {
            "type": "business_data",
            "msg_id": create_msg_id(),
            "device_id": DEVICE_ID,
            "data": business_data
        }

        await send_reliable_message(websocket, message)

        await asyncio.sleep(BUSINESS_INTERVAL)


# =====================================
# 6. 远程命令执行
# =====================================

async def execute_command(command, params):

    if command == "raise_barrier":

        lane = params.get("lane", 1)

        print(f"模拟执行：抬起 {lane} 号道闸")

        await asyncio.sleep(2)

        return {
            "success": True,
            "message": f"{lane} 号道闸模拟抬杆成功"
        }

    elif command == "print_message":

        content = params.get("message", "")

        print(f"收到服务端消息：{content}")

        return {
            "success": True,
            "message": "消息处理成功"
        }

    elif command == "get_device_status":

        return {
            "success": True,
            "device_status": "RUNNING"
        }

    else:

        return {
            "success": False,
            "message": f"未知命令：{command}"
        }


async def handle_command(websocket, message):

    command_id = message.get("msg_id")
    command = message.get("command")
    params = message.get("params", {})

    # 先回复命令接收 ACK
    ack_message = {
        "type": "command_ack",
        "msg_id": command_id,
        "device_id": DEVICE_ID
    }

    try:
        await send_message(websocket, ack_message)

    except ConnectionClosed:
        print("连接已断开，无法回复命令 ACK")
        return

    print(f"收到服务端命令：{command}")

    # 再执行命令
    try:

        result = await execute_command(command, params)

    except Exception as e:

        result = {
            "success": False,
            "message": str(e)
        }

    # 返回命令执行结果
    result_message = {
        "type": "command_result",
        "msg_id": command_id,
        "device_id": DEVICE_ID,
        "command": command,
        "result": result
    }

    try:

        await send_message(websocket, result_message)

        print(f"命令执行完毕：{command}")

    except ConnectionClosed:
        print("命令执行完成，但连接已断开，无法返回执行结果")


# =====================================
# 7. 统一消息接收与分发
# =====================================

async def message_receiver(websocket):

    global last_heartbeat_ack
    global client_status

    while True:

        data = await websocket.recv()

        try:
            message = json.loads(data)

        except (json.JSONDecodeError, TypeError):
            print("收到无法解析的 JSON 消息")
            continue

        if not isinstance(message, dict):
            print("收到的消息不是 JSON 对象")
            continue

        msg_type = message.get("type")
        msg_id = message.get("msg_id")

        # -------------------------------
        # 心跳 ACK
        # -------------------------------
        if msg_type == "heartbeat_ack":

            last_heartbeat_ack = time.monotonic()
            client_status = "ONLINE"

            print("收到心跳 ACK，服务端状态：ONLINE")

        # -------------------------------
        # 状态数据 / 业务数据 ACK
        # -------------------------------
        elif msg_type == "data_ack":

            future = pending_acks.get(msg_id)

            if future is not None and not future.done():

                future.set_result(message)

            print(
                f"收到 {message.get('acked_type')} 的 ACK"
            )

        # -------------------------------
        # 服务端远程命令
        # -------------------------------
        elif msg_type == "command":

            task = asyncio.create_task(
                handle_command(websocket, message)
            )

            command_tasks.add(task)
            task.add_done_callback(command_tasks.discard)

        else:

            print(f"收到未知消息类型：{msg_type}")


# =====================================
# 8. 客户端主程序：自动重连
# =====================================

async def main():

    global last_heartbeat_ack
    global client_status

    while True:

        websocket = None
        tasks = []

        try:

            # 建立 WebSocket 连接
            websocket = await connect(
                SERVER_URI,
                ping_interval=20,
                ping_timeout=20
            )

            print("成功连接服务端")

            last_heartbeat_ack = time.monotonic()
            client_status = "ONLINE"

            # 启动通信协程
            tasks = [

                asyncio.create_task(
                    heartbeat_sender(websocket)
                ),

                asyncio.create_task(
                    heartbeat_monitor()
                ),

                asyncio.create_task(
                    status_sender(websocket)
                ),

                asyncio.create_task(
                    business_data_sender(websocket)
                ),

                asyncio.create_task(
                    message_receiver(websocket)
                )
            ]

            # 任意一个关键协程结束，当前连接就需要退出
            done, pending = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED
            )

            # 先取消其他协程
            for task in pending:
                task.cancel()

            await asyncio.gather(
                *pending,
                return_exceptions=True
            )

            # 处理已结束任务的异常
            for task in done:

                if not task.cancelled():

                    exc = task.exception()

                    if exc is not None:
                        raise exc

            raise ConnectionError("通信任务意外结束")

        except (
            OSError,
            ConnectionError,
            ConnectionClosed
        ) as e:

            client_status = "OFFLINE"

            print("连接异常：", e)

        finally:

            # 取消未完成的通信协程
            for task in tasks:

                if not task.done():
                    task.cancel()

            if tasks:
                await asyncio.gather(
                    *tasks,
                    return_exceptions=True
                )

            # 清理等待 ACK 的 Future
            for future in pending_acks.values():

                if not future.done():
                    future.cancel()

            pending_acks.clear()

            # 关闭 WebSocket 连接
            if websocket is not None:

                try:
                    await websocket.close()
                except ConnectionClosed:
                    pass

        print("3 秒后重新连接服务端...")
        await asyncio.sleep(3)


if __name__ == "__main__":

    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        print("客户端退出")