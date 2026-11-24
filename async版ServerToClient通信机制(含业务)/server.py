import asyncio
import json
import time
import uuid
from collections import OrderedDict

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed


HOST = "0.0.0.0"
PORT = 8888

HEARTBEAT_TIMEOUT = 15

# 已连接的 ECU
clients = {}

# 消息去重缓存
processed_messages = OrderedDict()
MAX_PROCESSED_MESSAGES = 10000


def create_msg_id():
    return uuid.uuid4().hex


async def send_message(websocket, lock, message):
    """统一发送 JSON 消息"""

    data = json.dumps(message, ensure_ascii=False)

    async with lock:
        await websocket.send(data)


async def handle_client(websocket):
    """处理一个 ECU 的 WebSocket 连接"""

    addr = websocket.remote_address
    device_id = None
    client_info = None

    print(f"客户端连接：{addr}")

    try:
        while True:

            try:
                # 每秒检查一次消息，同时检查心跳超时
                data = await asyncio.wait_for(
                    websocket.recv(),
                    timeout=1
                )

                message = json.loads(data)

                if not isinstance(message, dict):
                    print("收到的消息不是 JSON 对象")
                    continue

                msg_type = message.get("type")
                device_id = message.get("device_id")

                if not device_id:
                    print("收到缺少 device_id 的消息")
                    continue

                # 注册客户端
                if (
                    device_id not in clients
                    or clients[device_id]["websocket"] is not websocket
                ):

                    old_info = clients.get(device_id)

                    client_info = {
                        "websocket": websocket,
                        "lock": asyncio.Lock(),
                        "last_heartbeat": time.monotonic(),
                        "addr": addr
                    }

                    clients[device_id] = client_info

                    print(f"设备 {device_id} 已注册")

                    # 如果同一设备建立了新连接，关闭旧连接
                    if old_info is not None:
                        try:
                            await old_info["websocket"].close(
                                code=1000,
                                reason="设备建立了新连接"
                            )
                        except ConnectionClosed:
                            pass

                else:
                    client_info = clients[device_id]

                # -------------------------------
                # 1. 心跳消息
                # -------------------------------
                if msg_type == "heartbeat":

                    client_info["last_heartbeat"] = time.monotonic()

                    print(f"收到设备 {device_id} 的心跳")

                    await send_message(
                        websocket,
                        client_info["lock"],
                        {
                            "type": "heartbeat_ack",
                            "msg_id": message.get("msg_id"),
                            "device_id": device_id
                        }
                    )

                # -------------------------------
                # 2. 框架内部状态上报
                # -------------------------------
                elif msg_type == "status_report":

                    msg_id = message.get("msg_id")

                    if not msg_id:
                        continue

                    cache_key = (device_id, msg_id)

                    if cache_key not in processed_messages:

                        status_data = message.get("data", {})

                        print(f"\n设备 {device_id} 状态上报：")

                        print(json.dumps(
                            status_data,
                            ensure_ascii=False,
                            indent=4
                        ))

                        processed_messages[cache_key] = True

                        if len(processed_messages) > MAX_PROCESSED_MESSAGES:
                            processed_messages.popitem(last=False)

                    # 重复消息也回复 ACK
                    await send_message(
                        websocket,
                        client_info["lock"],
                        {
                            "type": "data_ack",
                            "msg_id": msg_id,
                            "device_id": device_id,
                            "acked_type": "status_report"
                        }
                    )

                # -------------------------------
                # 3. 框架外业务数据
                # -------------------------------
                elif msg_type == "business_data":

                    msg_id = message.get("msg_id")

                    if not msg_id:
                        continue

                    cache_key = (device_id, msg_id)

                    if cache_key not in processed_messages:

                        business_data = message.get("data")

                        print(f"\n收到设备 {device_id} 的业务数据：")

                        # 不解析内部业务字段，只负责接收和转发
                        print(json.dumps(
                            business_data,
                            ensure_ascii=False,
                            indent=4
                        ))

                        processed_messages[cache_key] = True

                        if len(processed_messages) > MAX_PROCESSED_MESSAGES:
                            processed_messages.popitem(last=False)

                    await send_message(
                        websocket,
                        client_info["lock"],
                        {
                            "type": "data_ack",
                            "msg_id": msg_id,
                            "device_id": device_id,
                            "acked_type": "business_data"
                        }
                    )

                # -------------------------------
                # 4. 客户端收到命令的 ACK
                # -------------------------------
                elif msg_type == "command_ack":

                    print(
                        f"设备 {device_id} 已收到命令，"
                        f"command_id={message.get('msg_id')}"
                    )

                # -------------------------------
                # 5. 客户端返回命令执行结果
                # -------------------------------
                elif msg_type == "command_result":

                    print(f"\n设备 {device_id} 命令执行结果：")

                    print(json.dumps(
                        message,
                        ensure_ascii=False,
                        indent=4
                    ))

                else:
                    print(f"收到未知消息类型：{msg_type}")

            except asyncio.TimeoutError:
                pass

            # -------------------------------
            # 6. 检查心跳超时
            # -------------------------------
            if client_info is not None:

                elapsed = (
                    time.monotonic()
                    - client_info["last_heartbeat"]
                )

                if elapsed > HEARTBEAT_TIMEOUT:
                    print(f"设备 {device_id} 心跳超时，判定离线")
                    break

    except ConnectionClosed:
        print(f"客户端 {addr} WebSocket 连接断开")

    except (OSError, json.JSONDecodeError) as e:
        print(f"客户端 {addr} 通信异常：{e}")

    finally:

        # 防止旧连接断开时误删新连接
        if (
            device_id in clients
            and clients[device_id]["websocket"] is websocket
        ):
            del clients[device_id]
            print(f"设备 {device_id} 已从在线列表移除")

        print(f"连接 {addr} 已关闭")


# =====================================
# 服务端远程命令下发
# =====================================

async def send_command(device_id, command, params):

    if device_id not in clients:
        print(f"设备 {device_id} 当前不在线")
        return

    client_info = clients[device_id]

    message = {
        "type": "command",
        "msg_id": create_msg_id(),
        "device_id": device_id,
        "command": command,
        "params": params
    }

    try:

        await send_message(
            client_info["websocket"],
            client_info["lock"],
            message
        )

        print(
            f"已向设备 {device_id} 下发命令：{command}，"
            f"command_id={message['msg_id']}"
        )

    except ConnectionClosed:
        print(f"设备 {device_id} 连接已断开，命令发送失败")


# =====================================
# 服务端控制台
# =====================================

async def console():

    print("\n服务端命令控制台")
    print("命令格式：")
    print("list")
    print('send ECU_001 raise_barrier {"lane": 1}')
    print('send ECU_001 print_message {"message": "hello"}')
    print("quit\n")

    while True:

        command_line = await asyncio.to_thread(
            input,
            "server> "
        )

        command_line = command_line.strip()

        if not command_line:
            continue

        if command_line == "quit":
            break

        elif command_line == "list":

            print("当前在线设备：")

            for device_id in clients:
                print(f"  {device_id}")

        elif command_line.startswith("send "):

            try:

                parts = command_line.split(maxsplit=3)

                if len(parts) < 3:
                    print("命令格式错误")
                    continue

                device_id = parts[1]
                command = parts[2]

                params = {}

                if len(parts) == 4:
                    params = json.loads(parts[3])

                await send_command(
                    device_id,
                    command,
                    params
                )

            except json.JSONDecodeError:
                print("JSON 参数格式错误")

        else:
            print("未知命令")


# =====================================
# 服务端主程序
# =====================================

async def main():

    print(f"WebSocket 服务端启动：ws://{HOST}:{PORT}")

    async with serve(
        handle_client,
        HOST,
        PORT,
        ping_interval=20,
        ping_timeout=20
    ):

        await console()


if __name__ == "__main__":

    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        print("服务端退出")