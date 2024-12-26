import asyncio
import datetime
import pickle
from asyncio import StreamWriter
from typing import Callable, Awaitable

import config
from messages import MessageRequest, MessageResponse


class Transporter:
    handler: Callable[[MessageRequest], Awaitable[MessageResponse]] | None = None

    def __init__(self, host, port, stdout: StreamWriter):
        self.stdout = stdout
        self.host = host
        self.port = port
        self.ignore_nodes = {i: False for i in config.NODES}

    async def print(self, string: str):
        self.stdout.write(string.encode())
        self.stdout.write(b'\n')
        await self.stdout.drain()

    async def time_print(self, string: str):
        await self.print(f"({datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]}) {string}")

    def register_handler(self, handler: Callable[[MessageRequest], Awaitable[MessageResponse]]):
        self.handler = handler

    # noinspection PyMethodMayBeStatic
    async def send_request(self, node_id: int, message: MessageRequest) -> MessageResponse | None:
        try:
            reader, writer = await asyncio.open_connection(*config.NODES[node_id])

            writer.write(pickle.dumps(message))
            await writer.drain()
            await self.time_print(f"#{node_id} <- {message}")

            response = await reader.read(1048576)

            try:
                response = pickle.loads(response)
            except pickle.PickleError:
                await self.print("Bad response, not pickle")
                await self.time_print(f"#{node_id} -> {response}")
                return None
            except EOFError:
                await self.print("Data ended")
                await self.time_print(f"#{node_id} -> {response}")
                return None

            await self.time_print(f"#{node_id} -> {response}")

            writer.close()
            return response
        except OSError:
            return None

    async def receive_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        message = await reader.read(1048576)
        try:
            message = pickle.loads(message)
        except:
            await self.print(f"Bad request: {message}")
            return
        await self.time_print(f"?? -> {message}")

        response = await self.handler(message)

        writer.write(pickle.dumps(response))
        await writer.drain()
        await self.time_print(f"?? <- {response}")
        writer.close()

    async def run(self):
        server = await asyncio.start_server(self.receive_request, self.host, self.port)

        addr = server.sockets[0].getsockname()
        await self.print(f"Serving on {addr}")
        await self.print(f"given {self.host} {self.port}")

        async with server:
            await server.serve_forever()
