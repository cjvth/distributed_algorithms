import asyncio
import pickle
from typing import Callable, Awaitable

import config
from messages import MessageRequest, MessageResponse


class Transporter:
    handler: Callable[[MessageRequest], Awaitable[MessageResponse]] | None = None

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.ignore_nodes = [False] * config.total_nodes

    def register_handler(self, handler: Callable[[MessageRequest], Awaitable[MessageResponse]]):
        self.handler = handler

    # noinspection PyMethodMayBeStatic
    async def send_request(self, node_id, message: MessageRequest) -> MessageResponse:
        reader, writer = await asyncio.open_connection(*config.nodes[node_id])

        writer.write(pickle.dumps(message))
        await writer.drain()

        data = await reader.read()
        data = pickle.loads(data)

        writer.close()
        return data

    async def receive_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        message = await reader.read()
        try:
            message = pickle.loads(message)
        except:
            print(message)
            return

        response = await self.handler(message)

        writer.write(pickle.dumps(response))
        await writer.drain()
        writer.close()

    async def run(self):
        server = await asyncio.start_server(self.receive_request, self.host, self.port)

        addr = server.sockets[0].getsockname()
        print(f"Serving on {addr}")
        print(f"given {self.host} {self.port}")

        async with server:
            await server.serve_forever()
