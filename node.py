import asyncio

from messages import MessageRequest, MessageResponse
from transporter import Transporter


class Node:
    def __init__(self, node_id: int, transporter: Transporter):
        self.transporter = transporter
        transporter.register_handler(self.handle_request)
        self.node_id = node_id

    async def run(self):
        print(f"Node running on {self.transporter.host}, {self.transporter.port}")
        i = 1
        while True:
            await asyncio.sleep(5)
            print("Still working", i)
            i += 1

    async def handle_request(self, request: MessageRequest) -> MessageResponse:
        pass
