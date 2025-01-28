from __future__ import annotations

import asyncio
import datetime
import json
import pickle
import random
import re
import typing
from asyncio import StreamWriter
from json import JSONDecodeError
from pickle import PickleError
from typing import Callable, Awaitable

import config
import messages
from messages import MessageRequest, MessageResponse

if typing.TYPE_CHECKING:
    from node.node import Node


class Transporter:
    handler: Callable[[MessageRequest], Awaitable[MessageResponse]] | None = None
    node: Node | None = None
    node_latency: dict[int, float]
    block_in: dict[int, bool]
    block_out: dict[int, bool]

    def __init__(self, host, port, stdout: StreamWriter):
        self.stdout = stdout
        self.host = host
        self.port = port
        self.reset()
        self.hang = False

    def reset(self):
        self.node_latency = {i: config.DEFAULT_DELAY for i in config.NODES}
        self.block_in = {i: False for i in config.NODES}
        self.block_out = {i: False for i in config.NODES}

    async def print(self, string: str):
        self.stdout.write(string.encode())
        self.stdout.write(b'\n')
        await self.stdout.drain()

    async def time_print(self, string: str):
        await self.print(f"({datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]}) {string}")

    def register_handler(self, handler: Callable[[MessageRequest], Awaitable[MessageResponse]]):
        self.handler = handler

    @staticmethod
    def make_http_response(after_http_slash: str, body: str | dict | None) -> bytes:
        result = "HTTP/" + after_http_slash + "\r\n"
        content_type: str
        if body is None:
            return result.encode()
        if isinstance(body, dict):
            body_b = json.dumps(body)
            content_type = "application/json"
        else:
            body_b = str(body)
            content_type = "text/plain"
        result += f"Content-Length: {len(body_b)}\r\n"
        result += f"Content-Type: {content_type}\r\n"
        result += "\r\n"
        result += body_b
        return result.encode()

    # noinspection PyMethodMayBeStatic
    async def send_request(self, node_id: int, message: MessageRequest) -> MessageResponse | None:
        if self.block_out[node_id]:
            await asyncio.sleep(60)
            return None
        try:
            reader, writer = await asyncio.open_connection(*config.NODES[node_id])

            writer.write(pickle.dumps(message))
            await self.time_print(f"#{node_id} <- {message}")
            await asyncio.sleep(self.node_latency[node_id] * random.triangular(0.95, 1.2, 1))
            await writer.drain()

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
            await self.time_print(f" OSError with {node_id}")

    async def receive_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        if self.hang:
            return
        message = await reader.read(1048576)
        try:
            message = pickle.loads(message)
        except PickleError:
            try:
                message = message.decode()
            except UnicodeError:
                await self.print(f"Bad request: {message}")
            else:
                await self.receive_http_request(message, writer)
            return

        sender_id: int
        if isinstance(message, messages.RequestVoteRequest):
            sender_id = message.candidate_id
        elif isinstance(message, messages.AppendEntriesRequest):
            sender_id = message.leader_id
        else:
            await self.print(f"Bad request: {message}")
            return

        if self.block_in[sender_id]:
            return

        await self.time_print(f"IN  #{sender_id} -> {message}")

        response = await self.handler(message)

        writer.write(pickle.dumps(response))
        await self.time_print(f"OUT #{sender_id} <- {response}")
        await asyncio.sleep(self.node_latency[sender_id] * random.triangular(0.95, 1.2, 1))
        await writer.drain()
        writer.close()

    async def receive_http_request(self, message: str, writer: asyncio.StreamWriter):
        print(message)
        m = re.match(r"^(GET|POST|PUT|DELETE|PATCH|TRACE|CONNECT) (\S+) HTTP/([\d.]+)", message)
        if not m:
            writer.write(b"Not HTTP request")
        elif m.group(2) == "/get":
            if m.group(1) == "GET":
                response = await self.handler(messages.GetDictionaryRequest())
                if not isinstance(response, messages.GetDictionaryResponse):
                    raise RuntimeError("Expected GetDictionaryResponse, got " + str(type(response)))
                writer.write(self.make_http_response(f"{m.group(3)} {response.status}", response.dictionary))
            else:
                writer.write(self.make_http_response(f"{m.group(3)} 405 Method Not Allowed", f"do GET /get"))
        elif m.group(2) == "/update":
            if m.group(1) == "POST":
                m2 = re.findall(r"[\n\r]([^\n\r]+)$", message)
                if not m2:
                    writer.write(self.make_http_response(f"{m.group(3)} 400 Bad Request", "Bad body"))
                else:
                    data = m2[-1]
                    try:
                        d = json.loads(data)
                    except JSONDecodeError:
                        writer.write(self.make_http_response(f"{m.group(3)} 400 Bad Request", "Bad JSON"))
                    else:
                        if not isinstance(d, dict):
                            writer.write(
                                self.make_http_response(f"{m.group(3)} 400 Bad Request", "JSON should be a dictionary"))
                        else:
                            response = await self.handler(messages.UpdateDictionaryRequest(d))
                            if not isinstance(response, messages.UpdateDictionaryResponse):
                                raise RuntimeError("Expected UpdateDictionaryResponse, got " + str(type(response)))
                            writer.write(self.make_http_response(f"{m.group(3)} {response.status}", response.message))
            else:
                writer.write(self.make_http_response(f"{m.group(3)} 405 Method Not Allowed", f"do POST /update"))
        else:
            writer.write(self.make_http_response(f"{m.group(3)} 404 Not Found", f"Unknown method"))

        await writer.drain()
        writer.close()
        print("CLOSED WRITER")

    async def run(self):
        server = await asyncio.start_server(self.receive_request, self.host, self.port)

        addr = server.sockets[0].getsockname()
        await self.print(f"Serving on {addr}")
        await self.print(f"given {self.host} {self.port}")

        async with server:
            await server.serve_forever()

    async def hang_requests(self, what):
        self.hang = what
