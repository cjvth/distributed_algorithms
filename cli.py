import asyncio
import fcntl
import os
import sys
from asyncio import StreamReader, StreamWriter

from aioconsole import get_standard_streams

import config
from node.node import Node
from transporter import Transporter


async def hang(transporter: Transporter):
    fl = fcntl.fcntl(sys.stdin, fcntl.F_GETFL)
    fcntl.fcntl(sys.stdin, fcntl.F_SETFL, fl & ~os.O_NONBLOCK)

    await transporter.hang_requests(True)
    print("Server is hanging. Print anything to unhang")
    while True:
        x = 1
        for i in range(1000000):
            x += i
        input()
        break
    print("Unhanging")

    await asyncio.sleep(0.001)
    await transporter.hang_requests(False)
    fcntl.fcntl(sys.stdin, fcntl.F_SETFL, fl)
    return


async def main():
    reader: StreamReader
    writer: StreamWriter
    reader, writer = await get_standard_streams()

    number = int(sys.argv[1])
    host, port = config.NODES[number]
    transporter = Transporter(host, port, writer)
    node = Node(number, transporter, writer)

    async def read_cli():
        """
        hang
        block #
        reset
        """
        while True:
            res = await reader.readline()
            writer.write(b'> ' + res)
            if res.startswith(b"h"):
                await hang(transporter)
            elif res.startswith(b"b"):
                try:
                    s_id = int(res.split()[1])
                    transporter.block_in[s_id] = True
                    transporter.block_out[s_id] = True
                    writer.write(b'Blocked server for in and out connections')
                    await writer.drain()
                except (IndexError, ValueError):
                    writer.write(b'Should be "block id" with valid id')
                    await writer.drain()
            elif res.startswith(b"r"):
                transporter.reset()

    await asyncio.gather(read_cli(), node.run(), transporter.run())


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Terminated")
        exit(0)
