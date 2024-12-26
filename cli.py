import asyncio
import fcntl
import os
import sys
from aioconsole import get_standard_streams

import config
from node.node import Node
from transporter import Transporter


async def main():
    fl = fcntl.fcntl(sys.stdout, fcntl.F_GETFL)
    fcntl.fcntl(sys.stdout, fcntl.F_SETFL, fl | os.O_NONBLOCK)
    fl = fcntl.fcntl(sys.stdin, fcntl.F_GETFL)
    fcntl.fcntl(sys.stdin, fcntl.F_SETFL, fl | os.O_NONBLOCK)

    reader, writer = await get_standard_streams()

    number = int(sys.argv[1])
    host, port = config.NODES[number]
    transporter = Transporter(host, port, writer)
    node = Node(number, transporter, writer)

    async def read_cli():
        while True:
            res = await reader.readline()
            writer.write(b"Repeat: ")
            writer.write(res)
            await writer.drain()

    await asyncio.gather(read_cli(), node.run(), transporter.run())


if __name__ == '__main__':
    asyncio.run(main())
