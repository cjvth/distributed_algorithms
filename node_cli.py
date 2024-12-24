import asyncio
import sys
from aioconsole import get_standard_streams

import config
from node import Node
from transporter import Transporter


async def main():
    number = int(sys.argv[1])
    host, port = config.nodes[number]
    transporter = Transporter(host, port)
    node = Node(number, transporter)

    async def read_cli():
        reader, writer = await get_standard_streams()
        while True:
            res = await reader.readline()
            writer.write(b"Repeat: ")
            writer.write(res)
            await writer.drain()

    await asyncio.gather(read_cli(), node.run(), transporter.run())


if __name__ == '__main__':
    asyncio.run(main())
