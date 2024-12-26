import asyncio
from asyncio import Timeout, StreamWriter

import config
import messages
from messages import MessageRequest, MessageResponse
from node import follower, leader, candidate
from transporter import Transporter
from util import LogEntry, NodeState


class Node:
    def __init__(self, node_id: int, transporter: Transporter, stdout: StreamWriter):
        self.transporter = transporter
        transporter.register_handler(self.handle_request)
        self.node_id = node_id

        self.current_term = 0
        self.voted_for: int | None = None
        self.log: list[LogEntry] = [LogEntry.initial()]
        self.commit_index = 0
        self.last_applied = 0

        self.next_index: list[int] = []
        self.match_index: list[int] = []

        if self.node_id == config.INITIAL_LEADER:
            self.current_state = NodeState.LEADER
        else:
            self.current_state = NodeState.FOLLOWER

        self.stdout = stdout

        self.current_state_lock = asyncio.Lock()

        self.election_timeout: Timeout | None = None
        self.heartbeat_timeout: Timeout | None = None

        self.candidate_stop = asyncio.Event()

    async def print(self, string: str | None = None):
        if string is not None:
            self.stdout.write(string.encode())
        self.stdout.write(b'\n')
        await self.stdout.drain()

    async def new_epoch(self):
        await self.print("\n\n\n\n################################")
        await self.print(f"Term: {self.current_term}; State: {self.current_state}")

    async def run(self):
        await self.print(f"Node running on {self.transporter.host}, {self.transporter.port}")
        await asyncio.gather(follower.follower_task(self), leader.leader_task(self), candidate.candidate_task(self))

    async def handle_request(self, request: MessageRequest) -> MessageResponse:
        if isinstance(request, messages.AppendEntriesRequest):
            match self.current_state:
                case NodeState.FOLLOWER:
                    return await follower.handle_append_entries(self, request)
                case NodeState.LEADER:
                    return await leader.handle_append_entries(self, request)
                case NodeState.CANDIDATE:
                    return await candidate.handle_append_entries(self, request)

        elif isinstance(request, messages.RequestVoteRequest):
            match self.current_state:
                case NodeState.FOLLOWER:
                    return await follower.handle_request_vote(self, request)
                case NodeState.LEADER:
                    return await leader.handle_request_vote(self, request)
                case NodeState.CANDIDATE:
                    return await candidate.handle_request_vote(self, request)
        else:
            await self.print(f"Bad request {type(request)} {request}")
