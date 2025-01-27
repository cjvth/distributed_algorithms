from __future__ import annotations

import asyncio
import json
import typing
from asyncio import Timeout, StreamWriter
from collections import deque

import config
import messages
from messages import MessageRequest, MessageResponse
from node import follower, leader, candidate
from util import LogEntry, NodeState

if typing.TYPE_CHECKING:
    from transporter import Transporter


class Node:
    def __init__(self, node_id: int, transporter: Transporter, stdout: StreamWriter):
        self.transporter = transporter
        transporter.register_handler(self.handle_request)
        self.node_id = node_id

        self.current_term = 0
        self.voted_for: int | None = None
        self.log: list[LogEntry] = [LogEntry.initial()]
        self.commit_index = 0
        # self.last_applied = 0

        self.next_index: dict[int, int] = {}
        self.match_index: dict[int, int] = {}

        self.notify_follower = asyncio.Event()
        self.notify_candidate = asyncio.Event()
        self.notify_leader = asyncio.Event()

        self.candidate_stop = asyncio.Event()

        if self.node_id == config.INITIAL_LEADER:
            self.current_state = NodeState.LEADER
            self.notify_leader.set()
        else:
            self.current_state = NodeState.FOLLOWER
            self.notify_follower.set()

        self.stdout = stdout

        self.append_requests = deque[asyncio.Event]

        self.election_timeout: Timeout | None = None
        self.heartbeat_timeout: Timeout | None = None

    async def print(self, string: str | None = None):
        if string is not None:
            self.stdout.write(string.encode())
        self.stdout.write(b'\n')
        await self.stdout.drain()

    async def new_epoch(self):
        await self.print("\n\n\n\n################################")
        await self.print(f"Term: {self.current_term}; State: {self.current_state}")

    async def run(self):
        await self.print(f"Node running on {self.transporter.host}:{self.transporter.port}")
        await asyncio.gather(follower.follower_task(self), leader.leader_task(self), candidate.candidate_task(self))

    async def handle_get_dictionary(self) -> dict:
        return self.log[self.commit_index].dictionary

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

        elif isinstance(request, messages.UpdateDictionaryRequest):
            if self.current_state == NodeState.LEADER:
                return await leader.handle_update_dictionary(self, request)
            else:
                return messages.UpdateDictionaryResponse("400 Bad Request", "Not a leader")

        elif isinstance(request, messages.GetDictionaryRequest):
            return messages.GetDictionaryResponse("200 OK", self.log[self.commit_index].dictionary)
        else:
            await self.print(f"Bad request {type(request)} {request}")

