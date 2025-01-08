from __future__ import annotations

import asyncio
from asyncio import TaskGroup

import config
import messages
from util import NodeState, force_terminate_task_group, TerminateTaskGroup
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from node.node import Node


async def init_after_election(self: Node):
    self.next_index = {i: len(self.log) + 1 for i in config.NODES}
    self.match_index = {i: 0 for i in config.NODES}


async def cause_heartbeat_timeout(self: Node):
    try:
        self.heartbeat_timeout.reschedule(asyncio.get_running_loop().time())
    except RuntimeError:
        pass

async def update_one_follower(self: Node, node: int, responses: dict[int, messages.AppendEntriesResponse | None]):
    # prevLogIndex = self.next_index[node]
    response = await self.transporter.send_request(
        node,
        messages.AppendEntriesRequest(
            self.current_term,
            self.node_id,
            len(self.log) - 1,
            self.log[-1].term,
            [],
            self.commit_index
        )
    )
    if not isinstance(response, messages.AppendEntriesResponse):
        responses[node] = None
        return
    responses[node] = response
    if response.term > self.current_term:
        print("Response term is bigger")
        self.current_term = response.term
        self.current_state = NodeState.FOLLOWER
        await cause_heartbeat_timeout(self)
        return


async def leader_task(self: Node):
    while True:

        await self.notify_leader.wait()
        self.notify_leader.clear()

        await init_after_election(self)
        await self.new_epoch()
        while True:
            responses: dict[int, messages.AppendEntriesResponse | None] = {}
            try:
                async with TaskGroup() as group:
                    try:
                        async with asyncio.timeout(config.HEARTBEAT_TIMEOUT) as timeout:
                            self.heartbeat_timeout = timeout
                            for i in filter(lambda x: x != self.node_id, config.NODES):
                                group.create_task(update_one_follower(self, i, responses))
                            await asyncio.sleep(config.HEARTBEAT_TIMEOUT * 2)
                    except TimeoutError:
                        group.create_task(force_terminate_task_group())
            except* TerminateTaskGroup:
                pass

            match self.current_state:
                case NodeState.FOLLOWER:
                    self.notify_follower.set()
                    break
                case NodeState.LEADER:
                    pass
                case NodeState.CANDIDATE:
                    raise RuntimeError("leader -> candidate")
            await self.print()


async def handle_append_entries(
        self: Node,
        request: messages.AppendEntriesRequest) -> messages.AppendEntriesResponse:
    # await self.print(f"Received append entries {request}")
    if request.term < self.current_term:
        return messages.AppendEntriesResponse(False, self.current_term)
    if request.term == self.current_term:
        raise RuntimeError("Two leaders with same term", self.current_term)
    self.current_term = request.term
    self.voted_for = -1
    self.current_state = NodeState.FOLLOWER
    await cause_heartbeat_timeout(self)
    return messages.AppendEntriesResponse(True, self.current_term)


async def handle_request_vote(
        self: Node,
        request: messages.RequestVoteRequest) -> messages.RequestVoteResponse:
    # await self.print(f"Received request vote {request}")
    if request.term <= self.current_term:
        return messages.RequestVoteResponse(False, self.current_term)
    if request.term > self.current_term:
        self.current_term = request.term
        self.voted_for = request.candidate_id
        self.current_state = NodeState.FOLLOWER
        await cause_heartbeat_timeout(self)
        return messages.RequestVoteResponse(True, self.current_term)
    return messages.RequestVoteResponse(False, self.current_term)
