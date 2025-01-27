from __future__ import annotations

import asyncio
from asyncio import TaskGroup

import config
import messages
from util import NodeState, force_terminate_task_group, TerminateTaskGroup, LogEntry
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from node.node import Node


async def init_after_election(self: Node):
    self.next_index = {i: len(self.log) for i in config.NODES}
    self.match_index = {i: 0 for i in config.NODES}


async def cause_heartbeat_timeout(self: Node):
    try:
        self.heartbeat_timeout.reschedule(asyncio.get_running_loop().time())
    except RuntimeError:
        pass


async def cancel_append_requests(self: Node):
    while self.append_requests:
        i: asyncio.Event
        for i in self.append_requests:
            i.set()
        await asyncio.sleep(0.001)


async def update_one_follower(self: Node, node: int, responses: dict[int, messages.AppendEntriesResponse | None]):
    prev_log_index = self.next_index[node] - 1
    log_len = len(self.log)
    response = await self.transporter.send_request(
        node,
        messages.AppendEntriesRequest(
            self.current_term,
            self.node_id,
            prev_log_index,
            self.log[prev_log_index].term,
            self.log[prev_log_index + 1:],
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
    if response.success:
        self.next_index[node] = log_len
        self.match_index[node] = log_len - 1
    else:
        self.next_index[node] = max(1, self.next_index[node] - 1)


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

            await self.print(f"Current log: {self.log}\n")
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


async def handle_update_dictionary(
        self: Node,
        request: messages.UpdateDictionaryRequest
):
    self.log.append(LogEntry(self.current_term, self.log[-1].dictionary | request.message))
    return messages.UpdateDictionaryResponse("201 Created", None)
