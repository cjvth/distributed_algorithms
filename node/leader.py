from __future__ import annotations

import asyncio

import config
import messages
from util import NodeState
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from node.node import Node


async def leader_task(self: Node):
    while True:
        while True:
            await self.current_state_lock.acquire()
            if self.current_state != NodeState.LEADER:
                self.current_state_lock.release()
            else:
                break

        await self.new_epoch()
        while True:
            try:
                async with asyncio.timeout(config.HEARTBEAT_TIMEOUT) as timeout:
                    self.heartbeat_timeout = timeout
                    tasks = [
                        self.transporter.send_request(
                            i,
                            messages.AppendEntriesRequest(
                                self.current_term,
                                self.node_id,
                                len(self.log) - 1,
                                self.log[-1].term,
                                [],
                                self.commit_index
                            )
                        )
                        for i in filter(lambda x: x != self.node_id, config.NODES)
                    ]
                    await asyncio.gather(*tasks)
                    await asyncio.sleep(config.HEARTBEAT_TIMEOUT)

            except TimeoutError:
                match self.current_state:
                    case NodeState.FOLLOWER:
                        self.current_state_lock.release()
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
    self.heartbeat_timeout.reschedule(asyncio.get_running_loop().time())
    await self.print("Rescheduled heartbeat timeout")
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
        self.heartbeat_timeout.reschedule(asyncio.get_running_loop().time())
        await self.print("Rescheduled heartbeat timeout")
        return messages.RequestVoteResponse(True, self.current_term)
    return messages.RequestVoteResponse(False, self.current_term)
