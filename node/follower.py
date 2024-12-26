from __future__ import annotations

import asyncio
import random

import config
import messages
from util import NodeState
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from node.node import Node


async def reset_election_timeout(self: Node):
    self.election_timeout.reschedule(
        asyncio.get_running_loop().time() + config.ELECTION_TIMEOUT * random.uniform(0.5, 0.8))


async def follower_task(self: Node):
    while True:
        while True:
            await self.current_state_lock.acquire()
            if self.current_state != NodeState.FOLLOWER:
                self.current_state_lock.release()
            else:
                break

        await self.new_epoch()
        try:
            async with asyncio.timeout(config.ELECTION_TIMEOUT * random.uniform(0.5, 0.8)) as timeout:
                self.election_timeout = timeout
                while True:
                    await asyncio.sleep(config.ELECTION_TIMEOUT)
        except TimeoutError:
            match self.current_state:
                case NodeState.FOLLOWER:
                    self.current_state = NodeState.CANDIDATE
                case NodeState.LEADER:
                    raise RuntimeError("follower -> LEADER")
                case NodeState.CANDIDATE:
                    raise RuntimeError("follower -> CANDIDATE")

            self.current_state_lock.release()


async def handle_append_entries(
        self: Node,
        request: messages.AppendEntriesRequest) -> messages.AppendEntriesResponse:
    # await self.print(f"Received append entries {request}")
    if request.term < self.current_term:
        return messages.AppendEntriesResponse(False, self.current_term)
    await reset_election_timeout(self)
    if request.term > self.current_term:
        self.current_term = request.term
        self.voted_for = None
        await self.new_epoch()
    success = len(self.log) >= request.prev_log_index + 1 and self.log[
        request.prev_log_index].term == request.prev_log_term
    # if request.leader_commit > self.commit_index:
    #     self.commit_index = min()
    return messages.AppendEntriesResponse(success, self.current_term)


async def handle_request_vote(
        self: Node,
        request: messages.RequestVoteRequest) -> messages.RequestVoteResponse:
    # await self.print(f"Received request vote {request}")
    if request.term <= self.current_term:
        return messages.RequestVoteResponse(False, self.current_term)
    await reset_election_timeout(self)
    if request.term > self.current_term:
        self.current_term = request.term
        self.voted_for = request.candidate_id
        await self.new_epoch()
        return messages.RequestVoteResponse(True, self.current_term)
    if self.voted_for is None or self.voted_for == request.candidate_id:
        return messages.RequestVoteResponse(True, self.current_term)
    return messages.RequestVoteResponse(False, self.current_term)
