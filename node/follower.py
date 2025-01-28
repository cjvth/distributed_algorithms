from __future__ import annotations

import asyncio
import random

import config
import messages
from node.common import common_handle_append_entries, print_log
from util import NodeState
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from node.node import Node


async def reset_election_timeout(self: Node):
    try:
        self.election_timeout.reschedule(asyncio.get_running_loop().time() + random.uniform(config.MIN_ELECTION_TIMEOUT,
                                                                                            config.MAX_ELECTION_TIMEOUT))
    except RuntimeError:
        pass
    await print_log(self)
    await self.print()


async def follower_task(self: Node):
    while True:
        await self.notify_follower.wait()
        self.notify_follower.clear()

        await self.new_epoch()
        try:
            async with asyncio.timeout(
                    random.uniform(config.MIN_ELECTION_TIMEOUT, config.MAX_ELECTION_TIMEOUT)) as timeout:
                self.election_timeout = timeout
                while True:
                    await asyncio.sleep(config.MAX_ELECTION_TIMEOUT)
        except TimeoutError:
            match self.current_state:
                case NodeState.FOLLOWER:
                    self.current_state = NodeState.CANDIDATE
                    self.notify_candidate.set()
                case NodeState.LEADER:
                    raise RuntimeError("follower -> LEADER")
                case NodeState.CANDIDATE:
                    raise RuntimeError("follower -> CANDIDATE")


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
    # success = len(self.log) >= request.prev_log_index + 1 and self.log[
    #     request.prev_log_index].term == request.prev_log_term
    # if request.leader_commit > self.commit_index:
    #     self.commit_index = min()
    return common_handle_append_entries(self, request)


async def handle_request_vote(
        self: Node,
        request: messages.RequestVoteRequest) -> messages.RequestVoteResponse:
    # await self.print(f"Received request vote {request}")
    if request.term <= self.current_term:
        return messages.RequestVoteResponse(False, self.current_term)
    if request.term > self.current_term:
        await reset_election_timeout(self)
        self.current_term = request.term
        self.voted_for = request.candidate_id
        await self.new_epoch()
        return messages.RequestVoteResponse(True, self.current_term)
    if self.voted_for is None or self.voted_for == request.candidate_id:
        await reset_election_timeout(self)
        return messages.RequestVoteResponse(True, self.current_term)
    return messages.RequestVoteResponse(False, self.current_term)
