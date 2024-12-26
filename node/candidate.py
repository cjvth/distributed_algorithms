from __future__ import annotations

import asyncio
import random
from asyncio import TaskGroup

import config
import messages
from util import force_terminate_task_group, TerminateTaskGroup, NodeState
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from node.node import Node


async def gather_one_vote(self: Node, node: int, responses: list[messages.RequestVoteResponse | None],
                          votes_needed: int, votes_received: list[int]):
    response = await self.transporter.send_request(
        node,
        messages.RequestVoteRequest(
            self.current_term,
            self.node_id,
            len(self.log) - 1,
            self.log[-1].term,
        )
    )
    if isinstance(response, messages.RequestVoteResponse):
        if response.vote_granted:
            votes_received[0] += 1
            if votes_received[0] >= votes_needed:
                self.candidate_stop.set()
        elif response.term > self.current_term:
            await self.print("Setting my state to follower!!")
            self.current_term = response.term
            self.voted_for = -1
            self.current_state = NodeState.FOLLOWER


async def gather_votes(self: Node) -> bool:
    votes_needed = len(config.NODES) // 2 + 1
    votes_received = [1]
    responses: list[messages.RequestVoteResponse | None] = []
    try:
        async with TaskGroup() as group:
            for i in filter(lambda x: x != self.node_id, config.NODES):
                group.create_task(gather_one_vote(self, i, responses, votes_needed, votes_received))
            timeout = random.uniform(config.MIN_ELECTION_TIMEOUT, config.MAX_ELECTION_TIMEOUT)
            try:
                self.candidate_stop.clear()
                await asyncio.wait_for(self.candidate_stop.wait(), timeout)
            except TimeoutError:
                pass
            group.create_task(force_terminate_task_group())

    except* TerminateTaskGroup:
        pass

    return votes_received[0] >= votes_needed


async def candidate_task(self: Node):
    while True:
        while True:
            await self.current_state_lock.acquire()
            if self.current_state != NodeState.CANDIDATE:
                self.current_state_lock.release()
            else:
                break

        while True:
            self.current_term += 1
            self.voted_for = self.node_id
            await self.new_epoch()
            im_new_leader = await gather_votes(self)
            if im_new_leader:
                self.current_state = NodeState.LEADER
                self.current_state_lock.release()
                break
            else:
                match self.current_state:
                    case NodeState.FOLLOWER:
                        await self.new_epoch()
                        self.current_state_lock.release()
                        break
                    case NodeState.LEADER:
                        raise RuntimeError("candidate -> LEADER")
                    case NodeState.CANDIDATE:
                        pass


async def handle_append_entries(
        self: Node,
        request: messages.AppendEntriesRequest) -> messages.AppendEntriesResponse:
    # await self.print(f"Received append entries {request}")
    if request.term < self.current_term:
        return messages.AppendEntriesResponse(False, self.current_term)
    self.current_term = request.term
    self.current_state = NodeState.FOLLOWER
    self.candidate_stop.set()
    return messages.AppendEntriesResponse(True, self.current_term)


async def handle_request_vote(
        self: Node,
        request: messages.RequestVoteRequest) -> messages.RequestVoteResponse:
    # await self.print(f"Received request vote {request}")
    if request.term > self.current_term:
        self.current_term = request.term
        self.voted_for = request.candidate_id
        self.current_state = NodeState.FOLLOWER
        self.candidate_stop.set()
        return messages.RequestVoteResponse(True, self.current_term)
    else:
        return messages.RequestVoteResponse(False, self.current_term)
