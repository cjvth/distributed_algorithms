from __future__ import annotations

from typing import TYPE_CHECKING

import messages
from util import NodeState

if TYPE_CHECKING:
    from node.node import Node


def handle_get_dictionary(self: Node, request: messages.GetDictionaryRequest) -> messages.GetDictionaryResponse:
    return messages.GetDictionaryResponse("200 OK", self.log[self.commit_index].dictionary)


def common_handle_append_entries(self: Node, request: messages.AppendEntriesRequest):
    if request.term < self.current_term:
        return messages.AppendEntriesResponse(False, self.current_term)
    if len(self.log) <= request.prev_log_index or \
            self.log[request.prev_log_index].term != request.prev_log_term:
        return messages.AppendEntriesResponse(False, self.current_term)
    self.log = self.log[:request.prev_log_index + 1] + request.entries
    if request.leader_commit > self.commit_index:
        self.commit_index = min(request.leader_commit, len(self.log) - 1)
    return messages.AppendEntriesResponse(True, self.current_term)


async def print_log(self: Node):
    await self.print(f"Committed:\n{str(self.log[:self.commit_index + 1])}")
    if self.commit_index + 1 < len(self.log):
        await self.print(f"Not committed:\n{str(self.log[self.commit_index + 1:])}")
    await self.print("\n")


async def should_vote(self: Node, request: messages.RequestVoteRequest):
    if self.log[-1].term < request.last_log_term:
        await self.print(f"Better last term: {request.last_log_term}; My: {self.log}")
        return True
    if self.log[-1].term > request.last_log_term:
        return False
    if len(self.log) - 1 < request.last_log_index:
        await self.print(f"Better last index: {request.last_log_index}; My: {self.log}")
        return True
    if len(self.log) - 1 > request.last_log_index:
        return False
    await self.print(f"OK: term {request.last_log_term} index {request.last_log_index}; My: {self.log}")
    # return self.current_state != NodeState.CANDIDATE
    return True
