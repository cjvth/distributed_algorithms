from __future__ import annotations

from typing import TYPE_CHECKING

import messages

if TYPE_CHECKING:
    from node.node import Node


def handle_get_dictionary(self: Node, request: messages.GetDictionaryRequest) -> messages.GetDictionaryResponse:
    # TODO
    # return messages.GetDictionaryResponse("200 OK", self.log[self.commit_index].dictionary)
    return messages.GetDictionaryResponse("200 OK", self.log[-1].dictionary)


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
