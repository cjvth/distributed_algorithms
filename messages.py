from dataclasses import dataclass

from pydantic import BaseModel

from util import LogEntry


class MessageRequest:
    pass


class MessageResponse:
    pass


@dataclass
class AppendEntriesRequest(MessageRequest):
    term: int
    leader_id: int
    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntry]
    leader_commit: int


@dataclass
class AppendEntriesResponse(MessageResponse):
    success: bool
    term: int


@dataclass
class RequestVoteRequest(MessageRequest):
    term: int
    candidate_id: int
    last_log_index: int
    last_log_term: int


@dataclass
class RequestVoteResponse(MessageResponse):
    vote_granted: bool
    term: int


@dataclass
class UpdateDictionaryRequest(MessageRequest):
    message: dict | str


@dataclass
class UpdateDictionaryResponse(MessageResponse):
    status: str
    message: str | None


@dataclass
class GetDictionaryRequest(MessageRequest):
    pass


@dataclass
class GetDictionaryResponse(MessageResponse):
    status: str
    dictionary: dict
