from dataclasses import dataclass
from enum import Enum
from typing import Any


@dataclass
class LogEntry:
    term: int
    dictionary: dict[str, Any]

    @classmethod
    def initial(cls):
        return cls(-1, {})


class NodeState(Enum):
    FOLLOWER = "Follower"
    LEADER = "Leader"
    CANDIDATE = "Candidate"


class TerminateTaskGroup(Exception):
    """Exception raised to terminate a task group."""


async def force_terminate_task_group():
    """Used to force termination of a task group."""
    raise TerminateTaskGroup()
