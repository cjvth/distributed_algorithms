NODES = {
    1: ("localhost", "5001"),
    2: ("localhost", "5002"),
    3: ("localhost", "5003"),
    4: ("localhost", "5004"),
    5: ("localhost", "5005"),
}

TOTAL_NODES = len(NODES)

MAJORITY_NODES = TOTAL_NODES // 2 + 1

INITIAL_LEADER = -1

BASE_TIME = 3  # sec

DEFAULT_DELAY = 0.25 * BASE_TIME

HEARTBEAT_TIMEOUT = BASE_TIME

MIN_ELECTION_TIMEOUT = 4 * BASE_TIME
MAX_ELECTION_TIMEOUT = 6 * BASE_TIME
