NODES = {
    1: ("localhost", "5001"),
    2: ("localhost", "5002"),
    3: ("localhost", "5003"),
    4: ("localhost", "5004"),
    5: ("localhost", "5005"),
}

INITIAL_LEADER = -1

BASE_TIME = 2  # sec

HEARTBEAT_TIMEOUT = BASE_TIME
ELECTION_TIMEOUT = 2 * BASE_TIME

LEADER_HEARTBEAT_PERIOD = 1.0 * BASE_TIME
