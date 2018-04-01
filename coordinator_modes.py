from enum import Enum

class Mode(Enum):
    ELECTION = 1
    WORKER = 2
    OVERSEER = 3
    SHUTDOWN = 4
