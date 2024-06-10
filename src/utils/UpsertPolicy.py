from enum import Enum


class UpsertPolicy(Enum):
    DO_NOTHING: 0
    REPLACE: 1
