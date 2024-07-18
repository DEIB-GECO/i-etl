from enum import Enum


class UpsertPolicy(Enum):
    DO_NOTHING = "DO_NOTHING"
    REPLACE = "REPLACE"
