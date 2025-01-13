from typing import Any

import numpy as np
import pandas as pd

from enums.EnumAsClass import EnumAsClass
from utils.setup_logger import log


class Visibility(EnumAsClass):
    PUBLIC = "PUBLIC"
    ANONYMIZED = "ANONYMIZED"
    PRIVATE = "PRIVATE"

    METADATA_NB_UNRECOGNIZED_VISIBILITY = 0

    @classmethod
    def get_enum_from_name(cls, visibility_str: str):
        for existing_visibility in Visibility.values():
            if existing_visibility == visibility_str:
                return existing_visibility  # return the visibility enum
        # at the end of the loop, no enum value could match the given visibility
        # thus we need to raise an error
        raise ValueError(f"The given visibility value ({visibility_str}) does not correspond to any known visibility.")

    @classmethod
    def normalize(cls, visibility: str) -> str:
        if visibility == "":
            return ""
        else:
            visibility_u = visibility.upper()
            if visibility_u not in Visibility.values():
                log.error(f"{visibility_u} is not a recognized visibility; we will use 'PRIVATE' visibility by default.")
                Visibility.METADATA_NB_UNRECOGNIZED_VISIBILITY += 1
                return Visibility.PRIVATE
            else:
                return Visibility.get_enum_from_name(visibility)
