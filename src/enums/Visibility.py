from enums.EnumAsClass import EnumAsClass
from utils.assertion_utils import is_not_nan
from utils.setup_logger import log


class Visibility(EnumAsClass):
    PUBLIC_WITHOUT_ANONYMIZATION = "PUBLIC_WITHOUT_ANONYMIZATION"
    PUBLIC_WITH_ANONYMIZATION = "PUBLIC_WITH_ANONYMIZATION"
    PRIVATE = "PRIVATE"

    METADATA_NB_UNRECOGNIZED_VISIBILITY = 0

    @classmethod
    def get_enum_from_name(cls, visibility_str: str):
        for existing_ontology in Visibility.values():
            if existing_ontology == visibility_str:
                return existing_ontology  # return the ontology enum
        # at the end of the loop, no enum value could match the given ontology
        # thus we need to raise an error
        raise ValueError(f"The given visibility value ({visibility_str}) does not correspond to any known visibility.")

    @classmethod
    def normalize(cls, visibility: str) -> str:
        if is_not_nan(visibility):
            if visibility not in Visibility.values():
                log.error(f"{visibility} is not a recognized visibility; we will use private visibility by default.")
                Visibility.METADATA_NB_UNRECOGNIZED_VISIBILITY += 1
                return Visibility.PRIVATE
            else:
                return Visibility.get_enum_from_name(visibility)
        else:
            return visibility