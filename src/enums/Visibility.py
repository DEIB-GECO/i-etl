from enums.EnumAsClass import EnumAsClass
from utils.setup_logger import log


class Visibility(EnumAsClass):
    PUBLIC_WITHOUT_ANONYMIZATION = "PUBLIC_WITHOUT_ANONYMIZATION"
    PUBLIC_WITH_ANONYMIZATION = "PUBLIC_WITH_ANONYMIZATION"
    PRIVATE = "PRIVATE"

    @classmethod
    def get_enum_from_name(cls, visibility_str: str):
        log.info(visibility_str)
        for existing_ontology in Visibility.values():
            if existing_ontology == visibility_str:
                return existing_ontology  # return the ontology enum
        # at the end of the loop, no enum value could match the given ontology
        # thus we need to raise an error
        raise ValueError(f"The given visibility value ({visibility_str}) does not correspond to any known visibility.")
