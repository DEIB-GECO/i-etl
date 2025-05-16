import os

from constants.structure import DOCKER_FOLDER_TEST, DOCKER_FOLDER_METADATA, \
    DOCKER_FOLDER_DATA
from enums.EnumAsClass import EnumAsClass
from enums.ParameterKeys import ParameterKeys


class Profile(EnumAsClass):
    PHENOTYPIC = "phenotypic"
    CLINICAL = "clinical"
    DIAGNOSIS = "diagnosis"
    MEDICINE = "medicine"
    GENOMIC = "genomic"
    IMAGING = "imaging"
    PATIENT_IDS = "patient_ids"
    METADATA = "metadata"

    @classmethod
    def normalize(cls, file_type: str) -> str:
        return file_type.lower().strip()

    @classmethod
    def get_prefix_for_path(cls, filetype: str) -> str | None:
        if os.getenv("CONTEXT_MODE") == "TEST":
            return DOCKER_FOLDER_TEST
        else:
            if filetype.lower() in [Profile.PHENOTYPIC, Profile.CLINICAL, Profile.GENOMIC, Profile.IMAGING, Profile.MEDICINE, Profile.DIAGNOSIS]:
                return DOCKER_FOLDER_DATA
            elif filetype.lower() in [Profile.METADATA]:
                return DOCKER_FOLDER_METADATA
            # elif filetype.lower() == Profile.PATIENT_IDS:
            #     return DOCKER_FOLDER_ANONYMIZED_PATIENT_IDS
            else:
                return None

    @classmethod
    def get_preprocess_data_filename(cls, filetype) -> str:
        if filetype == Profile.PHENOTYPIC:
            return "phenotypic.csv"
        elif filetype == Profile.CLINICAL:
            return "samples.csv"
        elif filetype == Profile.DIAGNOSIS:
            return "diagnosis.csv"
        else:
            return None
