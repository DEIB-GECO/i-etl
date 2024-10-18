import os

from constants.structure import DOCKER_FOLDER_MEDICINE, DOCKER_FOLDER_DIAGNOSIS, DOCKER_FOLDER_GENOMIC, \
    DOCKER_FOLDER_PHENOTYPIC, DOCKER_FOLDER_IMAGING, DOCKER_FOLDER_ANONYMIZED_PATIENT_IDS, DOCKER_FOLDER_TEST, \
    DOCKER_FOLDER_SAMPLE, DOCKER_FOLDER_PREPROCESSED
from enums.EnumAsClass import EnumAsClass
from enums.ParameterKeys import ParameterKeys
from utils.setup_logger import log


class FileTypes(EnumAsClass):
    PHENOTYPIC = "phenotypic"
    SAMPLE = "sample"
    DIAGNOSIS = "diagnosis"
    MEDICINE = "medicine"
    GENOMIC = "genomic"
    IMAGING = "imaging"
    DIAGNOSIS_REGEX = "diagnosis_regex"
    PATIENT_IDS = "patient_ids"

    @classmethod
    def get_execution_key(cls, filetype) -> str | None:
        if filetype == FileTypes.PHENOTYPIC:
            return ParameterKeys.PHENOTYPIC_PATHS
        elif filetype == FileTypes.SAMPLE:
            return ParameterKeys.SAMPLE_PATHS
        elif filetype == FileTypes.DIAGNOSIS:
            return ParameterKeys.DIAGNOSIS_PATHS
        elif filetype == FileTypes.MEDICINE:
            return ParameterKeys.MEDICINE_PATHS
        elif filetype == FileTypes.GENOMIC:
            return ParameterKeys.GENOMIC_PATHS
        elif filetype == FileTypes.IMAGING:
            return ParameterKeys.IMAGING_PATHS
        elif filetype == FileTypes.DIAGNOSIS_REGEX:
            return ParameterKeys.DIAGNOSIS_REGEXES
        elif filetype == FileTypes.PATIENT_IDS:
            return ParameterKeys.ANONYMIZED_PATIENT_IDS
        else:
            return None

    @classmethod
    def get_prefix_for_path(cls, filetype) -> str | None:
        if os.getenv("CONTEXT_MODE") == "TEST":
            return DOCKER_FOLDER_TEST
        else:
            if filetype == FileTypes.PHENOTYPIC:
                return DOCKER_FOLDER_PHENOTYPIC
            elif filetype == FileTypes.SAMPLE:
                return DOCKER_FOLDER_SAMPLE
            elif filetype == FileTypes.GENOMIC:
                return DOCKER_FOLDER_GENOMIC
            elif filetype == FileTypes.IMAGING:
                return DOCKER_FOLDER_IMAGING
            elif filetype == FileTypes.MEDICINE:
                return DOCKER_FOLDER_MEDICINE
            elif filetype == FileTypes.DIAGNOSIS:
                return DOCKER_FOLDER_DIAGNOSIS
            elif filetype == FileTypes.DIAGNOSIS_REGEX:
                return DOCKER_FOLDER_DIAGNOSIS
            elif filetype == FileTypes.PATIENT_IDS:
                return DOCKER_FOLDER_ANONYMIZED_PATIENT_IDS
            else:
                return None
