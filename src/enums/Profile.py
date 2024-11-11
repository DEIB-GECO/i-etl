import os

from constants.structure import DOCKER_FOLDER_ANONYMIZED_PATIENT_IDS, DOCKER_FOLDER_TEST, DOCKER_FOLDER_METADATA, \
    DOCKER_FOLDER_DATA
from enums.EnumAsClass import EnumAsClass
from enums.ParameterKeys import ParameterKeys
from enums.TableNames import TableNames
from utils.setup_logger import log


class Profile(EnumAsClass):
    PHENOTYPIC = "phenotypic"
    CLINICAL = "clinical"
    DIAGNOSIS = "diagnosis"
    MEDICINE = "medicine"
    GENOMIC = "genomic"
    IMAGING = "imaging"
    DIAGNOSIS_REGEX = "diagnosis_regex"
    PATIENT_IDS = "patient_ids"
    METADATA = "metadata"

    @classmethod
    def get_record_table_name_from_profile(cls, profile: str) -> str:
        if profile == Profile.PHENOTYPIC:
            return TableNames.PHENOTYPIC_RECORD
        elif profile == Profile.CLINICAL:
            return TableNames.CLINICAL_RECORD
        elif profile == Profile.DIAGNOSIS:
            return TableNames.DIAGNOSIS_RECORD
        elif profile == Profile.GENOMIC:
            return TableNames.GENOMIC_RECORD
        elif profile == Profile.MEDICINE:
            return TableNames.MEDICINE_RECORD
        elif profile == Profile.IMAGING:
            return TableNames.IMAGING_RECORD
        else:
            raise KeyError(f"Unrecognised profile {profile}.")

    @classmethod
    def normalize(cls, file_type: str) -> str:
        return file_type.lower().strip()

    @classmethod
    def get_execution_key(cls, filetype) -> str | None:
        if filetype in [Profile.PHENOTYPIC, Profile.CLINICAL, Profile.GENOMIC, Profile.IMAGING, Profile.MEDICINE, Profile.DIAGNOSIS]:
            return ParameterKeys.DATA_FILES
        elif filetype == Profile.DIAGNOSIS_REGEX:
            return ParameterKeys.DIAGNOSIS_REGEXES
        elif filetype == Profile.PATIENT_IDS:
            return ParameterKeys.ANONYMIZED_PATIENT_IDS
        elif filetype == Profile.METADATA:
            return ParameterKeys.METADATA_PATH
        else:
            return None

    @classmethod
    def get_prefix_for_path(cls, filetype: str) -> str | None:
        if os.getenv("CONTEXT_MODE") == "TEST":
            return DOCKER_FOLDER_TEST
        else:
            if filetype.lower() in [Profile.PHENOTYPIC, Profile.CLINICAL, Profile.GENOMIC, Profile.IMAGING, Profile.MEDICINE, Profile.DIAGNOSIS, Profile.DIAGNOSIS_REGEX]:
                # TODO Nelly: see how I can move the regex diagnosis file in the metadata folder
                return DOCKER_FOLDER_DATA
            elif filetype.lower() in [Profile.METADATA]:
                return DOCKER_FOLDER_METADATA
            elif filetype.lower() == Profile.PATIENT_IDS:
                return DOCKER_FOLDER_ANONYMIZED_PATIENT_IDS
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
