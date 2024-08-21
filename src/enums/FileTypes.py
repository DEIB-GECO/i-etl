from database.Execution import Execution
from enums.EnumAsClass import EnumAsClass


class FileTypes(EnumAsClass):
    LABORATORY = "laboratory"
    DIAGNOSIS = "diagnosis"
    MEDICINE = "medicine"
    GENOMIC = "genomic"
    IMAGING = "imaging"

    @classmethod
    def get_execution_key(cls, datatype) -> str|None:
        if datatype == FileTypes.LABORATORY:
            return Execution.LABORATORY_PATHS_KEY
        elif datatype == FileTypes.DIAGNOSIS:
            return Execution.DIAGNOSIS_PATHS_KEY
        elif datatype == FileTypes.MEDICINE:
            return Execution.MEDICINE_PATHS_KEY
        elif datatype == FileTypes.GENOMIC:
            return Execution.GENOMIC_PATHS_KEY
        elif datatype == FileTypes.IMAGING:
            return Execution.IMAGING_PATHS_KEY
        else:
            return None
