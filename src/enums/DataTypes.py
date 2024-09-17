from enums.EnumAsClass import EnumAsClass
from utils.assertion_utils import is_not_nan
from utils.setup_logger import log
from utils.str_utils import process_spaces


class DataTypes(EnumAsClass):
    INTEGER = "int"
    FLOAT = "float"
    STRING = "str"
    DATE = "date"
    DATETIME = "datetime"
    BOOLEAN = "bool"
    CATEGORY = "category"
    IMAGE = "image"
    REGEX = "regex"

    METADATA_NB_UNRECOGNIZED_VAR_TYPE = 0
    METADATA_NB_UNRECOGNIZED_ETL_TYPE = 0

    @classmethod
    def normalize(cls, data_type: str, is_etl: bool) -> str:
        if is_not_nan(data_type):
            data_type = process_spaces(input_string=data_type)
            data_type = data_type.lower()

            if data_type == "int" or data_type == "integer":
                return DataTypes.INTEGER
            elif data_type == "str" or data_type == "string":
                return DataTypes.STRING
            elif data_type == "category" or data_type == "categorical":
                return DataTypes.CATEGORY
            elif data_type == "float" or data_type == "numeric":
                return DataTypes.FLOAT
            elif data_type == "bool" or data_type == "boolean":
                return DataTypes.BOOLEAN
            elif data_type == "image file":
                return DataTypes.IMAGE
            elif data_type == "date":
                return DataTypes.DATE
            elif data_type == "datetime" or data_type == "datetime64":
                return DataTypes.DATETIME
            elif data_type == "regex":
                return DataTypes.REGEX
            else:
                log.error(f"{data_type} is not a recognized data type; we will use string type by default.")
                if is_etl:
                    DataTypes.METADATA_NB_UNRECOGNIZED_ETL_TYPE += 1
                else:
                    DataTypes.METADATA_NB_UNRECOGNIZED_VAR_TYPE += 1
                return DataTypes.STRING
        else:
            return data_type