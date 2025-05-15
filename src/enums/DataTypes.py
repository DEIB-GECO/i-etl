from enums.EnumAsClass import EnumAsClass
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
    API = "api"
    LIST = "list"

    METADATA_NB_UNRECOGNIZED_ETL_TYPE = 0

    @classmethod
    def normalize(cls, data_type: str) -> str:
        if data_type == "":
            return ""
        else:
            data_type = process_spaces(input_string=data_type)
            data_type = data_type.lower()

            if data_type in ["int", "integer"]:
                return DataTypes.INTEGER
            elif data_type in ["str", "string"]:
                return DataTypes.STRING
            elif data_type in ["category", "categorical"]:
                return DataTypes.CATEGORY
            elif data_type in ["float", "numeric"]:
                return DataTypes.FLOAT
            elif data_type in ["bool", "boolean"]:
                return DataTypes.BOOLEAN
            elif data_type == "image file":
                return DataTypes.IMAGE
            elif data_type == "date":
                return DataTypes.DATE
            elif data_type in ["datetime", "datetime64"]:
                return DataTypes.DATETIME
            elif data_type == "regex":
                return DataTypes.REGEX
            elif data_type == "api":
                return DataTypes.API
            elif data_type == "list":
                return DataTypes.LIST
            else:
                log.error(f"{data_type} is not a recognized data type; we will use string type by default.")
                DataTypes.METADATA_NB_UNRECOGNIZED_ETL_TYPE += 1
                return DataTypes.STRING

    @classmethod
    def numeric(cls) -> list:
        return [DataTypes.INTEGER, DataTypes.FLOAT]

    @classmethod
    def categorical(cls) -> list:
        return [DataTypes.STRING, DataTypes.BOOLEAN, DataTypes.CATEGORY, DataTypes.API, DataTypes.LIST]

    @classmethod
    def dates(cls) -> list:
        return [DataTypes.DATE, DataTypes.DATETIME]
