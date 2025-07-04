from enums.DataTypes import DataTypes

from enums.EnumAsClass import EnumAsClass


class AggregationTypes(EnumAsClass):
    CATEGORICAL = "categorical"
    CONTINUOUS = "continuous"
    DATE = "date"

    @classmethod
    def get_agg_type(cls, data_type: str) -> str:
        if data_type in [DataTypes.CATEGORY, DataTypes.BOOLEAN, DataTypes.REGEX, DataTypes.STRING]:
            return AggregationTypes.CATEGORICAL
        elif data_type in [DataTypes.DATE, DataTypes.DATETIME]:
            return AggregationTypes.DATE
        else:
            return AggregationTypes.CONTINUOUS
