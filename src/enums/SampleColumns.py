from enums.EnumAsClass import EnumAsClass
from utils.utils import normalize_column_name


class SampleColumns(EnumAsClass):
    SAMPLE_BAR_CODE = normalize_column_name(column_name="SampleBarcode")
    SAMPLING = normalize_column_name(column_name="Sampling")
    SAMPLE_QUALITY = normalize_column_name(column_name="SampleQuality")
    SAMPLE_COLLECTED = normalize_column_name(column_name="SamTimeCollected")
    SAMPLE_RECEIVED = normalize_column_name(column_name="SamTimeReceived")
    SAMPLE_TOO_YOUNG = normalize_column_name(column_name="TooYoung")
    SAMPLE_BIS = normalize_column_name(column_name="BIS")
