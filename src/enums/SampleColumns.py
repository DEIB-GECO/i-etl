from enums.EnumAsClass import EnumAsClass
from enums.MetadataColumns import MetadataColumns


class SampleColumns(EnumAsClass):
    SAMPLE_BAR_CODE = MetadataColumns.normalize_name(column_name="SampleBarcode")
    SAMPLING = MetadataColumns.normalize_name(column_name="Sampling")
    SAMPLE_QUALITY = MetadataColumns.normalize_name(column_name="SampleQuality")
    SAMPLE_COLLECTED = MetadataColumns.normalize_name(column_name="SamTimeCollected")
    SAMPLE_RECEIVED = MetadataColumns.normalize_name(column_name="SamTimeReceived")
    SAMPLE_TOO_YOUNG = MetadataColumns.normalize_name(column_name="TooYoung")
    SAMPLE_BIS = MetadataColumns.normalize_name(column_name="BIS")
