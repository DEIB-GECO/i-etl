from enums.EnumAsClass import EnumAsClass
from enums.MetadataColumns import MetadataColumns


class DiagnosisColumns(EnumAsClass):
    ID = MetadataColumns.normalize_name(column_name="Id")
    AFFECTED = MetadataColumns.normalize_name(column_name="Affected")
    DIAGNOSIS_NAME = MetadataColumns.normalize_name(column_name="DiagnosisName")
    ACRONYM = MetadataColumns.normalize_name(column_name="Acronym")
    ORPHANET_CODE = MetadataColumns.normalize_name(column_name="OrphaNet")
    GENE_NAME = MetadataColumns.normalize_name(column_name="GeneName")
    INHERITANCE = MetadataColumns.normalize_name(column_name="VariantInheritance")
    CHR_NUMBER = MetadataColumns.normalize_name(column_name="ChromosomeNumber")
    ZIGOSITY = MetadataColumns.normalize_name(column_name="Zigosity")


