from enums.EnumAsClass import EnumAsClass
from utils.setup_logger import log


class TableNames(EnumAsClass):
    HOSPITAL = "Hospital"
    PATIENT = "Patient"
    FEATURE = "Feature"
    RECORD = "Record"
    TOP10_FEATURES = "TMP_Top10Features"
    PAIRS_FEATURES = "TMP_PairsFeatures"
    COUNTS_PATIENTS = "TMP_CountsPatients"
    COUNTS_SAMPLES = "TMP_CountsSamples"
    COUNTS_FEATURES = "TMP_CountsFeatures"
    FEATURE_PROFILE = "FeatureProfile"
    # PHENOTYPIC_FEATURE = "PhenotypicFeature"
    # PHENOTYPIC_RECORD = "PhenotypicRecord"
    # CLINICAL_FEATURE = "ClinicalFeature"
    # CLINICAL_RECORD = "ClinicalRecord"
    # DIAGNOSIS_FEATURE = "DiagnosisFeature"
    # DIAGNOSIS_RECORD = "DiagnosisRecord"
    # GENOMIC_FEATURE = "GenomicFeature"
    # GENOMIC_RECORD = "GenomicRecord"
    # MEDICINE_FEATURE = "MedicineFeature"
    # MEDICINE_RECORD = "MedicineRecord"
    # IMAGING_FEATURE = "ImagingFeature"
    # IMAGING_RECORD = "ImagingRecord"
    EXECUTION = "Execution"
    STATS_DB = "DatabaseStatistics"
    STATS_TIME = "TimeStatistics"
    STATS_QUALITY = "QualityStatistics"
    TEST = "Test"
    DATASET = "Dataset"
    VIEW_FEATURES_DATASET = "ViewFeaturesDatasets"

    # IMPORTANT NOTE:
    # do NOT import Database for type hinting in methods defined here
    # otherwise, this creates a circular dependency between Database and TableNames

    @classmethod
    def data_tables(cls) -> list:
        return [TableNames.HOSPITAL, TableNames.PATIENT, TableNames.RECORD, TableNames.FEATURE, TableNames.DATASET]

    @classmethod
    def values(cls, db):
        # override the values() method of Enum to only return tables that exists in the DB
        table_names = []
        for name, value in vars(cls).items():
            if not (name.startswith('__') or isinstance(value, classmethod)) and (db is None or (db.check_table_exists(table_name=value) and db is not None)):
                # if DB is None, we do not want to check particularly whether the tables exist or not (we simply want to iterate over the table names)
                # if DB is not None, we want to check that the table exists in the DB
                if "Statistics" not in value and "Execution" not in value:
                    # this is not a table for reporting stats
                    table_names.append(value)
        log.info(table_names)
        return table_names
