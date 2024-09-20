from enums.EnumAsClass import EnumAsClass
from utils.setup_logger import log


class TableNames(EnumAsClass):
    HOSPITAL = "Hospital"
    PATIENT = "Patient"
    LABORATORY_FEATURE = "LaboratoryFeature"
    LABORATORY_RECORD = "LaboratoryRecord"
    SAMPLE = "Sample"
    DIAGNOSIS_FEATURE = "DiagnosisFeature"
    DIAGNOSIS_RECORD = "DiagnosisRecord"
    GENOMIC_FEATURE = "GenomicFeature"
    GENOMIC_RECORD = "GenomicRecord"
    MEDICINE_FEATURE = "MedicineFeature"
    MEDICINE_RECORD = "MedicineRecord"
    IMAGING_FEATURE = "ImagingFeature"
    IMAGING_RECORD = "ImagingRecord"
    EXECUTION = "Execution"
    STATS_DB = "DatabaseStatistics"
    STATS_TIME = "TimeStatistics"
    STATS_QUALITY = "QualityStatistics"
    TEST = "Test"

    # IMPORTANT NOTE:
    # do NOT import Database for type hinting in methods defined here
    # otherwise, this creates a circular dependency between Database and TableNames

    @classmethod
    def values(cls, check_exists: bool, db):
        # override the values() method of Enum to only return tables that exists in the DB
        log.info(db)
        xs = []
        for name, value in vars(cls).items():
            if not (name.startswith('__') or isinstance(value, classmethod)) and db.check_table_exists(table_name=value):
                xs.append(value)
        return xs

    @classmethod
    def features(cls, check_exists: bool, db):
        return cls.get_tables(filters=["Feature"], db=db, check_exists=check_exists)

    @classmethod
    def records(cls, check_exists: bool, db):
        return cls.get_tables(filters=["Record"], db=db, check_exists=check_exists)

    @classmethod
    def features_and_records(cls, check_exists: bool, db):
        return cls.get_tables(filters=["Feature", "Record"], db=db, check_exists=check_exists)

    @classmethod
    def data_tables(cls, check_exists: bool, db):
        return cls.get_tables(filters=["Feature", "Record", "Hospital", "Patient", "Sample"], db=db, check_exists=check_exists)

    @classmethod
    def get_tables(cls, filters: list, check_exists: bool, db):
        table_names = []
        log.info(db)
        for name, value in vars(cls).items():
            if not (name.startswith('__') or isinstance(value, classmethod)):
                if check_exists and db.check_table_exists(table_name=value) or not check_exists:
                    for one_filter in filters:
                        if value.endswith(one_filter):
                            table_names.append(value)
        return table_names

