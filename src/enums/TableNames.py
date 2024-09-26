from enums.EnumAsClass import EnumAsClass
from utils.setup_logger import log


class TableNames(EnumAsClass):
    HOSPITAL = "Hospital"
    PATIENT = "Patient"
    LABORATORY_FEATURE = "LaboratoryFeature"
    LABORATORY_RECORD = "LaboratoryRecord"
    SAMPLE_FEATURE = "SampleFeature"
    SAMPLE_RECORD = "SampleRecord"
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
    def values(cls, db):
        # override the values() method of Enum to only return tables that exists in the DB
        log.info(db)
        xs = []
        for name, value in vars(cls).items():
            if not (name.startswith('__') or isinstance(value, classmethod)) and (db is None or (db.check_table_exists(table_name=value) and db is not None)):
                # if DB is None, we do not want to check particularly whether the tables exist or not (we simply want to iterate over the table names)
                # if DB is not None, we want to check that the table exists in the DB
                xs.append(value)
        return xs

    @classmethod
    def get_feature_table_from_record_table(cls, record_table_name: str) -> str:
        if not record_table_name.endswith("Record"):
            # this is not a Record table name
            return None
        else:
            extracted_entity = record_table_name.replace("Record", "")
            for feature_table_name in cls.features(db=None):
                if feature_table_name.startswith(extracted_entity):
                    return feature_table_name
            # no associated Feature table name found
            return None

    @classmethod
    def features(cls, db):
        return cls.get_tables(filters=["Feature"], db=db)

    @classmethod
    def records(cls, db):
        return cls.get_tables(filters=["Record"], db=db)

    @classmethod
    def features_and_records(cls, db):
        return cls.get_tables(filters=["Feature", "Record"], db=db)

    @classmethod
    def data_tables(cls, db):
        return cls.get_tables(filters=["Feature", "Record", "Hospital", "Patient"], db=db)

    @classmethod
    def get_tables(cls, filters: list, db):
        table_names = []
        log.info(db)
        for name, value in vars(cls).items():
            if not (name.startswith('__') or isinstance(value, classmethod)):
                if db is None or (db.check_table_exists(table_name=value) and db is not None):
                    for one_filter in filters:
                        if value.endswith(one_filter):
                            table_names.append(value)
        return table_names

