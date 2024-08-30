from enums.EnumAsClass import EnumAsClass


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
    REPORTING = "Reporting"
    TEST = "Test"

    @classmethod
    def features(cls):
        return cls.get_tables(filters=["Feature"])

    @classmethod
    def records(cls):
        return cls.get_tables(filters=["Record"])

    @classmethod
    def features_and_records(cls):
        return cls.get_tables(filters=["Feature", "Record"])

    @classmethod
    def data_tables(cls):
        return cls.get_tables(filters=["Feature", "Record", "Hospital", "Patient", "Sample"])

    @classmethod
    def get_tables(cls, filters):
        table_names = []
        for name, value in vars(cls).items():
            if not (name.startswith('__') or isinstance(value, classmethod)):
                for one_filter in filters:
                    if value.endswith(one_filter):
                        table_names.append(value)
        return table_names

