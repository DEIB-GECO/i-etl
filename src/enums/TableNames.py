class TableNames:
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
    TEST = "Test"

    @classmethod
    def values(cls):
        xs = []
        for name, value in vars(cls).items():
            if not (name.startswith('__') or isinstance(value, classmethod)):
                xs.append(value)
        return xs
