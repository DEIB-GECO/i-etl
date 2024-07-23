from enum import Enum


class TableNames(Enum):
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
