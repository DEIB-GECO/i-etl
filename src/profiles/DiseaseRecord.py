# from src.fhirDatatypes.CodeableConcept import CodeableConcept
# from src.fhirDatatypes.Reference import Reference
# from src.profiles.Disease import Disease
# from src.profiles.Hospital import Hospital
# from src.profiles.Patient import Patient
# from src.profiles.Resource import Resource
# from src.utils.utils import DISEASE_RECORD_TABLE_NAME
#
#
# class DiseaseRecord(Resource):
#     ID_COUNTER = 1
#
#     def __init__(self, clinical_status: str, subject: Patient, hospital: Hospital, severity: CodeableConcept, recorded_date: str):
#         super().__init__()
#
#         self.id = DiseaseRecord.ID_COUNTER
#         DiseaseRecord.ID_COUNTER = DiseaseRecord.ID_COUNTER + 1
#         self.clinical_status = clinical_status
#         self.recorded_by = Reference(hospital)
#         self.subject = Reference(subject)
#         self.instantiate = Reference(Disease)
#         self.severity = severity
#         self.recorded_date = recorded_date
#
#     def get_type(self):
#         return DISEASE_RECORD_TABLE_NAME
#
#     def to_json(self):
#         json_disease_record = {
#             "id": self.id,
#             "resourceType": "DiseaseRecord",
#             "clinicalStatus": self.clinical_status,
#             "recordedBy": self.recorded_by.to_json(),
#             "instantiateDisease": self.instantiate.to_json(),
#             "subject": self.subject.to_json(),
#             "severity": self.severity.to_json(),
#             "recordedDate": self.recorded_date
#         }
#
#         return json_disease_record
