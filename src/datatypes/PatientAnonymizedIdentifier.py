from datatypes.Identifier import Identifier
from enums.HospitalNames import HospitalNames
from utils.constants import DELIMITER_PATIENT_ID
from utils.setup_logger import log


class PatientAnonymizedIdentifier(Identifier):
    def __init__(self, id_value: str, hospital_name: str | None):
        if hospital_name is None:
            # we are retrieving an existing anonymized patient id
            # thus, it is already of the form "hospital_name:counter"
            super().__init__(value=id_value)
        else:
            # we are building a new Patient anonymized ID
            anonymized_patient_id = HospitalNames.short(hospital_name) + DELIMITER_PATIENT_ID + id_value
            super().__init__(value=anonymized_patient_id)

    def get_as_int(self):
        # Patient identifiers are composed of the hospital name and an auto-increment, e.g., "LAFE:1", "LAFE:2", etc
        # we need to split it to get the int value, i.e., the auto-increment
        # this is used to sort Patients by their (int) ID
        split_patient_id = self.value.split(DELIMITER_PATIENT_ID)
        patient_id_part = split_patient_id[1]  # split_patient_id[0] is the hospital name
        return int(patient_id_part)
