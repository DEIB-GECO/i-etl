from constants.idColumns import DELIMITER_PATIENT_ID
from datatypes.Identifier import Identifier
from enums.TableNames import TableNames


class ResourceIdentifier(Identifier):
    def __init__(self, id_value: str, resource_type: str):
        if resource_type is None:
            # we are retrieving an existing resource id
            # thus, it is already of the form "resource_type:counter"
            super().__init__(value=id_value)
        else:
            # we are building a new resource ID
            # the only exception is for Sample, we do not append the prefix
            # for patient, we have anonymized ids of the form hospital_name:ID (hospital_name is given as the resource type)
            # for other resources, we have something of the form resource_type:ID
            if resource_type == TableNames.SAMPLE:
                resource_id = id_value
            else:
                resource_id = resource_type + DELIMITER_PATIENT_ID + id_value
            super().__init__(value=resource_id)

    def get_as_int(self):
        # # Resource identifiers (except Patient ones, which override this method) are a stringified int, e.g., "1", "2", etc
        # # we only need to cast this value to int
        # return int(self.value)
        # Resource identifiers (except Patient ones) are composed of the resource type and an auto-increment, e.g., "LabFeat:1", "LabFeat:2", etc
        # we need to split it to get the int value, i.e., the auto-increment
        # this is used to sort Patients by their (int) ID
        split_resource_id = self.value.split(DELIMITER_PATIENT_ID)
        resource_id_part = split_resource_id[1]  # resource_id_part[0] is the resource type
        return int(resource_id_part)
