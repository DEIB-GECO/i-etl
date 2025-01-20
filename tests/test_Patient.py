from constants.defaults import NO_ID
from database.Counter import Counter
from entities.Patient import Patient
from enums.TableNames import TableNames
from utils.setup_logger import log


class TestPatient:
    def test_constructor(self):
        """
        Test whether the Patient constructor correctly assign IDs and the resource type.
        :return: None.
        """
        # this is a new Patient, thus with a new anonymised ID
        counter = Counter()
        patient1 = Patient(identifier=NO_ID, counter=counter)
        assert patient1.identifier is not None
        assert patient1.identifier == 1

        # this is an existing Patient, for which an anonymized ID already exists
        counter = Counter()
        patient1 = Patient(identifier=123, counter=counter)
        assert patient1.identifier is not None
        assert patient1.identifier == 123

    def test_to_json(self):
        counter = Counter()
        patient1 = Patient(identifier=NO_ID, counter=counter)
        patient1_json = patient1.to_json()

        log.info(patient1_json)

        assert patient1_json is not None
        assert "identifier" in patient1_json
        assert patient1_json["identifier"] == 1
        assert "timestamp" in patient1_json
        assert patient1_json["timestamp"] == patient1.timestamp
        assert "entity_type" in patient1_json
        assert patient1_json["entity_type"] == TableNames.PATIENT
