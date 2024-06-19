from src.datatypes.CodeableConcept import CodeableConcept
from src.profiles.Resource import Resource
from utils.Counter import Counter
from utils.InputOutput import InputOutput


class Analysis(Resource):
    def __init__(self, id_value: str, method_type: CodeableConcept, change_type: CodeableConcept,
                 genome_build: CodeableConcept, the_input: InputOutput, the_output: InputOutput, counter: Counter):
        super().__init__(id_value, self.get_type(), counter)

        self.method_type = method_type
        self.change_type = change_type
        self.genome_build = genome_build
        self.input = the_input
        self.output = the_output

    def get_type(self):
        # do not use a TableName here as we do not create a specific table for them,
        # instead we nest them (as JSON dicts) in GenomicData analysis
        return "Analysis"

    def to_json(self):
        return {
            "identifier": self.identifier.to_json(),
            "resourceType": self.get_type(),
            "methodType": self.method_type.to_json(),
            "changeType": self.change_type.to_json(),
            "genomeBuild": self.genome_build.to_json(),
            "input": self.input.to_json(),
            "output": self.output.to_json()
        }
