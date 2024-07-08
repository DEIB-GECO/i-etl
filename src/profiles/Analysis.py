from datatypes.CodeableConcept import CodeableConcept
from profiles.Resource import Resource
from utils.Counter import Counter
from profiles.InputOutput import InputOutput


class Analysis(Resource):
    def __init__(self, id_value: int, method_type: CodeableConcept, change_type: CodeableConcept,
                 genome_build: CodeableConcept, the_input: InputOutput, the_output: InputOutput, counter: Counter):
        super().__init__(id_value, self.get_type(), counter)

        self._method_type = method_type
        self._change_type = change_type
        self._genome_build = genome_build
        self._input = the_input
        self._output = the_output

    def get_type(self) -> str:
        # do not use a TableName here as we do not create a specific table for them,
        # instead we nest them (as JSON dicts) in GenomicData analysis
        return "Analysis"

    def to_json(self) -> dict:
        return {
            "identifier": self.identifier.to_json(),
            "resourceType": self.get_type(),
            "methodType": self._method_type.to_json(),
            "changeType": self._change_type.to_json(),
            "genomeBuild": self._genome_build.to_json(),
            "input": self._input.to_json(),
            "output": self._output.to_json()
        }
