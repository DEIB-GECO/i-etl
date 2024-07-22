from datatypes.CodeableConcept import CodeableConcept
from profiles.Resource import Resource
from utils.Counter import Counter
from profiles.InputOutput import InputOutput


class Analysis(Resource):
    def __init__(self, id_value: str, method_type: CodeableConcept, change_type: CodeableConcept,
                 genome_build: CodeableConcept, the_input: InputOutput, the_output: InputOutput, counter: Counter):
        super().__init__(id_value, "Analysis", counter)

        self.method_type = method_type
        self.change_type = change_type
        self.genome_build = genome_build
        self.input = the_input
        self.output = the_output
