from pandas import DataFrame

from database.Execution import Execution
from generators.DataGenerator import DataGenerator


class GeneratorDummy(DataGenerator):
    def __init__(self, execution: Execution):
        super().__init__(execution=execution)

    def generate(self):
        self.execution.nb_rows = 3
        data_1 = DataFrame({"calories": [420, 380, 390], "duration": [50, 40, 45]})
        self.save_generated_file(df=data_1, filename="dummy.csv")
