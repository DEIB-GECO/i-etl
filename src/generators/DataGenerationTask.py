from database.Execution import Execution
from enums.HospitalNames import HospitalNames
from generators.GeneratorBuzzi import GeneratorBuzzi
from generators.GeneratorDummy import GeneratorDummy
from generators.GeneratorHsjd import GeneratorHsjd
from generators.GeneratorImgge import GeneratorImgge
from generators.GeneratorUC2 import GeneratorUC2
from generators.GeneratorUC3 import GeneratorUC3
from utils.setup_logger import log


class DataGenerationTask:
    def __init__(self, execution: Execution):
        self.execution = execution

    def run(self):
        # 0. preprocess all the files given by the user
        # this task is specific to each hospital
        dg = None
        if self.execution.hospital_name == HospitalNames.IT_BUZZI_UC1:
            dg = GeneratorBuzzi(execution=self.execution)
        elif self.execution.hospital_name == HospitalNames.RS_IMGGE:
            dg = GeneratorImgge(execution=self.execution)
        elif self.execution.hospital_name == HospitalNames.ES_HSJD:
            dg = GeneratorHsjd(execution=self.execution)
        elif self.execution.hospital_name in [HospitalNames.ES_LAFE, HospitalNames.IL_HMC]:
            dg = GeneratorUC2(execution=self.execution)
        elif self.execution.hospital_name in [HospitalNames.IT_BUZZI_UC3, HospitalNames.DE_UKK, HospitalNames.ES_TERRASSA]:
            dg = GeneratorUC3(execution=self.execution)
        elif self.execution.hospital_name == HospitalNames.TEST_H1:
            dg = GeneratorDummy(execution=self.execution)
        else:
            log.error(f"Unknown hospital name {self.execution.hospital_name}. Cannot generate data.")
        # this writes generated data in the folder given in the .env,
        # folder to which a sub-folder named with the hospital name is append
        if dg is not None:
            dg.generate()
