import pandas as pd
from pandas import DataFrame

from database.Execution import Execution
from preprocessing.Preprocess import Preprocess
from utils.setup_logger import log


class PreprocessImgge(Preprocess):
    def __init__(self, execution: Execution, data: DataFrame, metadata: DataFrame, profile: str):
        super().__init__(execution=execution, data=data, profile=profile)

    def preprocess(self):
        log.info("pre-process IMGGE data: nothing to do")

