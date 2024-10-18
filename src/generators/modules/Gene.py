import random

import pandas as pd

from utils.file_utils import get_ground_data

sjd_diseases = pd.read_csv(get_ground_data("SJD_Diseases.csv"), sep=',')


class Gene:
    def __init__(self):
        pass

    @staticmethod
    def generate_chromosome(*arguments):
        options = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18",
                   "19", "20", "21", "22", "X", "Y"]
        return random.choice(options)

    @staticmethod
    def generate_transcript():
        n = random.randint(a=1000, b=9999)
        return f'NM_000{n}'

    @staticmethod
    def get_gene_from_disease(disease):
        x1 = sjd_diseases.loc[sjd_diseases['Disease'] == disease]
        genes = None
        for i, row in x1.iterrows():
            genes = row["AffectedGenes"]
            break
        gene_list = genes.split(',')

        return random.choice(gene_list)
