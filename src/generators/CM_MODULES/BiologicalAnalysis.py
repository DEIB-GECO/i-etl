import random
import pandas as pd

gene_to_pathways = pd.read_csv('DATA/gene2pathway_reactome.csv', sep=";")
# TODO: Completar los genes afectados
sjd_diseases = pd.read_csv('DATA/SJD_Diseases.csv', sep=',')

class BiologicalAnalysis:
    western_blot = None
    qpcr = None
    fluorescence_intensity = None
    protein_interaction = None
    subcellular_location = None
    cellular_morphology = None
    pathways = None
    splicing = None
    CADD = None
    metadome = None
    missense_3d = None
    dynamut2 = None
    acmg = None
    alphamiss = None

    
    def __init__(self):
        pass
    
    @staticmethod
    def generate_western_blot(options):
        return random.choice(list(options.keys()))
    
    @staticmethod
    def generate_qpcr(options):
        return random.choice(list(options.keys()))
    
    @staticmethod
    def generate_fluorescence_intensity(options):
        return random.choice(list(options.keys()))
    
    @staticmethod
    def generate_protein_interaction(options):
        return random.choice(list(options.keys()))
    
    @staticmethod
    def generate_subcellular_location(options):
        return random.choice(list(options.keys()))
    
    @staticmethod
    def generate_cellular_morphology(options):
        return random.choice(list(options.keys()))
    
    @staticmethod
    def generate_pathways(disease):
        x1 = sjd_diseases.loc[sjd_diseases['Disease'] == disease]
        if len(x1) != None:
            genes = None
            pathways = []
            for i, row in x1.iterrows():
                data = row['AffectedGenes']
                genes = data.split(',')
                break
            for gene in genes:
                x2 = gene_to_pathways.loc[gene_to_pathways['entity'] == gene]
                if len(x2) != None:
                    for i, row in x2.iterrows():
                        pathways.append(row['pathway'])
                        
            return (",").join(pathways)
        else:
            return None
    
    @staticmethod
    def generate_splicing(options):
        return random.choice(list(options.keys()))
    
    @staticmethod
    def generate_cadd():
        return random.randint(a=1, b=99)
    
    @staticmethod
    def generate_metadome(options):
        return random.choice(list(options.keys()))
    
    @staticmethod
    def generate_missense_3d(options):
        return random.choice(list(options.keys()))
    
    @staticmethod
    def generate_dynamut2(options):
        return random.choice(list(options.keys()))
    
    @staticmethod
    def generate_acmg(options):
        return random.choice(list(options.keys()))
    
    @staticmethod
    def generate_alphamiss():
        return round(random.uniform(a=0, b=1), 3)