import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import re

# Initialize Faker and seed for reproducibility
fake = Faker('en_US')
Faker.seed(42)
np.random.seed(42)
random.seed(42)

# Initialize number of patients per disease
PATIENTS_PER_DISEASE = 10

# Disease list
diseases = [
    {'disease_name': 'Aicardi-Goutieres syndrome 2', 'OMIM_Phenotype': '# 610181', 'OMIM_Gene': '* 610326', 'Gene': 'RNASEH2B', 'ICD_10': 'E79.81'},
    {'disease_name': 'Alexander disease type I', 'OMIM_Phenotype': '# 203450', 'OMIM_Gene': '* 137780', 'Gene': 'GFAP', 'ICD_10': 'G37.8'},
    {'disease_name': 'ALLAN-HERNDON-DUDLEY SYNDROME, AHDS', 'OMIM_Phenotype': '# 300523', 'OMIM_Gene': 'No', 'Gene': 'SLC16A2', 'ICD_10': 'E70.8'},
    {'disease_name': 'Alpha-methylacetoacetic aciduria', 'OMIM_Phenotype': '# 203750', 'OMIM_Gene': 'No', 'Gene': 'ACAT1', 'ICD_10': 'E71.19'},
    {'disease_name': 'Angiopathy, hereditary, with nephropathy, aneurysms, and muscle cramps; Brain small vessel disease with or without ocular anomalies', 'OMIM_Phenotype': '# 611773, #175780', 'OMIM_Gene': 'No', 'Gene': 'COL4A1', 'ICD_10': 'I77.9, N08, I72.9, Q87.89, I67.89, H49.9, G46.8, H55.9'},
    {'disease_name': 'Association of TRPC3 with epilepsy', 'OMIM_Phenotype': 'No', 'OMIM_Gene': '* 602345', 'Gene': 'TRPC3', 'ICD_10': 'G40.9, G40.8'},  
    {'disease_name': 'Ataxia-telangiectasia', 'OMIM_Phenotype': '# 208900', 'OMIM_Gene': 'No', 'Gene': 'ATM', 'ICD_10': 'g13.11'},     
    {'disease_name': 'Bardet-Biedl syndrome 7', 'OMIM_Phenotype': 'No', 'OMIM_Gene': '*607590', 'Gene': 'BBS7', 'ICD_10': 'Q87.83'},    
    {'disease_name': 'Borjeson-Forssman-Lehmann syndrome', 'OMIM_Phenotype': '# 301900', 'OMIM_Gene': 'No', 'Gene': 'PHF6', 'ICD_10': 'Q87.89'},
    {'disease_name': 'Brain small vessel disease 1 with or without ocular anomalies, BSVD1', 'OMIM_Phenotype': '# 175780', 'OMIM_Gene': 'No', 'Gene': 'COL4A1', 'ICD_10': 'I67.89, H35.8'},
    {'disease_name': 'Cardiomyopathy, dilated, 1S; Congenital myopathy 7A, myosin storage', 'OMIM_Phenotype': 'No', 'OMIM_Gene': '* 160760', 'Gene': 'MYH7', 'ICD_10': 'I42.0, G71.2'},
    {'disease_name': 'Ceroid lipofuscinosis, neuronal, 1', 'OMIM_Phenotype': '# 256730', 'OMIM_Gene': 'No', 'Gene': 'PPT1', 'ICD_10': 'E75.4'},
    {'disease_name': 'Ceroid lipofuscinosis, neuronal, 7', 'OMIM_Phenotype': '# 610951', 'OMIM_Gene': 'No', 'Gene': 'MFSD8', 'ICD_10': 'E75.4'},  
    {'disease_name': 'Cognitive impairment with or without cerebellar ataxia; Developmental and epileptic encephalopathy, DEE13', 'OMIM_Phenotype': '# 614306, # 614558', 'OMIM_Gene': 'No', 'Gene': 'SCN8A', 'ICD_10': 'R41.3, F84.0, G11.1, G40.0, F84.2'},
    {'disease_name': 'Combined oxidative phosphorylation deficiency 12', 'OMIM_Phenotype': '# 614924', 'OMIM_Gene': 'No', 'Gene': 'EARS2', 'ICD_10': 'E88.40'}, 
    {'disease_name': 'Darier disease', 'OMIM_Phenotype': '# 124200', 'OMIM_Gene': 'No', 'Gene': 'ATP2A2', 'ICD_10': 'L11.2'},
    {'disease_name': 'Developmental and epileptic encephalopathy 104', 'OMIM_Phenotype': 'No', 'OMIM_Gene': 'No', 'Gene': 'ATP6V0A1', 'ICD_10': 'G40.8'},
    {'disease_name': 'Developmental and epileptic encephalopathy 104, Neurodevelopmental disorder with impaired language and ataxia and with or without seizures, Epilepsy, nocturnal frontal lobe, 1', 'OMIM_Phenotype': '# 619970, # 619580, #600513', 'OMIM_Gene': 'No', 'Gene': 'ATP6V0A1,CHRNA4,GRIK2', 'ICD_10': 'G40.8, F84.9, G40.3'},
    {'disease_name': 'Developmental and epileptic encephalopathy 11', 'OMIM_Phenotype': '#613721', 'OMIM_Gene': 'No', 'Gene': 'SCN2A', 'ICD_10': 'G40.8, G40.9'},
    {'disease_name': 'Developmental and epileptic encephalopathy 11, Episodic ataxia, type 9, Seizures, benign familial infantile, 3; Dystonia-1, torsion', 'OMIM_Phenotype': '# 613721, # 618924, # 607745', 'OMIM_Gene': '* 182390', 'Gene': 'SCN2A,TOR1A', 'ICD_10': 'G40.8, G25.3, G40.4, G24.0'},
    {'disease_name': 'Developmental and epileptic encephalopathy 2 (DEE2)', 'OMIM_Phenotype': '', 'OMIM_Gene': 'No', 'Gene': 'CDKL5', 'ICD_10': 'G40.8'},
    {'disease_name': 'Developmental and epileptic encephalopathy 28, Spinocerebellar ataxia type 12', 'OMIM_Phenotype': '# 616211, # 614322', 'OMIM_Gene': 'No', 'Gene': 'WWOX', 'ICD_10': 'G40.8, G11.1'},
    {'disease_name': 'Developmental and epileptic encephalopathy 36, Blepharophimosis-impaired intellectual development syndrome, Nicolaides-Baraitser syndrome', 'OMIM_Phenotype': 'No', 'OMIM_Gene': '* 600014, * 300776', 'Gene': 'ALG13,SMARCA2', 'ICD_10': 'G40.9, Q87.1, Q87.3'},
    {'disease_name': 'Developmental and epileptic encephalopathy 5', 'OMIM_Phenotype': '# 613477', 'OMIM_Gene': 'No', 'Gene': 'SPTAN1', 'ICD_10': 'G40.9'},
    {'disease_name': 'Developmental and epileptic encephalopathy 6B', 'OMIM_Phenotype': '# 619317', 'OMIM_Gene': 'No', 'Gene': 'SCN1A', 'ICD_10': 'G40.9'},
    {'disease_name': 'Developmental and epileptic encephalopathy 6B, non-Dravet; Dravet syndrome; Generalised epilepsy with febrile seizures plus, type 2; Intellectual developmental disorder, X-linked syndromic 13', 'OMIM_Phenotype': ' # 300055', 'OMIM_Gene': '* 182389', 'Gene': 'SCN1A,MECP2', 'ICD_10': 'G40.9, G40.89, G40.8, Q93.0'},
    {'disease_name': 'Developmental and epileptic encephalopathy 7, DEE7', 'OMIM_Phenotype': ' 613720', 'OMIM_Gene': 'No', 'Gene': 'KCNQ2', 'ICD_10': 'G40.809'},
    {'disease_name': 'Ethylmalonic encephalopathy', 'OMIM_Phenotype': '# 602473', 'OMIM_Gene': 'No', 'Gene': 'ETHE1', 'ICD_10': 'E71.89'},
    {'disease_name': 'Glycine encephalopathy', 'OMIM_Phenotype': '# 620398', 'OMIM_Gene': 'No', 'Gene': 'AMT', 'ICD_10': 'E71.0'},
    {'disease_name': 'Hyperkalemic periodic paralysis', 'OMIM_Phenotype': '# 170500', 'OMIM_Gene': 'No', 'Gene': 'SCN4A', 'ICD_10': 'G72.3'},
    {'disease_name': 'Homocystinuria, B6-responsive and nonresponsive types', 'OMIM_Phenotype': '# 236200', 'OMIM_Gene': 'No', 'Gene': 'CBS', 'ICD_10': 'E72.11'},
    {'disease_name': 'Hyperphosphatasia with impaired intellectual development syndrome 1', 'OMIM_Phenotype': '# 239300', 'OMIM_Gene': '* 610274', 'Gene': 'PIGV', 'ICD_10': 'E83.39'},
    {'disease_name': 'Hypertrichotic osteochondrodysplasia (Cantu syndrome)', 'OMIM_Phenotype': '# 239850', 'OMIM_Gene': 'No', 'Gene': 'ABCC9', 'ICD_10': 'Q87.8'},
    {'disease_name': 'Hypotonia, infantile, with psychomotor retardation and characteristic facies 2', 'OMIM_Phenotype': '# 616801', 'OMIM_Gene': 'No', 'Gene': 'UNC80', 'ICD_10': 'Q87.0'},
    {'disease_name': 'Impaired intellectual development and distinctive facial features with or without cardiac defects', 'OMIM_Phenotype': '# 616789', 'OMIM_Gene': 'No', 'Gene': 'MED13L', 'ICD_10': 'Q87.0'},
    {'disease_name': 'Intellectual developmental disorder 5', 'OMIM_Phenotype': '# 612621', 'OMIM_Gene': 'No', 'Gene': 'SYNGAP1', 'ICD_10': 'F70'},
    {'disease_name': 'Intellectual developmental disorder with autism and macrocephaly', 'OMIM_Phenotype': '# 615032', 'OMIM_Gene': 'No', 'Gene': 'CHD8', 'ICD_10': 'F84.0, Q75.2'},
    {'disease_name': 'Intellectual developmental disorder, autosomal dominant 61, MRD61, Cleft palate, cardiac defects, and impaired intellectual development', 'OMIM_Phenotype': ' # 618009, #600987', 'OMIM_Gene': 'No', 'Gene': 'MED13,MEIS2', 'ICD_10': 'F70, Q35, Q20'},    
    {'disease_name': 'Intellectual developmental disorder, autosomal recessive 46', 'OMIM_Phenotype': '# 616116', 'OMIM_Gene': 'No', 'Gene': 'NDST1', 'ICD_10': 'Q93.8'},
    {'disease_name': 'Intellectual developmental disorder, X-linked', 'OMIM_Phenotype': '# 309530', 'OMIM_Gene': 'No', 'Gene': 'IQSEC2', 'ICD_10': 'Q93.2'},
    {'disease_name': 'Intellectual developmental disorder, X-linked 41', 'OMIM_Phenotype': '# 300849', 'OMIM_Gene': 'No', 'Gene': 'GDI1', 'ICD_10': 'Q93.2'},
    {'disease_name': 'Intellectual developmental disorder, X-linked syndromic 14', 'OMIM_Phenotype': '# 300676', 'OMIM_Gene': 'No', 'Gene': 'UPF3B', 'ICD_10': 'Q93.4'},
    {'disease_name': 'Intellectual developmental disorder, X-linked syndromic 35', 'OMIM_Phenotype': '# 300998', 'OMIM_Gene': 'No', 'Gene': 'RPL10', 'ICD_10': 'Q93.4'},
    {'disease_name': 'Intellectual developmental disorder, X-linked syndromic 35; King-Denborough and Congenital myopathy 1A, autosomal dominant, with susceptibility to malignant hyperthermia', 'OMIM_Phenotype': '# 300998', 'OMIM_Gene': '* 180901', 'Gene': 'RPL10,RYR1', 'ICD_10': 'F70, G72.1'},
    {'disease_name': 'Intellectual developmental disorder, X-linked syndromic, Turner type', 'OMIM_Phenotype': '# 309590', 'OMIM_Gene': 'No', 'Gene': 'HUWE1', 'ICD_10': 'Q87.2'},
    {'disease_name': 'Kabuki syndrome-2', 'OMIM_Phenotype': '# 300867', 'OMIM_Gene': 'No', 'Gene': 'KDM6A', 'ICD_10': 'Q87.8'},
    {'disease_name': 'L-2-HYDROXYGLUTARATE DEHYDROGENASE; L2HGDH', 'OMIM_Phenotype': 'No', 'OMIM_Gene': '* 609584', 'Gene': 'L2HGDH', 'ICD_10': 'Q87.8'},    
    {'disease_name': 'L-2-hydroxyglutaric aciduria, L2HGA', 'OMIM_Phenotype': '# 236792', 'OMIM_Gene': 'No', 'Gene': 'L2HGDH', 'ICD_10': 'E72.3'},
    {'disease_name': 'Leber congenital amaurosis', 'OMIM_Phenotype': '# 204000', 'OMIM_Gene': 'No', 'Gene': 'CEP290', 'ICD_10': 'H47.1'},
    {'disease_name': 'Lesch-Nyhan syndrome (LNS)', 'OMIM_Phenotype': '# 300322', 'OMIM_Gene': 'No', 'Gene': 'HPRT1', 'ICD_10': 'E79.1'},
    {'disease_name': 'Lowe syndrome', 'OMIM_Phenotype': '# 309000', 'OMIM_Gene': 'No', 'Gene': 'OCRL', 'ICD_10': 'E72.03'},
    {'disease_name': 'MED13-associated syndrome or Intellectual developmental disorder 61', 'OMIM_Phenotype': '# 618009, #143465', 'OMIM_Gene': '* 603808', 'Gene': 'MED13,DRD4', 'ICD_10': 'F70'},
    {'disease_name': 'METHYLMALONIC ACIDURIA, type cba1', 'OMIM_Phenotype': '# 251100', 'OMIM_Gene': 'No', 'Gene': 'MMAA', 'ICD_10': 'E71.2'},
    {'disease_name': 'Microcephaly with or without chorioretinopathy, lymphedema, or mental retardation, MCLID', 'OMIM_Phenotype': '# 152950', 'OMIM_Gene': 'No', 'Gene': 'KIF11', 'ICD_10': 'Q02'},
    {'disease_name': 'Mitochondrial DNA depletion syndrome 7', 'OMIM_Phenotype': '# 271245', 'OMIM_Gene': 'No', 'Gene': 'TWNK', 'ICD_10': 'E88.4'},
    {'disease_name': 'Myoclonic-atonic epilepsy', 'OMIM_Phenotype': '# 616421', 'OMIM_Gene': 'No', 'Gene': 'SLC6A1', 'ICD_10': 'G40.8'},
    {'disease_name': 'Nemaline myopathy 2, autosomal recessive; Scapuloperoneal spinal muscular atrophy i Congenital distal spinal muscular atrophy', 'OMIM_Phenotype': '# 256030', 'OMIM_Gene': '* 605427', 'Gene': 'NEB,TRPV4', 'ICD_10': 'G72.4, G12.0'},
    {'disease_name': 'Neurodegeneration due to cerebral folate transport deficiency', 'OMIM_Phenotype': '# 613068', 'OMIM_Gene': 'No', 'Gene': 'FOLR1', 'ICD_10': 'E75.2'},
    {'disease_name': 'Neurodevelopmental disorder with nonspecific brain abnormalities and with or without seizures', 'OMIM_Phenotype': '# 618709, # 300908', 'OMIM_Gene': 'No', 'Gene': 'DLL1', 'ICD_10': 'F88'},
    {'disease_name': 'Neurodevelopmental disorder with or without hyperkinetic movements and seizures, autosomal dominant', 'OMIM_Phenotype': '# 614254', 'OMIM_Gene': 'No', 'Gene': 'GRIN1', 'ICD_10': 'F82'},
    {'disease_name': 'Neurodevelopmental disorder with visual defects and brain anomalies (NEDVIBA)', 'OMIM_Phenotype': '# 618547', 'OMIM_Gene': 'No', 'Gene': 'HK1', 'ICD_10': 'H54'}, 
    {'disease_name': 'NEUROFIBROMATOSIS, TYPE I; NF1', 'OMIM_Phenotype': '# 162200', 'OMIM_Gene': 'No', 'Gene': 'NF1', 'ICD_10': 'Q85.01'},    
    {'disease_name': 'Noonan syndrome 4, MIRAGE syndrome', 'OMIM_Phenotype': '# 610733, # 617053', 'OMIM_Gene': 'No', 'Gene': 'SOS1,SAMD9', 'ICD_10': 'Q87.1, Q87.8'},
    {'disease_name': 'Noonan syndrome type 3', 'OMIM_Phenotype': '# 609942', 'OMIM_Gene': 'No', 'Gene': 'KRAS', 'ICD_10': 'Q87.1'},
    {'disease_name': 'Noonan syndrome type I', 'OMIM_Phenotype': '# 163950', 'OMIM_Gene': 'No', 'Gene': 'PTPN11', 'ICD_10': 'Q87.1'},
    {'disease_name': 'Noonan syndrome-like disorder with or without juvenile myelomonocytic leukaemia', 'OMIM_Phenotype': '# 613563', 'OMIM_Gene': 'No', 'Gene': 'CBL', 'ICD_10': 'Q87.1'},
    {'disease_name': 'Phenylketonuria', 'OMIM_Phenotype': '# 261600', 'OMIM_Gene': 'No', 'Gene': 'PAH', 'ICD_10': 'E70.0'},
    {'disease_name': 'Pierpont syndrome', 'OMIM_Phenotype': '# 602342', 'OMIM_Gene': 'No', 'Gene': 'TBL1XR1', 'ICD_10': 'C94.0'},
    {'disease_name': 'Pitt-Hopkins syndrome', 'OMIM_Phenotype': '# 610954', 'OMIM_Gene': 'No', 'Gene': 'TCF4', 'ICD_10': 'Q87.0'},
    {'disease_name': 'POLG-related disorders', 'OMIM_Phenotype': '', 'OMIM_Gene': '* 174763', 'Gene': 'POLG', 'ICD_10': 'E88.8'},
    {'disease_name': 'Pyridoxine-Dependent Epilepsy–ALDH7A1', 'OMIM_Phenotype': '# 266100', 'OMIM_Gene': 'No', 'Gene': 'ALDH7A1', 'ICD_10': 'E70.0'},
    {'disease_name': 'Rett syndrome', 'OMIM_Phenotype': '# 312750', 'OMIM_Gene': 'No', 'Gene': 'MECP2', 'ICD_10': 'F84.2'},
    {'disease_name': 'Seizures, benign familial infantile; Convulsions, familial infantile, with paroxysmal choreoathetosis; Episodic kinesigenic dyskinesia', 'OMIM_Phenotype': 'No', 'OMIM_Gene': '* 614386', 'Gene': 'PRRT2', 'ICD_10': 'G40.4, G40.8, G24.1'},
    {'disease_name': 'Short stature, Hyperextensibility, Hernia, Ocular depression, Rieger anomaly, and Teething delay, SHORT syndrome', 'OMIM_Phenotype': '# 269880', 'OMIM_Gene': 'No', 'Gene': 'PIK3R1', 'ICD_10': 'R62.0, K40.9, Q87.2, Q87.8'},
    {'disease_name': 'Spastic paraplegia 47,  Epilepsy, familial temporal lobe, 5', 'OMIM_Phenotype': '# 614066, # 614417', 'OMIM_Gene': 'No', 'Gene': 'AP4B1,CPA6', 'ICD_10': 'G11.4, G40.1'},
    {'disease_name': 'SPECC1L syndrome, Craniosynostosis', 'OMIM_Phenotype': '# 614140, # 604757', 'OMIM_Gene': 'No', 'Gene': 'MSX2,SPECC1L', 'ICD_10': 'Q87.8, Q75.0'},
    {'disease_name': 'Subcortical laminar heterotopia, Lissencephaly', 'OMIM_Phenotype': '# 607432', 'OMIM_Gene': 'No', 'Gene': 'PAFAH1B1', 'ICD_10': 'Q04.0'},
    {'disease_name': 'Tuberous sclerosis-2', 'OMIM_Phenotype': '# 613254', 'OMIM_Gene': 'No', 'Gene': 'TSC2', 'ICD_10': 'Q85.1'},
    {'disease_name': 'X-linked lissencephaly i X-linked subcortical laminal heterotopia', 'OMIM_Phenotype': '# 300067', 'OMIM_Gene': 'No', 'Gene': 'DCX', 'ICD_10': 'Q04.4'},
    {'disease_name': 'ZMYND11-related syndromic intellectual disability', 'OMIM_Phenotype': '# 616083', 'OMIM_Gene': 'No', 'Gene': 'ZMYND11', 'ICD_10': 'F70'},
]

# Variant information (transcript, HGVS coding name, HGVS protein name)
TRANSCRIPTS = ["NM_000314","NM_001126112","NM_000546","NM_001354609","NM_000492","NM_004985","NM_000222","NM_000051","NM_007294","NM_000059"]
CODING_VARIANTS = ["c.76A>T","c.35delG","c.1582G>A","c.1082C>T","c.230_231insA","c.418+1G>A","c.678_680del","c.403C>G","c.500_501dupTA","c.743_744delCT"]
PROTEIN_VARIANTS = ["p.(Gly12Asp)", "p.G12D","p.(Phe508del)", "p.F508del","p.(Arg117His)", "p.R117H","p.(Trp282Ter)", "p.W282*","p.(Val600Glu)", "p.V600E","p.(Lys76_Asn78del)", "p.E6Vfs*5","p.(Arg132Cys)", "p.R132C","p.(Ile157Thr)", "p.I157T"]

# Relationship between genes and chromosomes
GENE_TO_CHROMOSOME = {
    "RNASEH2B": "9", "GFAP": "17", "SLC16A2": "X", "ACAT1": "11", "COL4A1": "13", "TRPC3": "4", "ATM": "11", "BBS7": "4", "PHF6": "X",
    "MYH7": "14", "PPT1": "1", "MFSD8": "4", "SCN8A": "12", "EARS2": "16", "ATP2A2": "12", "ATP6V0A1": "17", "CHRNA4": "20", "GRIK2": "6",
    "SCN2A": "2", "TOR1A": "9", "CDKL5": "X", "WWOX": "16", "ALG13": "X", "SMARCA2": "9", "SPTAN1": "9", "SCN1A": "2", "MECP2": "X",
    "KCNQ2": "20", "ETHE1": "19", "AMT": "3", "SCN4A": "17", "CBS": "21", "PIGV": "1", "ABCC9": "12", "UNC80": "2", "MED13L": "12", "SYNGAP1": "6",
    "CHD8": "14", "MED13": "17", "MEIS2": "15", "NDST1": "5", "IQSEC2": "X", "GDI1": "X", "UPF3B": "X", "RPL10": "X", "RYR1": "19", "HUWE1": "X", 
    "KDM6A": "X", "L2HGDH": "14", "CEP290": "12", "HPRT1": "X", "OCRL": "X", "DRD4": "11", "MMAA": "4", "KIF11": "10", "TWNK": "10",
    "SLC6A1": "3", "NEB": "2", "TRPV4": "12", "FOLR1": "11", "DLL1": "6", "GRIN1": "9", "HK1": "10", "NF1": "17",
    "SOS1": "2", "SAMD9": "7", "KRAS": "12", "PTPN11": "12", "CBL": "11", "PAH": "12", "TBL1XR1": "3", "TCF4": "18", "POLG": "15",
    "ALDH7A1": "5", "PRRT2": "16", "PIK3R1": "5", "AP4B1": "1", "CPA6": "8", "MSX2": "5", "SPECC1L": "22", "PAFAH1B1": "17", "TSC2": "16",
    "DCX": "X", "ZMYND11": "10", "TP53": "17", "BRCA1": "17", "EGFR": "7", "MYC": "8", "CDKN2A": "9", "APOE": "19", "PTEN": "10", "CFTR": "7", "TNF": "6"
}

# Common inheritance patter of each gene
GENE_TO_INHERITANCE = {
    "RNASEH2B": "AR", "GFAP": "AD", "SLC16A2": "XL", "ACAT1": "AR", "COL4A1": "AD", "TRPC3": "AR", "ATM": "AR", "BBS7": "AR",
    "PHF6": "XL", "MYH7": "AD", "PPT1": "AR", "MFSD8": "AR", "SCN8A": "AD", "EARS2": "AR", "ATP2A2": "AD", "ATP6V0A1": "AR",
    "CHRNA4": "AD", "GRIK2": "AR", "SCN2A": "AD", "TOR1A": "AD", "CDKL5": "XL", "WWOX": "AR", "ALG13": "XL", "SMARCA2": "AD",
    "SPTAN1": "AD", "SCN1A": "AD", "MECP2": "XL", "KCNQ2": "AD", "ETHE1": "AR", "AMT": "AR", "SCN4A": "AD", "CBS": "AR",
    "PIGV": "AR", "ABCC9": "AD", "UNC80": "AR", "MED13L": "AD", "SYNGAP1": "AD", "CHD8": "AD", "MED13": "AD", "MEIS2": "AD",
    "NDST1": "AR", "IQSEC2": "XL", "GDI1": "XL", "UPF3B": "XL", "RPL10": "XL", "RYR1": "AD", "HUWE1": "XL", "KDM6A": "XL",
    "L2HGDH": "AR", "CEP290": "AR", "HPRT1": "XL", "OCRL": "XL", "DRD4": "AD", "MMAA": "AR", "KIF11": "AD", "TWNK": "AR",
    "SLC6A1": "AD", "NEB": "AR", "TRPV4": "AD", "FOLR1": "AR", "DLL1": "AD", "GRIN1": "AD", "HK1": "AR", "NF1": "AD", "SOS1": "AD",
    "SAMD9": "AD", "KRAS": "No", "PTPN11": "AD", "CBL": "AD", "PAH": "AR", "TBL1XR1": "AD", "TCF4": "AD", "POLG": "AR",
    "ALDH7A1": "AR", "PRRT2": "AD", "PIK3R1": "AD", "AP4B1": "AR", "CPA6": "AD", "MSX2": "AD", "SPECC1L": "AD", "PAFAH1B1": "AD",
    "TSC2": "AD", "DCX": "XL", "ZMYND11": "AD", "TP53": "AD", "BRCA1": "AD", "EGFR": "No", "MYC": "No", "CDKN2A": "AD", "APOE": "AD",
    "PTEN": "AD", "CFTR": "AR", "TNF": "AD"
}

# List of genes not related to ID and metabolic disease
SECONDARY_GENES = ["TP53", "BRCA1", "EGFR", "KRAS", "MYC", "CDKN2A", "APOE", "PTEN", "CFTR", "TNF"]

SYMPTOMS = [
    "Intellectual disability HP:0001249", "Developmental delay HP:0001263", "Language delay HP:0002463",
    "Autistic behavior HP:0000729", "Global developmental delay HP:0001263", "Attention deficit hyperactivity disorder HP:0007018",
    "Learning disability HP:0001328", "Motor delay HP:0001270", "Speech delay HP:0000750", "Hypotonia HP:0001252",
    "Seizures HP:0001250", "Behavioral abnormality HP:0000708", "Social impairment HP:0100716", "Stereotypic behavior HP:0000733",
    "Aggressive behavior HP:0000718", "Cognitive impairment HP:0100543", "Anxiety HP:0000739", "Hyperactivity HP:0000752",
    "Obsessive-compulsive behavior HP:0000722", "Sleep disturbance HP:0002360", "Self-injurious behavior HP:0100716",
    "Delayed speech and language development HP:0000750", "Gait disturbance HP:0001288", "Poor coordination HP:0002370",
    "Abnormal facial shape HP:0001999", "Short stature HP:0004322", "Microcephaly HP:0000252", "Macrocephaly HP:0000256",
    "Failure to thrive HP:0001508", "Hypersensitivity to stimuli HP:0000739", "Emotional lability HP:0000712",
    "Irritability HP:0000737", "Poor eye contact HP:0000817", "Echolalia HP:0012430", "Toe walking HP:0002165",
    "Dyslexia HP:0001339", "Epileptic encephalopathy HP:0200134", "Hyposmia HP:0004409", "Hypomimia HP:0000338",
    "Motor stereotypies HP:0100711", "Tics HP:0100753", "Hand flapping HP:0010514", "Delayed motor milestones HP:0001270",
    "Dysarthria HP:0001260", "Hypersomnolence HP:0200133", "Apraxia HP:0007015", "Dysgraphia HP:0011444", "Bruxism HP:0000347", "Pica HP:0000734"
]

# Main Serbian hospitals
HOSPITALS = [
    "Klinicki Centar Srbije", "Vojnomedicinska Akademija, VMA", "Klinicki Centar Vojvodine", "Klinicki Centar Nis", 
    "(Institut za Onkologiju i Radiologiju Srbije", "Institut za Kardiovaskularne Bolesti Dedinje", "Univerzitetska Decja Klinika Tirsova",
    "Institut za Mentalno Zdravlje", "Klinicki Bolnicki Centar Zemun"
    ]

# Allowed Values
GENDER = ['M', 'F', 'Other']
BINARY_ANSWER = ['Yes', 'No']
TERNARY_ANSWER_UNKNOWN = ['Yes', 'No', 'Unknown']
TERNARY_ANSWER_NA = ["Yes", "No", "NA"]
QUATERNARY_ANSWER = ["Yes","No","NA","Available in collaboration with clinical center"]
CONSANGUINEOUS_RATE = [0.10, 0.90]
CONSANGUINEOUS_RELATION = ['First', 'Second']
RELATIVES = ['Bother', 'Sister', 'Mother', 'Father', 'Grandmother', 'Grandfather', 'Other']
IN_VITRO_RATE = [0.20, 0.70, 0.10]
GENETIC_TEST = ['Positive', 'Negative', 'Inconclusive']
GENETIC_TEST_RATE = [0.8, 0.1, 0.1]
GENETIC_APPROACH = ['CES', 'WES', 'WGS']
NGS_PLATFORM = ['MiSeq', 'NextSeq 550', 'NextSeq 2000', 'DNBSEQ G-400']
REF_GENOME = ['hg19', 'hg38']
OTHER_IQ_TEST = ["Wechsler Intelligence Scale for Children (WISC)", "Wechsler Adult Intelligence Scale (WAIS)", "Stanford-Binet Intelligence Scales", "Adaptive Behavior Assessment System (ABAS)", "Vineland Adaptive Behavior Scales", "Battelle Developmental Inventory (BDI)", "Reynolds Intellectual Assessment Scales (RIAS)", "Bayley Scales of Infant and Toddler Development (Bayley-III)"]
OTHER_IQ_RESULT = ["Average","Below average","Over average"]
ZYGOSITY = ["Hemizygous", "Het", "Hom"]
SEGREGATION = ["NA", "De novo", "Maternally inherited", "Paternally inherited"]

# Generate date within a range
def generate_date(start_date, end_date):
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return (start_date + timedelta(days=random_days))

# Generate age in months and years within a range
def generate_age(start_date, end_date):
    test_date = generate_date(start_date, end_date)
    age = test_date.year - start_date.year
    if age == 0:
        month = random.randint(1,12)
        return str(month) + 'm'
    else:
        return str(age) + 'y'

# Generate patient data
def generate_patient_data():
    base_date = datetime.now() - timedelta(days=random.randint(0, 365*5))  # base date within the last 5 years
    IMGGI_ID = fake.bothify(text='RFZO ###')
    birth_date = generate_date(base_date - timedelta(days=365*5), base_date).date()
    birth_country = "Serbia"
    gender = random.choice(GENDER)
    testing_age =  generate_age(birth_date, datetime.now().date())
    onset_age =  random.choice(["No", generate_age(birth_date, datetime.now().date())])
    centre_of_referral = random.choice(HOSPITALS)
    symptoms = generate_symptoms()
    HPO_identifiers = get_HPO_identifiers(symptoms)
    
    return {
        "IMGGI_ID": IMGGI_ID,
        "birth_date": birth_date, 
        "country_of_birth": birth_country,
        "gender": gender,
        "testing_age": testing_age,
        "onset_age": onset_age,
        "centre_of_referral": centre_of_referral,
        "symptoms": symptoms,
        "HPO_identifiers": HPO_identifiers
    }

# Generate clinical data
def generate_clinical_data():
    
    data = {
        "consanguineous": np.random.choice(BINARY_ANSWER, p=CONSANGUINEOUS_RATE),
        "consanguineous_relation": "No",
        "affected_relatives": random.choice(TERNARY_ANSWER_UNKNOWN),
        "relatives_relation": "No",
        "spontaneous_abortions": random.choice(TERNARY_ANSWER_UNKNOWN),
        "number_abortions": "No",
        "in_vitro": np.random.choice(TERNARY_ANSWER_UNKNOWN, p=IN_VITRO_RATE),
        "ID": random.choice(BINARY_ANSWER),
        "IQ": "No",
        "IQ_descriptive": "No",
        "other_IQ_test": "No",
        "other_IQ_result": "No",
        "metabolic_disease": random.choice(TERNARY_ANSWER_UNKNOWN),
        "CT": random.choice(QUATERNARY_ANSWER),
        "MR": random.choice(QUATERNARY_ANSWER),
        "EEG": random.choice(QUATERNARY_ANSWER)
    }
    
    if data["consanguineous"] == "Yes":
        data["consanguineous_relation"] = random.choice(CONSANGUINEOUS_RELATION)
    
    if data["affected_relatives"] == "Yes":
        data["relatives_relation"] = random.choice(RELATIVES)    
    
    if data["spontaneous_abortions"] == "Yes":
        data["number_abortions"] = random.randint(1, 5)
    
    if data["ID"] == "Yes":
        data["IQ"] = random.randint(10, 70)
        data["IQ_descriptive"] = random.choice(["No", generate_IQ_descriptive(data["IQ"])])
        data["other_IQ_test"] = random.choice(["No", random.choice(OTHER_IQ_TEST)])
        if data["other_IQ_test"] != "No":
            data["other_IQ_result"] = random.choice(OTHER_IQ_RESULT)
    
    return data

# Generate genetic test data
def generate_genetic_test_data(gender, disease):
    
    data = {
        "previous_karyotype": random.choice(BINARY_ANSWER),
        "karyotype_result": "No",
        "previous_microarray": "No",
        "microarray_result": "No",
        "other_genetic_analysis": generate_has_other_test(disease),
        "other_analysis_type": "No",
        "other_test_result": "No",
        "genetic_test_result": np.random.choice(GENETIC_TEST, p=GENETIC_TEST_RATE),
        "genetic_test_approach": random.choice(GENETIC_APPROACH),
        "NGS_platform": random.choice(NGS_PLATFORM),
        "ref_genome": random.choice(REF_GENOME),
        "VCF_location": fake.bothify(text='TSO_NextSeq_#####')
    }
    
    if data["previous_karyotype"] == "Yes":
        data["karyotype_result"] = generate_karyotype_result(gender, disease)
        
    if disease["disease_name"] in ["NEUROFIBROMATOSIS, TYPE I; NF1", "Subcortical laminar heterotopia, Lissencephaly", "X-linked lissencephaly i X-linked subcortical laminal heterotopia", "Tuberous sclerosis-2"]:
        data["previous_microarray"] = "Yes"
    else:
        data["karyotype_result"] = random.choice(TERNARY_ANSWER_NA)
    
    if data["previous_microarray"] == "Yes":
        data["microarray_result"] = generate_microarray_result(gender, disease)
    
    if data["other_genetic_analysis"] == "Yes":
        data["other_analysis_type"] = generate_other_test_type(disease)
        data["other_test_result"] = generate_other_test_result(data["other_analysis_type"], disease)

    
    return data

# Generate variant data
def generate_variant(disease, type):
        data = {
            "chromosome": "No",
            "gene": "No",
            "chromosome": "No",
            "inheritance": "No",
            "variants": "No",
            "zygosity": random.choice(ZYGOSITY),
            "transcript": random.choice(TRANSCRIPTS),
            "coding_variant_1": random.choice(CODING_VARIANTS),
            "protein_variant_1": random.choice(["No", random.choice(PROTEIN_VARIANTS)]),
            "coding_variant_2": "No",
            "protein_variant_2": "No",
            "segregation_1": random.choice(SEGREGATION),
            "segregation_2": "No"
        }
        
        if type == "primary":
            data["gene"] = random.choice(disease["Gene"].split(","))
        else:
            data["gene"] = random.choice(SECONDARY_GENES)
        
        data["chromosome"] = GENE_TO_CHROMOSOME[data["gene"]]
        
        data["inheritance"] = GENE_TO_INHERITANCE[data["gene"]]
        
        has_second_variant = random.choice([0,1])
        if has_second_variant:
            data["coding_variant_2"] = random.choice(CODING_VARIANTS)
            data["protein_variant_2"] = random.choice(["No", random.choice(PROTEIN_VARIANTS)])
            data["segregation_2"] = random.choice(SEGREGATION)           
            data["variants"] = "2"
        else:
            data["variants"] = "1"
        
        return data

# Describe IQ according to the IQ value
def generate_IQ_descriptive(IQ_result):
    if IQ_result < 20:
        return "Profound Intellectual Disability"
    elif IQ_result >= 20 and IQ_result < 34:
        return "Severe Intellectual Disability"
    elif IQ_result >= 35 and IQ_result < 49:
        return "Moderate Intellectual Disability"
    elif IQ_result >= 50 and IQ_result < 69:
        return "Mild Intellectual Disability"
    else:
        return "No"

# Generate a aleatory number of symptoms related to ID or NDD
def generate_symptoms():
    num_symptoms = random.randint(1,5)
    result = []
    for i in range(num_symptoms):
        result.append(random.choice(SYMPTOMS))
    
    return ','.join([str(symptom) for symptom in result])

# Return the list of HPO identifiers from the list of symptoms
def get_HPO_identifiers(symptoms):
    symptoms_list = symptoms.split(",")
    HPO_list = []
    for symptom in symptoms_list:
        #HPO_ID = (symptom.split(" "))[1]
        HPO_ID = re.search('HP:[0-9]+$', symptom)
        HPO_list.append(HPO_ID.group(0))
    
    return ','.join([str(id) for id in HPO_list])

# Generate karyotypes considering common alterations according to the disease and the gender
def generate_karyotype_result(gender, disease):
        match disease["disease_name"]:
            case "NEUROFIBROMATOSIS, TYPE I; NF1": 
                if gender == "M":
                    return random.choice(["46, XY", "46, XY, del(17)(q11.2q11.2)"])
                else:
                    return random.choice(["46, XX", "46, XX, del(17)(q11.2q11.2)"])
            case "Subcortical laminar heterotopia, Lissencephaly":
                if gender == "M":
                    return "46 XY"
                else:
                    return random.choice(["46, XX", "46, XX, del(Xq22.3)", "46, XX, dup(Xq22.3)"])
            case "X-linked lissencephaly i X-linked subcortical laminal heterotopia":
                if gender == "M":
                    return "46 XY"
                else:
                    return random.choice(["46, XX", "46, XX, del(Xq22.3)", "46, XX, dup(Xq22.3)"])
            case "Tuberous sclerosis-2":
                if gender == "M":
                    return random.choice(["46, XY", "46, XY, del(16)(p13.3)"])
                else:
                    return random.choice(["46, XX", "46, XX,del(16)(p13.3)"])
            case _: 
                if gender == "M":
                    return "46, XY"
                else:
                    return "46, XX"

# Generate microarray results considering common alterations according to the disease and the gender
def generate_microarray_result(gender, disease):
        match disease["disease_name"]:
            case "NEUROFIBROMATOSIS, TYPE I; NF1": 
                return "deletion 17q11.2"
            case "Subcortical laminar heterotopia, Lissencephaly":
                if gender == "M":
                    return "No"
                else:
                    return random.choice(["deletion Xq22.3", "duplication Xq22.3"])
            case "X-linked lissencephaly i X-linked subcortical laminal heterotopia":
                if gender == "M":
                    return "No"
                else:
                    return random.choice(["deletion Xq22.3", "duplication Xq22.3"])
            case "Tuberous sclerosis-2":
                return "deletion 16p13.3"
            case _: 
                return "No"

# Define if there is other genetic test based on the disease
def generate_has_other_test(disease):
    if disease["disease_name"] in ["NEUROFIBROMATOSIS, TYPE I; NF1","X-linked lissencephaly i X-linked subcortical laminal heterotopia","Bardet-Biedl syndrome 7",
                                    "Ceroid lipofuscinosis, neuronal, 1","Rett syndrome","Pitt-Hopkins syndrome","Ataxia-telangiectasia",
                                    "Microcephaly with or without chorioretinopathy, lymphedema, or mental retardation, MCLID", "Intellectual developmental disorder, X-linked",
                                    "Intellectual developmental disorder, X-linked 41", "Intellectual developmental disorder, X-linked syndromic 14",
                                    "Intellectual developmental disorder, X-linked syndromic 35", "Intellectual developmental disorder, X-linked syndromic 35; King-Denborough and Congenital myopat<hy 1A, autosomal dominant, with susceptibility to malignant hyperthermia",
                                    "Intellectual developmental disorder, X-linked syndromic, Turner type", "Kabuki syndrome-2",
                                    "Borjeson-Forssman-Lehmann syndrome"]:
        return "Yes"
    
    return "No"

# Define other genetic test type based on the disease
def generate_other_test_type(disease):
        match disease["disease_name"]:
            case "NEUROFIBROMATOSIS, TYPE I; NF1": 
                return "MLPA"
            case "X-linked lissencephaly i X-linked subcortical laminal heterotopia": 
                return "FraX"
            case "Bardet-Biedl syndrome 7": 
                return "MLPA"
            case "Ceroid lipofuscinosis, neuronal, 1": 
                return "MLPA"
            case "Rett syndrome": 
                return "DNA methylation test"
            case "Pitt-Hopkins syndrome": 
                return "MLPA"
            case "Microcephaly with or without chorioretinopathy, lymphedema, or mental retardation, MCLID": 
                return "MLPA"
            case "Ataxia-telangiectasia": 
                return "Other"
            case "Intellectual developmental disorder, X-linked":
                return "FraX"
            case "Intellectual developmental disorder, X-linked 41":
                return "FraX"
            case "Intellectual developmental disorder, X-linked syndromic 14":
                return "FraX"
            case "Intellectual developmental disorder, X-linked syndromic 35":
                return "FraX"
            case "Intellectual developmental disorder, X-linked syndromic 35; King-Denborough and Congenital myopathy 1A, autosomal dominant, with susceptibility to malignant hyperthermia":
                return "FraX"
            case "Intellectual developmental disorder, X-linked syndromic, Turner type":
                return "FraX"
            case "Kabuki syndrome-2":
                return "FraX"
            case "Borjeson-Forssman-Lehmann syndrome":
                return "FraX"

# Generate other genetic test results considering the test type and the disease
def generate_other_test_result(test_type, disease):
        match test_type:
            case "FraX": 
                return random.choice(["Normal", "Grey Zone", "Premutation", "Full Mutation"])
            case "MLPA":
                    return random.choice(["Positive", "Negative"])
            case "DNA methylation test":
                return "Hypermethylation of the gene promoter region"
            case "Other":
                return random.choice(["Detection of truncated proteins", "Absence of full-length protein"])
            case _: 
                return "Nothing found"

    
# Generate the synthetic dataset
data = []

for disease in diseases:
    for i in range(PATIENTS_PER_DISEASE):
        patient_data = generate_patient_data()
        clinical_data = generate_clinical_data()
        genetic_data = generate_genetic_test_data(patient_data["gender"], disease)
        
        row = {
            "IMGGI ID": patient_data["IMGGI_ID"], 
            "Diagnosis":disease["disease_name"], 
            "OMIM Phenotype": disease["OMIM_Phenotype"], 
            "OMIM gen/locus":disease["OMIM_Gene"], 
            "Diagnosis code in ICD 10": disease["ICD_10"], 
            "DOB": patient_data["birth_date"].strftime("%d/%m/%y"),
            "Country of Birth": patient_data["country_of_birth"], 
            "GENDER": patient_data["gender"],
            "AGE when sent for genetic testing": patient_data["testing_age"], 
            "Consanguineous offspring": clinical_data["consanguineous"], 
            "Consanguineous offspring - relation": clinical_data["consanguineous_relation"], 
            "Affected relatives": clinical_data["affected_relatives"],
            "Affected relatives - relation": clinical_data["relatives_relation"], 
            "Spontaneous abortions": clinical_data["spontaneous_abortions"], 
            "Spontaneous abortions - how many": clinical_data["number_abortions"], 
            "In vitro fertilisation": clinical_data["in_vitro"],
            "Indication for refferal: HPO descriptive": patient_data["symptoms"],
            "Indication for referral: HPO data": patient_data["HPO_identifiers"],
            "Age at Onset": patient_data["onset_age"], 
            "Intellectual disability": clinical_data["ID"],
            "IQ test - result": clinical_data["IQ"], 
            "IQ test - descriptive result": clinical_data["IQ_descriptive"], 
            "Other test for measuring intelectual disability (ID) or neurodevelopmental disorder (NDD)": clinical_data["other_IQ_test"],
            "Other test for measuring intelectual disability (ID) or neurodevelopmental disorder (NDD) - Result": clinical_data["other_IQ_result"], 
            "Metabolic disease": clinical_data["metabolic_disease"], 
            "Measurements for metabolic disease": "No", # QUÉ MEDIDAS???
            "CT": clinical_data["CT"], 
            "MR": clinical_data["MR"],
            "EEG": clinical_data["EEG"], 
            "Clinical centre of referral": patient_data["centre_of_referral"], 
            "Previous genetic testing - karyotype": genetic_data["previous_karyotype"],
            "Previous genetic testing - karyotype - result": genetic_data["karyotype_result"], 
            "Previous genetic testing - microarray": genetic_data["previous_microarray"], 
            "Previous genetic testing - microarray - result": genetic_data["microarray_result"],
            "Previous genetic testing - other genetic analysis- Additional": genetic_data["other_genetic_analysis"], 
            "Previous genetic testing - other genetic analysis - Additional - which analysis": genetic_data["other_analysis_type"],
            "Previous genetic testing - other genetic analysis - Additional - Result": genetic_data["other_test_result"], 
            "Genetic test result": genetic_data["genetic_test_result"], 
            "Genetic test approach": genetic_data["genetic_test_approach"], 
            "NGS platform": genetic_data["NGS_platform"],
            "Ref genome": genetic_data["ref_genome"],            
            "Gene 1": "No", 
            "Chromosome 1": "No",
            "Variant(s) 1": "No", 
            "Inheritance 1": "No", 
            "Zygosity 1": "No", 
            "Transcript 1": "No", 
            "Variant name at cDNA level 1.1": "No",
            "Variant name at protein level 1.1": "No",
            "Segregation analysis result 1.1": "No",
            "Variant name at cDNA level 1.2 (if exists)": "No", 
            "Variant name at protein level 1.2 (if exists)": "No",
            "Segregation analysis result 1.2": "No",
            "Secondary findings": "No",
            "Chromosome 2": "No", 
            "Gene 2": "No", 
            "Variant(s) 2": "No", 
            "Inheritance 2": "No",
            "Zygosity 2": "No", 
            "Transcript 2": "No", 
            "Variant name at cDNA level 2.1": "No", 
            "Variant name at protein level 2.1": "No",
            "Segregation analysis result 2.1": "No",
            "Variant name at cDNA level 2.2 (if exists)": "No",
            "Variant name at protein level 2.2 (if exists)": "No",
            "Segregation analysis result 2.2": "No",
            "Location of VCF (IMGGI server, External HD, etc)": genetic_data["VCF_location"], 
            "Comment, Other": "No"
        }
        
        if row["Genetic test result"] == "Positive":
            primary_variant = generate_variant(disease, "primary")
            
            row["Gene 1"] = primary_variant["gene"] 
            row["Chromosome 1"] = primary_variant["chromosome"]
            row["Variant(s) 1"] = primary_variant["variants"] 
            row["Inheritance 1"] = primary_variant["inheritance"] 
            row["Zygosity 1"] = primary_variant["zygosity"] 
            row["Transcript 1"] = primary_variant["transcript"] 
            row["Variant name at cDNA level 1.1"] = primary_variant["coding_variant_1"]
            row["Variant name at protein level 1.1"] = primary_variant["protein_variant_1"]
            row["Segregation analysis result 1.1"] = primary_variant["segregation_1"]
            row["Variant name at cDNA level 1.2 (if exists)"] = primary_variant["coding_variant_2"] 
            row["Variant name at protein level 1.2 (if exists)"] = primary_variant["protein_variant_2"]
            row["Segregation analysis result 1.2"] = primary_variant["segregation_2"]
            
            row["Secondary findings"] = random.choice(BINARY_ANSWER)
            if row["Secondary findings"] == "Yes":
                secondary_variant = generate_variant(disease, "secondary")
                row["Chromosome 2"] = secondary_variant["chromosome"]
                row["Gene 2"] = secondary_variant["gene"]
                row["Variant(s) 2"] = secondary_variant["variants"]
                row["Inheritance 2"] = secondary_variant["inheritance"]
                row["Zygosity 2"] = secondary_variant["zygosity"] 
                row["Transcript 2"] = secondary_variant["transcript"] 
                row["Variant name at cDNA level 2.1"] = secondary_variant["coding_variant_1"] 
                row["Variant name at protein level 2.1"] = secondary_variant["protein_variant_1"]
                row["Segregation analysis result 2.1"] = secondary_variant["segregation_1"]
                row["Variant name at cDNA level 2.2 (if exists)"] = secondary_variant["coding_variant_2"]
                row["Variant name at protein level 2.2 (if exists)"] = secondary_variant["protein_variant_2"]
                row["Segregation analysis result 2.2"] = secondary_variant["segregation_2"]
        
        data.append(row)
            

# Convert to DataFrame
columns = [
    "IMGGI ID", "Diagnosis", "OMIM Phenotype", "OMIM gen/locus", "Diagnosis code in ICD 10", "DOB", "Country of Birth", "GENDER",
    "AGE when sent for genetic testing", "Consanguineous offspring", "Consanguineous offspring - relation", "Affected relatives",
    "Affected relatives - relation", "Spontaneous abortions", "Spontaneous abortions - how many", "In vitro fertilisation",
    "Indication for refferal: HPO descriptive", "Indication for referral: HPO data", "Age at Onset", "Intellectual disability",
    "IQ test - result", "IQ test - descriptive result", "Other test for measuring intelectual disability (ID) or neurodevelopmental disorder (NDD)",
    "Other test for measuring intelectual disability (ID) or neurodevelopmental disorder (NDD) - Result", "Metabolic disease", 
    "Measurements for metabolic disease", "CT", "MR", "EEG", "Clinical centre of referral", "Previous genetic testing - karyotype",
    "Previous genetic testing - karyotype - result", "Previous genetic testing - microarray", "Previous genetic testing - microarray - result",
    "Previous genetic testing - other genetic analysis- Additional", "Previous genetic testing - other genetic analysis - Additional - which analysis",
    "Previous genetic testing - other genetic analysis - Additional - Result", "Genetic test result", "Genetic test approach", "NGS platform",
    "Ref genome", "Gene 1", "Chromosome 1", "Variant(s) 1", "Inheritance 1", "Zygosity 1", "Transcript 1", "Variant name at cDNA level 1.1",
    "Variant name at protein level 1.1","Segregation analysis result 1.1", "Variant name at cDNA level 1.2 (if exists)", "Variant name at protein level 1.2 (if exists)", 
    "Segregation analysis result 2.2", "Secondary findings", "Chromosome 2", "Gene 2", "Variant(s) 2", "Inheritance 2", "Zygosity 2", "Transcript 2", "Variant name at cDNA level 2.1",
    "Variant name at protein level 2.1", "Segregation analysis result 2.1","Variant name at cDNA level 2.2 (if exists)", "Variant name at protein level 2.2 (if exists)",
    "Segregation analysis result 2.2", "Location of VCF (IMGGI server, External HD, etc)", "Comment, Other"
]

df = pd.DataFrame(data, columns=columns)

# Save to CSV
output_path = "../../datasets/data/IMGGE/Core_Table.xlsx"
print(output_path)
df.to_excel(output_path, index=False)

