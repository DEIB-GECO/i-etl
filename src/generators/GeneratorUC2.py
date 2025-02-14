import datetime
import random

import headfake
import headfake.field
import headfake.transformer
import pandas as pd

from constants.datageneration.uc2 import FUNDUS_DESCRIPTIONS, CENTRAL_OPTICAL_COHERENCE, WIDE_FIELD_AUTOFLUORESCENCE, \
    WIDE_FIELD_COLOR, INHERITENCE, ETHNICITIES
from database.Execution import Execution
from generators.DataGenerator import DataGenerator
from generators.modules.Diagnosis import Diagnosis
from generators.modules.Patient import Patient
from utils.file_utils import get_ground_data


class GeneratorUC2(DataGenerator):
    def __init__(self, execution: Execution):
        super().__init__(execution=execution)
        self.diseases = pd.read_csv(get_ground_data("UC2_Diseases.csv"), sep=',')
        self.variants = pd.read_csv(get_ground_data("variants.csv"), sep=",")
        self.baseline_df = None

    def generate(self):
        num_rows = self.execution.nb_rows

        # Create the baseline clinical table
        fieldset = self.generate_baseline_clinical_dataset()
        hf = headfake.HeadFake.from_python({"fieldset": fieldset})
        data_1 = hf.generate(num_rows=num_rows)
        self.save_generated_file(df=data_1, filename="Baseline_Clinical_Table.xlsx")

        # Create the genetic table
        self.baseline_df = data_1
        data_2 = self.generate_genetic_dataset()
        self.save_generated_file(df=data_2, filename="Genetic_Table.xlsx")

        # Create the dynamic table
        data_3 = self.generate_dynamic_dataset(data_1, 4)
        self.save_generated_file(df=data_3, filename="Dynamic_Clinical_Table.xlsx")

        # Create the imaging data table
        data_4 = self.generate_imaging_dataset(data_1)
        self.save_generated_file(df=data_4, filename="imaging.csv")

    def generate_baseline_clinical_dataset(self):
        patient_id = Patient.generate_patient_id("Patient ID", min_value=1, length=7, prefix="UC2_")
        yob = headfake.field.OperationField(name="YOB", operator=None, first_value=datetime.datetime.now().year - 90,
                                            second_value=datetime.datetime.now().year, operator_fn=random.randint)
        sex = Patient.generate_sex(field_name="Sex", male_value="M", female_value="F")

        paternal_origin = headfake.field.OperationField(name="Porigin", operator=None, first_value=ETHNICITIES,
                                                        second_value=None, operator_fn=self.select_ethnicity)
        maternal_origin = headfake.field.OperationField(name="Morigin", operator=None, first_value=ETHNICITIES,
                                                        second_value=None, operator_fn=self.select_ethnicity)
        diagnosis = Diagnosis.generate_diagnosis_name(field_name="Dx",
                                                      disease_file_name=get_ground_data("UC2_Diseases.csv"),
                                                      key_field="DiseaseName")

        year_of_birth = headfake.field.LookupField(field="YOB", hidden=True)

        onset = headfake.field.OperationField(name="Onset", operator=None, first_value=year_of_birth, second_value=None,
                                              operator_fn=self.get_onset)
        hearing_disability = headfake.field.OptionValueField(name="HR", probabilities={"no": 0.5, "yes": 0.5})

        comorbidities = headfake.field.OperationField(name="Comorb", operator=None, first_value=None, second_value=None,
                                                      operator_fn=self.generate_comorbidities)

        fs = headfake.Fieldset(
            fields=[patient_id, yob, sex, paternal_origin, maternal_origin, diagnosis, onset, hearing_disability,
                    comorbidities])

        return fs

    def generate_genetic_dataset(self):
        patient_id = self.baseline_df["Patient ID"]
        inheritance = []
        gene = []
        mutation1 = []
        mutation2 = []

        for pid in patient_id:
            inheritance.append(random.choice(INHERITENCE))
            disease = self.baseline_df.loc[self.baseline_df['Patient ID'] == pid]['Dx'].values[0]
            gene.append(self.get_gene(disease))

            index = random.randint(0, len(self.variants) - 1)
            conding_name = self.variants["coding_name"][index]
            protein_name = self.variants["protein_name"][index]
            if isinstance(protein_name, float):
                mutation1.append(f'{conding_name}')
            else:
                mutation1.append(f'{conding_name};{protein_name.replace("p.", "p(")})')

            index = random.randint(0, len(self.variants) - 1)
            conding_name = self.variants["coding_name"][index]
            protein_name = self.variants["protein_name"][index]
            if isinstance(protein_name, float):
                mutation2.append(f'{conding_name}')
            else:
                mutation2.append(f'{conding_name};{protein_name.replace("p.", "p(")})')

        data = {'Patient ID': patient_id,
                "Inheritance pattern": inheritance,
                "Gene": gene,
                "Mutation 1": mutation1,
                "Mutation 2": mutation2}

        df = pd.DataFrame(data=data)

        return df

    def generate_dynamic_dataset(self, baseline_df, num_evolution_records):
        id_list = []
        age = []
        reva = []
        leva = []
        reref = []
        leref = []
        rodre = []
        rodle = []
        mixrea = []
        mixlea = []
        mixreb = []
        mixleb = []
        conereamp = []
        coneleamp = []
        conereimp = []
        coneleimp = []
        pvfre = []
        pvfle = []
        cvfre = []
        cvfle = []
        rcfr_iii = []
        rcfl_iii = []
        rcfr_v = []
        rcfl_v = []

        for pid in baseline_df["Patient ID"]:
            age_onset = baseline_df.loc[baseline_df['Patient ID'] == pid]['Onset'].values[0]
            for i in range(num_evolution_records):
                id_list.append(pid)

                age_onset = age_onset + 1
                age.append(age_onset)

                visual_acuity = random.randint(a=0, b=100)
                reva.append(visual_acuity / 100)

                visual_acuity = random.randint(a=0, b=100)
                leva.append(visual_acuity / 100)

                myop_hiper = random.choice(["-", "+"])
                diop = random.randint(1, 8)
                med = random.choice([1, 0])
                if med:
                    reref.append(f'{myop_hiper}{diop}.{5}')
                else:
                    reref.append(f'{myop_hiper}{diop}')

                myop_hiper = random.choice(["-", "+"])
                diop = random.randint(1, 8)
                med = random.choice([1, 0])

                if med:
                    leref.append(f'{myop_hiper}{diop}.{5}')
                else:
                    leref.append(f'{myop_hiper}{diop}')

                rodre.append(random.choice([0, "NA", random.randint(a=100, b=300)]))
                rodle.append(random.choice([0, "NA", random.randint(a=100, b=300)]))

                mixrea.append(random.choice([0, "NA", random.randint(a=150, b=250)]))
                mixlea.append(random.choice([0, "NA", random.randint(a=150, b=250)]))

                mixreb.append(random.choice([0, "NA", random.randint(a=300, b=600)]))
                mixleb.append(random.choice([0, "NA", random.randint(a=300, b=600)]))

                conereamp.append(random.choice([0, "NA", random.randint(a=20, b=60)]))
                coneleamp.append(random.choice([0, "NA", random.randint(a=20, b=60)]))

                conereimp.append(random.choice([0, "NA", random.randint(a=25, b=35)]))
                coneleimp.append(random.choice([0, "NA", random.randint(a=25, b=35)]))

                pvfre.append(random.choice(["yes", "no"]))
                pvfle.append(random.choice(["yes", "no"]))
                cvfre.append(random.choice(["yes", "no"]))
                cvfle.append(random.choice(["yes", "no"]))

                rcfr_iii.append(random.randint(a=25, b=35))
                rcfl_iii.append(random.randint(a=25, b=35))
                rcfr_v.append(random.randint(a=20, b=30))
                rcfl_v.append(random.randint(a=20, b=30))

        data = {"Patient ID": id_list, "Age": age, "REVA": reva, "LEVA": leva, "REref": reref, "LEref": leref,
                "RodRE": rodre, "RodLE": rodle, "MixREA": mixrea, "MixLEA": mixlea, "MixREB": mixreb, "MixLEB": mixleb,
                "ConeREamp": conereamp, "ConeLEamp": coneleamp, "ConeREimp": conereimp, "ConeLEimp": coneleimp,
                "PVFRE": pvfre, "PVFLE": pvfle, "CVFRE": cvfre, "CVFLE": cvfle, "RCFR_III": rcfr_iii,
                "RCFL_III": rcfl_iii,
                "RCFR_V": rcfr_v, "RCFL_V": rcfl_v}

        df = pd.DataFrame(data=data)

        return df

    def generate_imaging_dataset(self, baseline_df):
        id_list = baseline_df["Patient ID"]
        fundus = []
        wfre = []
        wfle = []
        wffafre = []
        wffafle = []
        cmere = []
        cmele = []
        octre = []
        octle = []
        bc = []

        for _ in id_list:
            fundus.append(random.choice(FUNDUS_DESCRIPTIONS))
            wfre.append(random.choice(WIDE_FIELD_COLOR))
            wfle.append(random.choice(WIDE_FIELD_COLOR))
            wffafre.append(random.choice(WIDE_FIELD_AUTOFLUORESCENCE))
            wffafle.append(random.choice(WIDE_FIELD_AUTOFLUORESCENCE))
            cmere.append(random.choice(["yes", "no"]))
            cmele.append(random.choice(["yes", "no"]))
            octre.append(random.choice(CENTRAL_OPTICAL_COHERENCE))
            octle.append(random.choice(CENTRAL_OPTICAL_COHERENCE))
            bc.append(random.choice(["yes", "no"]))

        data = {"Patient ID": id_list, "Fundus": fundus, "WFRE": wfre, "WFLE": wfle,
                "WFFAFRE": wffafre, "WFFAFLE": wffafle, "CMERE": cmere, "CMELE": cmele,
                "OCTRE": octre, "OCTLE": octle, "BC": bc}

        df = pd.DataFrame(data=data)

        return df

    def get_gene(self, disease):
        x1 = self.diseases.loc[self.diseases['DiseaseName'] == disease]
        genes = None
        for i, row in x1.iterrows():
            genes = row["AffectedGenes"]
            break
        gene_list = genes.split(',')

        return random.choice(gene_list)

    def select_ethnicity(self, ethnicities, *arguments):
        return random.choice(ethnicities)

    def get_onset(self, yob, *arguments):
        return int((datetime.datetime.now().year - yob) / 2)

    def generate_comorbidities(self, *arguments):
        comorbidities = ["Hypertension", "Diabetes Mellitus", "Obesity", "Hyperlipidemia",
                         "Chronic Obstructive Pulmonary Disease", "Coronary Artery Disease",
                         "Chronic Kidney Disease", "Arthritis", "Asthma"]
        result = []
        for comorb in comorbidities:
            has_comorb = random.choice(["yes", "no"])
            result.append(f'{comorb} {has_comorb}')

        return ",".join(result)
