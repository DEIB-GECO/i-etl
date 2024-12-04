import datetime
import random
import re

import headfake
import headfake.field
import headfake.transformer
import pandas as pd
import scipy

from constants.datageneration.hsjd import ZIGOSITY2VAR, INHERITANCE, CONSANGUINITY, AFFECTED_RELATIVES, \
    WALK_INDEPENDENTLY, SPEAK_INDEPENDENTLY, DEVELOPMENTAL_DISORDER, INTELECTUAL_DISABILITY, AUTISM_SPECTRUM_DISORDER, \
    ATENTION_DEFICIT_HIPERACTIVITY, LANGUAGE_ABSENCE, PHOTO_AVAILABLE, WESTERN_BLOT, QPCR, FLUORESCENCE_INTENSITY, \
    PROTEIN_INTERACTION, SUBCELLULAR_LOCATION, CELLULAR_MORPHOLOGY, SPLICING, METADOME, MISSENSE_3D, DYNAMUT, ACMG, \
    ZYGOSITY
from database.Execution import Execution
from generators.DataGenerator import DataGenerator
from generators.modules.BiologicalAnalysis import BiologicalAnalysis
from generators.modules.Diagnosis import Diagnosis
from generators.modules.Gene import Gene
from generators.modules.GeneticTest import NGSTest
from generators.modules.MedicalHistory import MedicalHistory, IDTest
from generators.modules.Patient import Patient, BirthData
from generators.modules.Variant import Variant
from generators.modules.Variant import VariantInheritance
from utils.file_utils import get_ground_data


class GeneratorHsjd(DataGenerator):
    def __init__(self, execution: Execution):
        super().__init__(execution=execution)
        self.variants = pd.read_csv(get_ground_data("variants.csv"), sep=",")

    def generate(self):
        num_rows = self.execution.nb_rows

        # Create the baseline clinical dataset
        fieldset = self.generate_baseline_clinical_dataset()
        hf = headfake.HeadFake.from_python({"fieldset": fieldset})
        data_1 = hf.generate(num_rows=num_rows)
        self.save_generated_file(df=data_1, filename="Baseline_Clinical_Table.xlsx")

        # Create the biological dataset
        data_2 = self.generate_biological_dataset(data_1)
        self.save_generated_file(df=data_2, filename="Biological_Table.xlsx")

        # Create the dynamic clinical dataset
        data_3 = self.generate_dynamic_clinical_dataset(data_1, 4)
        self.save_generated_file(df=data_3, filename="Dynamic_Clinical_Table.xlsx")

        # Create the genomic dataset
        data_4 = self.generate_genomic_dataset(data_1)
        self.save_generated_file(df=data_4, filename="Genomic_Table.xlsx")

    def generate_baseline_clinical_dataset(self):
        patient_id = Patient.generate_patient_id(field_name="Patient ID", min_value=1, length=8, prefix="SJD_")
        zigosity2var = Variant.generate_zygosity(field_name="Zigosity2var", options=ZIGOSITY2VAR)
        inheritance = VariantInheritance.generate_inheritance(field_name="Inheritance", options=INHERITANCE)
        dx = Diagnosis.generate_diagnosis_name(field_name="Dx", disease_file_name=get_ground_data("SJD_Diseases.csv"),
                                               key_field="Disease")
        orpha_id = headfake.field.LookupMapFileField(lookup_value_field="Orphanet", map_file_field="Dx", hidden=True)
        hpo = Diagnosis.get_symptoms(field_name="HPO", omim_id=None, orpha_id=orpha_id, symptoms_max_number=5)
        dob = BirthData.generate_date_of_birth(field_name="DOB", distribution=scipy.stats.uniform, min_year=0,
                                               max_year=16, date_format="%d/%m/%Y")
        sex = Patient.generate_sex(field_name="Sex", male_value="male", female_value="female")
        consanguinity = MedicalHistory.generate_consanguinity(field_name="Consanguinity", options=CONSANGUINITY)
        relatives = MedicalHistory.generate_affected_relatives(field_name="Relatives", options=AFFECTED_RELATIVES)
        walk_indep = MedicalHistory.generate_walk_independently(field_name="Walkindep", options=WALK_INDEPENDENTLY)
        walk = headfake.field.LookupField(field="Walkindep", hidden=True)
        walk_age = MedicalHistory.generate_walk_age("Walkage", walk_indep=walk)
        speak_indep = MedicalHistory.generate_speak_independently(field_name="Speak", options=SPEAK_INDEPENDENTLY)
        speak = headfake.field.LookupField(field="Speak", hidden=True)
        speak_age = MedicalHistory.generate_speak_age(field_name="Speakage", speak_indep=speak)
        dd = MedicalHistory.generate_developmental_disorder(field_name="DD", options=DEVELOPMENTAL_DISORDER)
        id = MedicalHistory.generate_speak_independently(field_name="ID", options=INTELECTUAL_DISABILITY)
        has_id = headfake.field.LookupField(field="ID", hidden=True)
        id_level = IDTest.generate_ID_test_to_string(field_name="IDlevel", hasID=has_id)
        asd = MedicalHistory.generate_speak_independently(field_name="ASD", options=AUTISM_SPECTRUM_DISORDER)
        adhd = MedicalHistory.generate_speak_independently(field_name="ADHD", options=ATENTION_DEFICIT_HIPERACTIVITY)
        language_abs = MedicalHistory.generate_language_absence(field_name="languageabs", options=LANGUAGE_ABSENCE)
        ga = headfake.field.OperationField(operator=None, first_value=27, second_value=40,
                                           operator_fn=BirthData.generate_gestational_age, hidden=True)
        gestational_age = BirthData.gestational_age_to_string(field_name="Gestationalage", ga=ga)
        weight_birth = BirthData.generate_weight(field_name="Weightbirth", gestational_age=gestational_age)
        length_birth = BirthData.generate_length(field_name="Lengthbirth", gestational_age=gestational_age)
        ofc_birth = BirthData.generate_ofc(field_name="OFCbirth", gestational_age=gestational_age)
        face = Diagnosis.get_facial_symptoms(field_name="Face", orpha_id=orpha_id, symptoms_max_number=5)
        face_hpo = Diagnosis.get_facial_HPO(field_name="Face HPO", symptoms_list=face)
        photo = MedicalHistory.generate_photo_available(field_name="PhotoAvailable", options=PHOTO_AVAILABLE)

        fs = headfake.Fieldset(fields=[patient_id, zigosity2var, inheritance, dx, orpha_id, hpo, dob, sex,
                                       consanguinity, relatives, walk_indep, walk_age,
                                       speak_indep, speak_age, dd, id, id_level,
                                       asd, adhd, language_abs, gestational_age, weight_birth, length_birth, ofc_birth,
                                       face, face_hpo, photo])

        return fs

    def generate_biological_dataset(self, baseline_df):
        patient_id = baseline_df["Patient ID"]
        wb = []
        qpcr = []
        fluo = []
        protinter = []
        subcel = []
        morcel = []
        pathways = []
        splicing = []
        cadd = []
        metadome = []
        missense_3d = []
        dynamut = []
        acmg = []
        alphamiss = []

        for pid in patient_id:
            wb.append(BiologicalAnalysis.generate_western_blot(options=WESTERN_BLOT))
            qpcr.append(BiologicalAnalysis.generate_qpcr(options=QPCR))
            fluo.append(BiologicalAnalysis.generate_fluorescence_intensity(options=FLUORESCENCE_INTENSITY))
            protinter.append(BiologicalAnalysis.generate_protein_interaction(options=PROTEIN_INTERACTION))
            subcel.append(BiologicalAnalysis.generate_subcellular_location(options=SUBCELLULAR_LOCATION))
            morcel.append(BiologicalAnalysis.generate_cellular_morphology(options=CELLULAR_MORPHOLOGY))
            disease = baseline_df.loc[baseline_df['Patient ID'] == pid]['Dx'].values[0]
            pathways.append(BiologicalAnalysis.generate_pathways(disease))
            splicing.append(BiologicalAnalysis.generate_splicing(options=SPLICING))
            cadd.append(BiologicalAnalysis.generate_cadd())
            metadome.append(BiologicalAnalysis.generate_metadome(options=METADOME))
            missense_3d.append(BiologicalAnalysis.generate_missense_3d(options=MISSENSE_3D))
            dynamut.append(BiologicalAnalysis.generate_dynamut2(options=DYNAMUT))
            acmg.append(BiologicalAnalysis.generate_acmg(options=ACMG))
            alphamiss.append(BiologicalAnalysis.generate_alphamiss())

        data = {'Patient ID': patient_id, 'WB': wb, 'qPCR': qpcr, 'Fluo': fluo, 'Protinter': protinter,
                'Subcel': subcel,
                'Subcel': subcel, 'Morcel': morcel, 'Pathways': pathways, 'Splicing': splicing, 'CADD': cadd,
                'Metadome': metadome,
                'Missense3D': missense_3d, 'Dynamut2': dynamut, 'ACMG': acmg, 'Alphamiss': alphamiss}

        df = pd.DataFrame(data=data)

        return df

    def generate_dynamic_clinical_dataset(self, baseline_df, num_evolution_records):
        patient_id = []
        weight_date = []
        weigth = []
        height_date = []
        height = []
        ofc_date = []
        ofc = []

        for pid in baseline_df["Patient ID"]:
            date = datetime.datetime.strptime(baseline_df.loc[baseline_df['Patient ID'] == pid]['DOB'].values[0],
                                              "%d/%m/%Y")
            regexpre = "[0-9]+[.]*[0-9]+"

            new_weight = baseline_df.loc[baseline_df['Patient ID'] == pid]['Weightbirth'].values[0]
            weight_value = None
            m = re.search(regexpre, new_weight)
            if m:
                weight_value = float(m.group(0))

            new_length = baseline_df.loc[baseline_df['Patient ID'] == pid]['Lengthbirth'].values[0]
            length_value = None
            m = re.search(regexpre, new_length)
            if m:
                length_value = float(m.group(0))

            new_ofc = baseline_df.loc[baseline_df['Patient ID'] == pid]['OFCbirth'].values[0]
            ofc_value = None
            m = re.search(regexpre, new_ofc)
            if m:
                ofc_value = float(m.group(0))

            for i in range(num_evolution_records):
                date = date + datetime.timedelta(days=15)
                weight_value = round(weight_value + 0.4, 2)  # Add 400 gr each 15 days
                length_value = round(length_value + 1.5, 2)  # Add 1.5 cm each 15 days
                ofc_value = round(ofc_value + 1, 2)  # Add 1 cm each 15 days

                if date < datetime.datetime.now():
                    patient_id.append(pid)
                    weight_date.append(date.strftime("%d/%m/%Y"))
                    weigth.append(f'{weight_value} kg')
                    height_date.append(date.strftime("%d/%m/%Y"))
                    height.append(f'{length_value} cm')
                    ofc_date.append(date.strftime("%d/%m/%Y"))
                    ofc.append(f'{ofc_value} cm')

        data = {"Patient ID": patient_id, "Weightdate1": weight_date, "Weight1": weigth,
                "Heightdate1": height_date, "Height1": height, "OFCdate1": ofc_date, "OFC1": ofc}

        df = pd.DataFrame(data=data)

        return df

    def generate_genomic_dataset(self, baseline_df):
        patient_id = baseline_df["Patient ID"]
        ref_seq = []
        start = []
        end = []
        ref = []
        alt = []
        transcript = []
        variant = []
        variant_type = []
        zygosity = []
        protein_name = []
        gene = []
        chromosome = []

        for pid in patient_id:
            index = random.randint(0, len(self.variants) - 1)
            ref_seq.append(NGSTest.generate_reference_genome())
            start.append(self.variants["start"][index])
            end.append(self.variants["end"][index])
            ref.append(self.variants["ref"][index])
            alt.append(self.variants["alt"][index])
            transcript.append(Gene.generate_transcript())
            variant.append(self.variants["coding_name"][index])
            variant_type.append(self.variants["variant_type"][index])
            zygosity.append(Variant.generate_zygosity_2(ZYGOSITY))
            protein_name.append(self.variants["protein_name"][index])
            disease = baseline_df.loc[baseline_df['Patient ID'] == pid]['Dx'].values[0]
            gene.append(Gene.get_gene_from_disease(disease))
            chromosome.append(Gene.generate_chromosome())

        data = {'Patient ID': patient_id,
                "GenomicRefSeq": ref_seq, "Startnt": start, "Endnt": end,
                "Ref": ref, "Alt": alt, "NM_transcript": transcript, "Variant": variant, "Type": variant_type,
                "Zygosity": zygosity,
                "ProteinRefSeq": protein_name,
                "Gene": gene,
                "ChromosomeNumber": chromosome}

        df = pd.DataFrame(data=data)

        return df
