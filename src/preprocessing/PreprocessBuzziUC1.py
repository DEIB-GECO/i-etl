import os
import re

from pandas import DataFrame

from constants.structure import DOCKER_FOLDER_DATA
from database.Execution import Execution
from enums.AccessTypes import AccessTypes
from enums.DiagnosisColumns import DiagnosisColumns
from enums.MetadataColumns import MetadataColumns
from enums.Profile import Profile
from preprocessing.Preprocess import Preprocess
from utils.api_utils import send_query_to_api, parse_json_response
from utils.file_utils import read_tabular_file_as_string
from utils.setup_logger import log


class PreprocessBuzziUC1(Preprocess):
    def __init__(self, execution: Execution, data: DataFrame, profile: str):
        super().__init__(execution=execution, data=data, profile=profile)

        self.ids = []
        self.diagnosis_acronyms = []
        self.diagnosis_names = []
        self.affected_booleans = []
        self.orphanet = []
        self.gene_names = []
        self.inheritance = []
        self.chr_number = []
        self.zigosity = []
        self.diagnosis_counters = []
        self.mapping_diagnoses_infos = {}
        self.mapping_barcode_pid = {}

    def preprocess(self):
        log.info("pre-process BUZZI data")
        if self.profile == Profile.DIAGNOSIS:
            # 1. associate each disease to its information: gene, orphanet code, zigosity, etc
            transformation_df = read_tabular_file_as_string(os.path.join(DOCKER_FOLDER_DATA, "ds-transformation-table.xlsx"))
            transformation_df.rename(columns=lambda x: MetadataColumns.normalize_name(column_name=x), inplace=True)  # normalize column names
            transformation_df.rename(columns={"gene": DiagnosisColumns.GENE_NAME, "orpha_net": DiagnosisColumns.ORPHANET_CODE}, inplace=True)
            log.info(transformation_df)

            for row in transformation_df.itertuples(index=False):
                # acronym column
                acronym = row[transformation_df.columns.get_loc(DiagnosisColumns.ACRONYM)].lower().strip()
                if acronym not in self.mapping_diagnoses_infos:
                    self.mapping_diagnoses_infos[acronym] = {}
                # gene column
                gene_name = row[transformation_df.columns.get_loc(DiagnosisColumns.GENE_NAME)]
                if gene_name is not None and len(gene_name) > 0 and "," not in gene_name:
                    self.mapping_diagnoses_infos[acronym][DiagnosisColumns.GENE_NAME] = gene_name
                else:
                    # there is no gene for that disease or this is a multigenic disease,
                    # we do not record this
                    self.mapping_diagnoses_infos[acronym][DiagnosisColumns.GENE_NAME] = None
                # diagnosis name column
                diagnosis = row[transformation_df.columns.get_loc(DiagnosisColumns.DIAGNOSIS_NAME)]
                if diagnosis is not None and len(diagnosis) > 0:
                    self.mapping_diagnoses_infos[acronym][DiagnosisColumns.DIAGNOSIS_NAME] = diagnosis
                else:
                    self.mapping_diagnoses_infos[acronym][DiagnosisColumns.DIAGNOSIS_NAME] = None
                # orphanet code column
                orpha_code = row[transformation_df.columns.get_loc(DiagnosisColumns.ORPHANET_CODE)]
                if orpha_code != "":
                    code = orpha_code.replace("ORPHA:", "")
                    self.mapping_diagnoses_infos[acronym][DiagnosisColumns.ORPHANET_CODE] = orpha_code
                    self.mapping_diagnoses_infos[acronym][DiagnosisColumns.INHERITANCE] = PreprocessBuzziUC1.get_inheritance(diagnosis_code=code)
                    self.mapping_diagnoses_infos[acronym][DiagnosisColumns.CHR_NUMBER] = PreprocessBuzziUC1.get_chromosome(diagnosis_code=code)
                else:
                    self.mapping_diagnoses_infos[acronym][DiagnosisColumns.ORPHANET_CODE] = None
                    self.mapping_diagnoses_infos[acronym][DiagnosisColumns.INHERITANCE] = None
                    self.mapping_diagnoses_infos[acronym][DiagnosisColumns.CHR_NUMBER] = None

                self.mapping_diagnoses_infos[acronym][DiagnosisColumns.ZIGOSITY] = None
            log.info(f"{len(self.mapping_diagnoses_infos)} acronyms")

            # 2. associate each sample barcode to the patient id
            prefix = Profile.get_prefix_for_path(filetype=Profile.PHENOTYPIC)
            df = read_tabular_file_as_string(filepath=f"{os.path.join(prefix, "screening.csv")}")  # cannot replace this by self.execution.current_filepath because it contains the diagnosis file data
            self.mapping_barcode_pid = {row[df.columns.get_loc("SampleBarcode")]: row[df.columns.get_loc("id")] for row in df.itertuples(index=False)}

            # 3. for each patient, collect the acronym and whether he is affected or a carrier
            count_affected = 0
            count_carrier = 0
            count_skipped = 0
            log.info(self.data.columns)
            for row in self.data.itertuples(index=False):
                pid = row[self.data.columns.get_loc("patient ID")]
                if row[self.data.columns.get_loc("affetto")] != "":
                    count_affected += 1
                    # the patient is affected by this disease, so we record this
                    self.record_diagnosis_for_patient(pid=pid, row=row, column="affetto")
                if row[self.data.columns.get_loc("carrier")] != "":
                    count_carrier += 1
                    # the patient is a carrier of the disease
                    # if we decide to record it, we record everything...
                    if self.execution.record_carrier_patients:
                        self.record_diagnosis_for_patient(pid=pid, row=row, column="carrier")
                    else:
                        # ...otherwise we do not record any information
                        count_skipped += 1

            log.info(f"count affected is {count_affected}, count carrier is {count_carrier}, count skipped is {count_skipped}, multi diagnosis is 11 = {count_affected+count_carrier+count_skipped+11}")
            log.info(f"{len(self.ids)} ids")
            log.info(f"{len(self.diagnosis_names)} diagnosis name")
            log.info(f"{len(self.diagnosis_acronyms)} acronyms")
            log.info(f"{len(self.affected_booleans)} booleans")
            log.info(f"{len(self.orphanet)} orphanet codes")
            log.info(f"{len(self.gene_names)} gene names")
            log.info(f"{len(self.inheritance)} inheritance names")
            log.info(f"{len(self.chr_number)} chr number")
            log.info(f"{len(self.zigosity)} zigosity")
            log.info(f"{len(self.diagnosis_counters)} diagnosis counters")

            self.data = DataFrame()
            self.data[DiagnosisColumns.ID] = self.ids
            self.data[DiagnosisColumns.DIAGNOSIS_NAME] = self.diagnosis_names
            self.data[DiagnosisColumns.ACRONYM] = self.diagnosis_acronyms
            self.data[DiagnosisColumns.AFFECTED] = self.affected_booleans
            self.data[DiagnosisColumns.ORPHANET_CODE] = self.orphanet
            self.data[DiagnosisColumns.GENE_NAME] = self.gene_names
            self.data[DiagnosisColumns.INHERITANCE] = self.inheritance
            self.data[DiagnosisColumns.CHR_NUMBER] = self.chr_number
            self.data[DiagnosisColumns.ZIGOSITY] = self.zigosity
            self.data[DiagnosisColumns.DISEASE_COUNTER] = self.diagnosis_counters
            log.info(self.data)
            log.info(self.data.iloc[0])

    def add_id(self, pid):
        if pid in self.mapping_barcode_pid:
            # the column is named "patient_id" in buzzi
            # but this actually contains sample bar codes
            self.ids.append(self.mapping_barcode_pid[pid])
        else:
            # we did not find the corresponding patient id in the mapping,
            # thus we simply write the sample bar code as it is
            self.ids.append(pid)

    def record_diagnosis_for_patient(self, pid, row, column: str):
        # column is "affetto" or "carrier"
        self.data.loc[:, column] = self.data[column].apply(lambda x: x.replace("/", "+") if "/" in x else x)
        for counter, disease in enumerate(row[self.data.columns.get_loc(column)].split("+")):
            self.add_id(pid=pid)
            acronym = disease.lower().strip()
            self.diagnosis_acronyms.append(acronym)
            self.diagnosis_counters.append(int(counter+1))  # +1 because enumerates starts at 0
            if column == "affetto":
                self.affected_booleans.append(True)
            else:
                self.affected_booleans.append(False)  # carrier
            if acronym in self.mapping_diagnoses_infos:
                self.diagnosis_names.append(self.mapping_diagnoses_infos[acronym][DiagnosisColumns.DIAGNOSIS_NAME])
                self.orphanet.append(self.mapping_diagnoses_infos[acronym][DiagnosisColumns.ORPHANET_CODE])
                self.gene_names.append(self.mapping_diagnoses_infos[acronym][DiagnosisColumns.GENE_NAME])
                self.inheritance.append(self.mapping_diagnoses_infos[acronym][DiagnosisColumns.INHERITANCE])
                self.chr_number.append(self.mapping_diagnoses_infos[acronym][DiagnosisColumns.CHR_NUMBER])
                self.zigosity.append(self.mapping_diagnoses_infos[acronym][DiagnosisColumns.ZIGOSITY])
            else:
                self.diagnosis_names.append(None)
                self.orphanet.append(None)
                self.gene_names.append(None)
                self.inheritance.append(None)
                self.chr_number.append(None)
                self.zigosity.append(None)

    @classmethod
    def get_inheritance(cls, diagnosis_code: str) -> str | None:
        url = f"https://api.orphadata.com/rd-natural_history/orphacodes/{diagnosis_code}"
        response = send_query_to_api(url=url, secret="nbarret", access_type=AccessTypes.API_KEY_IN_HEADER)
        data = parse_json_response(response)
        if "data" in data and "results" in data["data"] and "TypeOfInheritance" in data["data"]["results"]:
            inheritances = data["data"]["results"]["TypeOfInheritance"]
            if len(inheritances) >= 1:
                return data["data"]["results"]["TypeOfInheritance"][0]
        return None

    @classmethod
    def get_chromosome(cls, diagnosis_code: str) -> str | None:
        url = f"https://api.orphadata.com/rd-associated-genes/orphacodes/{diagnosis_code}"
        response = send_query_to_api(url=url, secret="nbarret", access_type=AccessTypes.API_KEY_IN_HEADER)
        data = parse_json_response(response)
        if "data" in data and "results" in data["data"] and "DisorderGeneAssociation" in data["data"]["results"]:
            associations = data["data"]["results"]["DisorderGeneAssociation"]
            if len(associations) >= 1:
                associations = associations[0]
                if "Gene" in associations and "Locus" in associations["Gene"]:
                    all_locus = associations["Gene"]["Locus"]
                    if len(all_locus) >= 1:
                        all_locus = all_locus[0]
                        if "GeneLocus" in all_locus:
                            full_chromosome_position = all_locus["GeneLocus"]
                            regex_elements = re.search(r"[0-9]+", full_chromosome_position)
                            return regex_elements.group()  # the chromosome_number is the first number in the string
        return None
