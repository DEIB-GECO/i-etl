import os.path
from functools import reduce

import pandas as pd
from pandas import DataFrame

from database.Execution import Execution
from enums.DataTypes import DataTypes
from enums.MetadataColumns import MetadataColumns
from enums.Ontologies import Ontologies
from enums.Profile import Profile
from enums.Visibility import Visibility
from preprocessing.Preprocess import Preprocess
from utils.setup_logger import log


class PreprocessImgge(Preprocess):
    def __init__(self, execution: Execution, data: DataFrame, metadata: DataFrame, profile: str, all_columns_dataset: list):
        super().__init__(execution=execution, data=data, metadata=metadata, profile=profile)
        self.metadata = metadata
        self.all_columns_dataset = all_columns_dataset

    def preprocess(self):
        log.info("pre-process IMGGE data")

        if self.profile == Profile.PHENOTYPIC:
            hpo_data_column = MetadataColumns.normalize_name(column_name="hpo_data")

            # 0. replace the "|" by "," in the list of HPOs + normalize HPO labels
            log.info(self.data[hpo_data_column])
            self.data[hpo_data_column] = self.data[hpo_data_column].apply(lambda x: x.replace("|", ",").replace("_", ":").replace(" ", ""))
            log.info(self.data[hpo_data_column])

            # 1. flatten the list of HPO terms to obtain one column for each term. The ontology resource associated to each is an hpo term
            # see issue 305: https://git.rwth-aachen.de/padme-development/external/better/data-cataloging/etl/-/issues/305
            all_hpo_terms = []
            for one_hpo_list in self.data[hpo_data_column]:
                all_hpo_terms.extend(one_hpo_list.split(","))
            log.info(f"{len(all_hpo_terms)} HPO terms")
            all_hpo_terms = list(set(all_hpo_terms))
            log.info(all_hpo_terms)
            # all_hpo_terms = [MetadataColumns.normalize_name(column_name=hpo_term) for hpo_term in all_hpo_terms]  # normalize later otherwise we cannot match HP: with hp:
            all_hpo_terms = [hpo_term for hpo_term in all_hpo_terms if hpo_term != ""]  # remove empty hpo terms (coming from the split or from the list)
            log.info(f"{len(all_hpo_terms)} distinct HPO terms")
            log.info(all_hpo_terms)

            # build the set of columns to add and add it once to avoid fragmented dataframe/performance issues
            # we need to write "0" and "1" instead of True and False because the df is read as a string
            hpo_dfs = [pd.DataFrame({hpo_term: self.data[hpo_data_column].apply(lambda x: "1" if hpo_term in x else None if x == "" else "0")}) for hpo_term in all_hpo_terms]
            log.info(hpo_dfs)
            # indexed_hpo_dfs = []
            # for one_hpo_df in hpo_dfs:
            #     # the drop_na() below is required because real IMGGE data contain two lines per patient (but no sample)
            #     # the below merge fails when there are duplicates
            #     # however, there are no overlaping values between two lines of a patient so we can drop the lines containing empty values
            #     one_hpo_df[self.execution.patient_id_column_name] = DataFrame(self.data[self.execution.patient_id_column_name].drop_duplicates().dropna())
            #     indexed_hpo_dfs.append(one_hpo_df)
            # log.info(indexed_hpo_dfs)
            # log.info("fin boucle")
            # this merge ends up in MemoryError because of the duplicates in patient ID (two lines per patients in real IMGGE data but no sample id)
            # concat_hpo_dfs = reduce(lambda x, y: x.merge(y, how="outer", on=self.execution.patient_id_column_name), indexed_hpo_dfs)  # concat_hpo_dfs = pd.merge(indexed_hpo_dfs, axis=1)
            # log.info("after reduce")
            # log.info(concat_hpo_dfs)
            log.info(self.data)
            for one_hpo_df in hpo_dfs:
                if len(one_hpo_df) == len(self.data):
                    self.data[one_hpo_df.columns[0]] = one_hpo_df  # one_hpo_df.columns[0] contains the name of the HPO term
                else:
                    log.info(f"Different number of patients in hpo data and data. Skipping hpo {one_hpo_df.columns[0]}")
            # self.data = self.data.merge(concat_hpo_dfs, how="inner", on=self.execution.patient_id_column_name)
            log.info(self.data)

            # 2. then, add the flattened HPO terms as features in the metadata
            current_filename = os.path.basename(self.execution.current_filepath)
            hpo_metadata = DataFrame(data={
                MetadataColumns.ONTO_NAME: [Ontologies.HPO["name"] for _ in range(len(hpo_dfs))],
                MetadataColumns.ONTO_CODE: all_hpo_terms,
                MetadataColumns.DATASET_NAME: [current_filename for _ in range(len(hpo_dfs))],
                MetadataColumns.COLUMN_NAME: all_hpo_terms,
                MetadataColumns.PROFILE: [Profile.PHENOTYPIC for _ in range(len(hpo_dfs))],
                MetadataColumns.VISIBILITY: [Visibility.PUBLIC for _ in range(len(hpo_dfs))],
                MetadataColumns.ETL_TYPE: [DataTypes.BOOLEAN for _ in range(len(hpo_dfs))]
            })

            self.metadata = pd.concat([self.metadata, hpo_metadata], axis=0)
            self.all_columns_dataset = pd.concat([self.all_columns_dataset, pd.Series(all_hpo_terms)])
