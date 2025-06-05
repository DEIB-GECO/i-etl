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
            self.data[hpo_data_column] = self.data[hpo_data_column].apply(lambda x: MetadataColumns.normalize_name(column_name=x.replace("|", ",").replace("_", ":")).replace("hp:", "").replace(" ", ""))

            # 1. flatten the list of HPO terms to obtain one column for each term. The ontology resource associated to each is an hpo term
            # see issue 305: https://git.rwth-aachen.de/padme-development/external/better/data-cataloging/etl/-/issues/305
            all_hpo_terms = []
            for one_hpo_list in self.data[hpo_data_column]:
                all_hpo_terms.extend(one_hpo_list.split(","))
            log.info(f"{len(all_hpo_terms)} HPO terms")
            all_hpo_terms = list(set(all_hpo_terms))
            all_hpo_terms = [MetadataColumns.normalize_name(column_name=hpo_term) for hpo_term in all_hpo_terms]
            log.info(f"{len(all_hpo_terms)} distinct HPO terms")

            # build the set of columns to add and add it once to avoid fragmented dataframe/performance issues
            # we need to write "0" and "1" instead of True and False because the df is read as a string
            hpo_dfs = [pd.Series(self.data[hpo_data_column].apply(lambda x: "1" if hpo_term in x else "0"), name=hpo_term).to_frame() for hpo_term in all_hpo_terms]
            indexed_hpo_dfs = []
            for one_hpo_df in hpo_dfs:
                one_hpo_df[self.execution.patient_id_column_name] = self.data[self.execution.patient_id_column_name]
                indexed_hpo_dfs.append(one_hpo_df)
            concat_hpo_dfs = reduce(lambda x, y: x.merge(y, how="inner", on=self.execution.patient_id_column_name), indexed_hpo_dfs)  # concat_hpo_dfs = pd.merge(indexed_hpo_dfs, axis=1)
            self.data = self.data.merge(concat_hpo_dfs, how="inner", on=self.execution.patient_id_column_name)

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
