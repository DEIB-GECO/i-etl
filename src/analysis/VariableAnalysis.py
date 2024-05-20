from src.utils.setup_logger import log
from src.utils.utils import is_not_nan


class VariableAnalysis:
    def __init__(self, samples, metadata):
        self.samples = samples
        self.metadata = metadata

        self.sample_variables = self.samples.columns.to_list()
        self.metadata_variables = self.metadata["name"].to_list()

        self.nb_categorical_features_without_mapping = 0
        self.total_nb_categorical_features = 0

    def run_analysis(self):
        self.__compute_nb_variables_without_ontology()
        self.__compute_nb_categorical_features_without_mapping()

    def __compute_nb_variables_without_ontology(self):
        nb_variables_without_ontology = 0
        for data_variable in self.sample_variables:
            if data_variable not in self.metadata_variables:
                nb_variables_without_ontology += 1
        total_number_variables = len(self.sample_variables)
        ratio_variable_no_ontology = nb_variables_without_ontology / total_number_variables
        log.info("Number of variables without ontology: %s/%s=%s", nb_variables_without_ontology, total_number_variables, ratio_variable_no_ontology)

    def __compute_nb_categorical_features_without_mapping(self):
        self.nb_categorical_features_without_mapping = 0
        for index, metadata_variable in self.metadata.iterrows():
            if metadata_variable["vartype"] == "category":
                if not is_not_nan(metadata_variable["JSON_values"]):
                    self.nb_categorical_features_without_mapping += 1
                self.total_nb_categorical_features += 1
        ratio_categorical_feature_with_no_mapping = self.nb_categorical_features_without_mapping / self.total_nb_categorical_features
        log.info("Ratio of categorical feature having no mapping: %s/%s=%s", self.nb_categorical_features_without_mapping, self.total_nb_categorical_features, ratio_categorical_feature_with_no_mapping)
