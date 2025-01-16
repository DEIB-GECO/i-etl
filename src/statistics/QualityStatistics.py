import dataclasses

from statistics.Statistics import Statistics


@dataclasses.dataclass(kw_only=True)
class QualityStatistics(Statistics):
    columns_no_ontology: list = dataclasses.field(default_factory=list)  # list of column names for which no ontology resource is provided
    columns_no_etl_type: list = dataclasses.field(default_factory=list)  # list of column names for which no etl type is provided
    columns_unmatched_typeof_etl_types: dict = dataclasses.field(default_factory=dict)  # { column_name: { "typeof_type": ttype, "etl_type": etype }, ... }
    columns_unknown_ontology: dict = dataclasses.field(default_factory=dict)  # { column_name: [set of unknown ontology names], ... }
    columns_unknown_etl_type: dict = dataclasses.field(default_factory=dict)  # { column_name: unknown_etl_type, ... }
    categorical_columns_without_json_values: list = dataclasses.field(default_factory=list)  # list of column names for which the ETL type is "category" and for which the column JSON_values is empty
    categorical_columns_unparseable_json: dict = dataclasses.field(default_factory=dict)  # { column_name: broken_json, ... }
    diagnosis_no_orphanet_code: list = dataclasses.field(default_factory=list)  # list of diagnosis standard names for which no orphanet code is provided
    data_columns_not_in_metadata: list = dataclasses.field(default_factory=list)  # list of data columns that are "removed" because not part of the metadata
    empty_cells_per_column: dict = dataclasses.field(default_factory=dict)  # { "column_name": X }
    failed_api_calls: dict = dataclasses.field(default_factory=dict)  # { "ontology/id_code": api_error, ... }
    unknown_categorical_values: dict = dataclasses.field(default_factory=dict)  # { column_name: [ set of unknown categorical values ], ... }
    unknown_boolean_values: dict = dataclasses.field(default_factory=dict)  # { column_name: [ set of unknown categorical boolean values ], ... }
    numerical_values_unmatched_unit: dict = dataclasses.field(default_factory=dict)  # { column_name: { value: { "expected_dim": exp_dim, "current_unit": curr_unit }, ... }, ... }
    non_numeric_values_with_unit: dict = dataclasses.field(default_factory=dict)  # { column_name: { value: curr_dim, ... }, ... }
    
    def add_column_with_no_ontology(self, column_name: str):
        if self.record_stats and column_name not in self.columns_no_ontology:
            self.columns_no_ontology.append(column_name)

    def add_column_with_no_etl_type(self, column_name: str):
        if self.record_stats and column_name not in self.columns_no_etl_type:
            self.columns_no_etl_type.append(column_name)

    def add_column_with_unmatched_typeof_etl_types(self, column_name: str, typeof_type: str, etl_type: str):
        # {column_name: { etl_type: [all encountered var types different from etl_type] }, ... }
        if self.record_stats:
            if column_name not in self.columns_unmatched_typeof_etl_types:
                self.columns_unmatched_typeof_etl_types[column_name] = {etl_type: []}
            if typeof_type not in self.columns_unmatched_typeof_etl_types[column_name][etl_type]:
                self.columns_unmatched_typeof_etl_types[column_name][etl_type].append(typeof_type)

    def add_column_unknown_ontology(self, column_name: str, ontology_name: str):
        if self.record_stats:
            if column_name not in self.columns_unknown_ontology:
                self.columns_unknown_ontology[column_name] = []
            if ontology_name not in self.columns_unknown_ontology[column_name]:
                self.columns_unknown_ontology[column_name].append(ontology_name)

    def add_column_unknown_etl_type(self, column_name: str, etl_type: str):
        if self.record_stats and column_name not in self.columns_unknown_etl_type:
            self.columns_unknown_etl_type[column_name] = etl_type

    def add_categorical_column_with_no_json(self, column_name: str):
        if self.record_stats and column_name not in self.categorical_columns_without_json_values:
            self.categorical_columns_without_json_values.append(column_name)

    def add_categorical_colum_with_unparseable_json(self, column_name: str, broken_json: str):
        if self.record_stats and column_name not in self.categorical_columns_unparseable_json:
            self.categorical_columns_unparseable_json[column_name] = broken_json

    def add_diagnosis_with_no_orphanet_code(self, diagnosis_standard_name: str):
        if self.record_stats and diagnosis_standard_name not in self.diagnosis_no_orphanet_code:
            self.diagnosis_no_orphanet_code.append(diagnosis_standard_name)

    def add_column_not_described_in_metadata(self, data_column_name: str):
        if self.record_stats and data_column_name not in self.data_columns_not_in_metadata:
            self.data_columns_not_in_metadata.append(data_column_name)

    def count_empty_cell_for_column(self, column_name: str) -> None:
        if column_name not in self.empty_cells_per_column:
            self.empty_cells_per_column[column_name] = 1
        else:
            self.empty_cells_per_column[column_name] += 1

    def add_failed_api_call(self, system: str, code: str, api_error: str):
        if self.record_stats and f"{system}/{code}" not in self.failed_api_calls:
            self.failed_api_calls[f"{system}/{code}"] = api_error

    def add_unknown_categorical_value(self, column_name: str, categorical_value: str):
        if self.record_stats:
            if column_name not in self.unknown_categorical_values:
                self.unknown_categorical_values[column_name] = []
            if categorical_value not in self.unknown_categorical_values[column_name]:
                self.unknown_categorical_values[column_name].append(categorical_value)

    def add_unknown_boolean_value(self, column_name: str, boolean_value: str):
        if self.record_stats:
            if column_name not in self.unknown_boolean_values:
                self.unknown_boolean_values[column_name] = []
            if boolean_value not in self.unknown_boolean_values[column_name]:
                self.unknown_boolean_values[column_name].append(boolean_value)

    def add_numerical_value_with_unmatched_unit(self, column_name: str, expected_unit: str, current_unit: str, value: str):
        # { column_name: { value: { "expected_dim": exp_dim, "current_unit": curr_unit }, ... }, ... }
        if self.record_stats:
            if column_name not in self.numerical_values_unmatched_unit:
                self.numerical_values_unmatched_unit[column_name] = {}
            if value not in self.numerical_values_unmatched_unit[column_name]:
                self.numerical_values_unmatched_unit[column_name][value] = {}
            self.numerical_values_unmatched_unit[column_name][value] = {"expected_unit": expected_unit, "current_unit": current_unit}

    def add_non_numeric_value_with_unit(self, column_name: str, unit: str, value: str):
        # { column_name: { value: curr_dim, ... }, ... }
        if self.record_stats:
            if column_name not in self.non_numeric_values_with_unit:
                self.non_numeric_values_with_unit[column_name] = {}
            if value not in self.non_numeric_values_with_unit[column_name]:
                self.non_numeric_values_with_unit[column_name][value] = unit
