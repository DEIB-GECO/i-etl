import itertools
import json
import os

from pandas import DataFrame

from database.Database import Database
from database.Execution import Execution
from entities.Feature import Feature
from entities.OntologyResource import OntologyResource
from enums.DataTypes import DataTypes
from enums.HospitalNames import HospitalNames
from enums.MetadataColumns import MetadataColumns
from enums.Ontologies import Ontologies
from enums.Profile import Profile
from enums.TableNames import TableNames
from enums.VcfColumns import vcf_columns
from enums.Visibility import Visibility
from etl.Task import Task
from preprocessing.PreprocessingTask import PreprocessingTask
from statistics.QualityStatistics import QualityStatistics
from utils.file_utils import read_tabular_file_as_string
from utils.setup_logger import log


class Extract(Task):

    def __init__(self, metadata: DataFrame, profile: str, database: Database, execution: Execution, quality_stats: QualityStatistics):
        super().__init__(database=database, execution=execution, quality_stats=quality_stats)
        self.data = None
        self.metadata = metadata
        self.profile = Profile.normalize(profile)
        self.columns_dataset_all_profiles = None
        # self.mapping_categorical_value_to_onto_resource = {}  # <categorical value label ("JSON_values" column), OntologyResource>
        self.mapping_column_to_categorical_value = {}  # <column name, list of normalized accepted values>
        self.mapping_column_to_vartype = {}  # <column name, var type ("vartype" column)>
        self.mapping_column_to_unit = {}  # <column name, unit provided in the metadata>
        self.mapping_column_to_domain = {}  # <column name, <min: x, max: y> or <accepted_values: [...]>>

    def run(self) -> None:

        # filter and normalize metadata
        # filter: keep metadata about the current triplet <dataset, profile, hospital>
        # normalize: the header and the values
        self.filter_metadata_file()
        self.normalize_metadata_file()
        self.export_metadata_to_csv_for_generative_ai()

        # filter and normalize data
        # filter: remove data columns that are not described in the metadata
        # normalize: the header
        if self.metadata is not None:
            # preprocess input to have all the necessary data, as described in the metadata
            self.load_tabular_data()
            self.pre_process_data_file()
            self.filter_data_file()
            self.normalize_data_file()

            # compute mappings (categories, units and domains)
            self.compute_mapping_categorical_value_to_onto_resource()
            self.compute_column_to_unit()
            self.compute_column_to_domain()
            self.export_data_to_csv_for_generative_ai()

    def export_metadata_to_csv_for_generative_ai(self):
        exported_filepath = os.path.join(self.execution.working_dir_current, "exported_metadata.csv")
        log.info(self.metadata)
        if os.path.exists(exported_filepath) and os.path.getsize(exported_filepath) > 0:
            # existing metadata, we happen
            self.metadata.to_csv(exported_filepath, mode="a", header=False, index=False)
        else:
            self.metadata.to_csv(exported_filepath, index=False)

    def export_data_to_csv_for_generative_ai(self):
        log.info(self.execution.current_filepath)
        filename = os.path.basename(self.execution.current_filepath)  # it contains the .csv
        filename = filename[0:filename.index(".")]
        log.info(filename)
        exported_filepath = os.path.join(self.execution.working_dir_current, f"exported_{filename}.csv")
        log.info(self.data)
        if os.path.exists(exported_filepath) and os.path.getsize(exported_filepath) > 0:
            # existing metadata, we happen
            self.data.to_csv(exported_filepath, mode="a", header=False, index=False)
        else:
            self.data.to_csv(exported_filepath, index=False)

    def filter_metadata_file(self) -> None:
        # Normalize the header, e.g., "Significato it" becomes "significato_it"
        # this also normalizes hospital names if they are in the header (UC 2 and UC 3)
        self.metadata.rename(columns=lambda x: MetadataColumns.normalize_name(column_name=x), inplace=True)

        # normalize the profiles before filtering
        self.metadata.loc[:, MetadataColumns.PROFILE] = self.metadata[MetadataColumns.PROFILE].apply(lambda x: Profile.normalize(file_type=x))
        self.columns_dataset_all_profiles = self.metadata[MetadataColumns.COLUMN_NAME]
        self.metadata = self.metadata[self.metadata[MetadataColumns.PROFILE].values == self.profile]

        # if the filtered metadata (by dataset and profile) is not empty, we check whether we need to further filter
        # metadata by hospital name
        if len(self.metadata) > 0:
            normalized_hospital_name = HospitalNames.normalize(self.execution.hospital_name)
            if normalized_hospital_name in self.metadata.columns:
                # the current hospital is in the metadata, we need to select the metadata line for which the
                # hospital column has 1
                self.metadata.loc[:, normalized_hospital_name] = self.metadata[normalized_hospital_name].apply(lambda x: MetadataColumns.normalize_value(column_value=x))
                self.metadata = self.metadata[self.metadata[normalized_hospital_name].isin([1, "1"])]
            else:
                # we have no column specifying a hospital name, so the metadata is only for the current hospital
                # thus, nothing to do
                pass

            # 6. reindex the remaining metadata because when dropping rows/columns, they keep their original index
            self.metadata.reset_index(drop=True, inplace=True)

            log.info(f"{len(self.metadata.columns)} columns and {len(self.metadata)} lines in the metadata file.")
        else:
            # no metadata for this combination of <dataset, profile>, so we skip it
            self.metadata = None
            log.info("metadata is None")

    def normalize_metadata_file(self) -> None:
        if self.metadata is not None:
            # Normalize ontology names (but not codes because they will be normalized within OntologyResource)
            self.metadata.loc[:, MetadataColumns.ONTO_NAME] = self.metadata[MetadataColumns.ONTO_NAME].apply(lambda value: Ontologies.normalize_name(ontology_name=value))

            # Normalize ETL type
            self.metadata.loc[:, MetadataColumns.ETL_TYPE] = self.metadata[MetadataColumns.ETL_TYPE].apply(lambda x: DataTypes.normalize(data_type=x))

            # Normalize visibility
            self.metadata.loc[:, MetadataColumns.VISIBILITY] = self.metadata[MetadataColumns.VISIBILITY].apply(lambda x: Visibility.normalize(visibility=x))

            # Normalize column name
            self.metadata.loc[:, MetadataColumns.COLUMN_NAME] = self.metadata[MetadataColumns.COLUMN_NAME].apply(lambda x: MetadataColumns.normalize_name(column_name=x))

            # Compute some stats about the metadata
            for row in self.metadata.itertuples(index=False):
                column_name = row[self.metadata.columns.get_loc(MetadataColumns.COLUMN_NAME)]
                etl_type = row[self.metadata.columns.get_loc(MetadataColumns.ETL_TYPE)]
                onto_name = row[self.metadata.columns.get_loc(MetadataColumns.ONTO_NAME)]
                onto_code = row[self.metadata.columns.get_loc(MetadataColumns.ONTO_CODE)]
                if onto_code == "":
                    self.quality_stats.add_column_with_no_ontology(column_name=column_name)
                if etl_type == "":
                    self.quality_stats.add_column_with_no_etl_type(column_name=column_name)
                else:
                    if etl_type not in DataTypes.values():
                        self.quality_stats.add_column_unknown_etl_type(column_name=column_name, etl_type=etl_type)
                if onto_name != "" and len(Ontologies.get_enum_from_name(onto_name)) == 0:
                    self.quality_stats.add_column_unknown_ontology(column_name=column_name, ontology_name=onto_name)
        else:
            # metadata is None because nothing remains from the filtering
            # skip the normalization (nothing to normalize)
            pass

    def load_tabular_data(self) -> None:
        log.info(f"Data filepath is {self.execution.current_filepath}")
        assert os.path.exists(self.execution.current_filepath), "The provided data file could not be found."
        self.data = read_tabular_file_as_string(filepath=self.execution.current_filepath)

    def normalize_data_file(self):
        # Normalize the data values
        # they will be cast to the right type (int, float, datetime) in the Transform step
        # issue 113: we do not normalize identifiers assigned by hospitals to avoid discrepancies (not the VCF filenames)
        columns_no_normalization = []
        columns_no_normalization.append(self.execution.patient_id_column_name)
        if self.execution.hospital_name in vcf_columns:
            columns_no_normalization.append(vcf_columns[self.execution.hospital_name])
        if self.execution.sample_id_column_name != "":
            columns_no_normalization.append(self.execution.sample_id_column_name)

        for column in self.data:
            if column not in columns_no_normalization:
                self.data.loc[:, column] = self.data[column].apply(lambda x: MetadataColumns.normalize_value(column_value=x))

        log.info(f"{len(self.data.columns)} columns and {len(self.data)} lines in the data file.")

    def pre_process_data_file(self) -> None:
        # preprocess data files, i.e., change the data DataFrame to fit the metadata
        # we do not write the pre-processed data to any new ile, we simply run the ETL with it
        # this avoids to (a) overwrite given data files and (b) to have filenames which differ from the metadata
        preprocessing_task = PreprocessingTask(execution=self.execution, data=self.data, metadata=self.metadata, profile=self.profile)
        preprocessing_task.run()
        self.data = preprocessing_task.data

        # normalize column names
        self.data = self.data.rename(columns=lambda x: MetadataColumns.normalize_name(column_name=x))

    def filter_data_file(self) -> None:
        # Normalize column names ("sex", "dateOfBirth", "Ethnicity", etc.) to match column names described in the metadata
        self.data = self.data.rename(columns=lambda x: MetadataColumns.normalize_name(column_name=x))

        # removes the data columns that are NOT described in the metadata or that are explicitly marked as not to be loaded (except if this is an ID column)
        # if a column is described in the metadata but is not present in the data or this column is empty we keep it
        # because people took the time to describe it.
        data_columns = list(set(self.data.columns))  # get the distinct list of columns
        columns_described_in_metadata = list(self.columns_dataset_all_profiles.apply(lambda x: MetadataColumns.normalize_name(x)))  # https://git.rwth-aachen.de/padme-development/external/better/data-cataloging/etl/-/issues/282
        columns_to_drop = [data_column for data_column in data_columns if data_column not in columns_described_in_metadata or (data_column in self.execution.columns_to_remove and data_column not in [self.execution.patient_id_column_name, self.execution.sample_id_column_name])]
        self.data = self.data.drop(columns_to_drop, axis=1)  # axis=1 -> columns
        for data_column in data_columns:
            # we record this column in the stats only if it is not described at all in the current file metadata
            # this is because we iteratively look at the metadata of each pair <dataset, profile>
            # and we do not want to record a column as "not described" if it is later described in another profile
            # if data_column not in self.described_columns_current_file:
            if data_column in columns_to_drop:
                log.info(f"Add data column {data_column} as not described.")
                self.quality_stats.add_column_not_described_in_metadata(data_column_name=data_column)

    def compute_mapping_categorical_value_to_onto_resource(self) -> None:
        # Apr 15, 2025: I cannot use the mapping category/OR
        # because in IMGGE all categorical values are encoded with numbers (1, 2, 3, etc.)
        # and thus this mapping lacks a level of nesting with the feature name to avoid writing agin and again new OR for categorical values
        # self.mapping_categorical_value_to_onto_resource = {}
        self.mapping_column_to_categorical_value = {}
        # 1. first, we retrieve the existing categorical values already transformed as OntologyResource
        # this will avoid to send again API queries to re-build already-built OntologyResource,
        # e.g., when starting from an existing DB (drop=False)
        existing_categorical_codeable_concepts = {}
        # the set of categorical values are defined in Features only, thus we can restrict the find to only those:
        # categorical_values_for_table_name = {'_id': ObjectId('...'), 'categorical_values': [{...}, {...}, ...]}
        categorical_values_for_table_name = self.database.find_operation(table_name=TableNames.FEATURE, filter_dict={Feature.CATEGORIES_: {"$exists": 1}}, projection={Feature.CATEGORIES_: 1})
        for one_tuple in categorical_values_for_table_name:
            # existing_categorical_value_for_table_name = [{...}, {...}, ...]}
            existing_categorical_values_for_table_name = one_tuple[Feature.CATEGORIES_]
            for encoded_categorical_value in existing_categorical_values_for_table_name:
                existing_or = OntologyResource.from_json(encoded_categorical_value, quality_stats=self.quality_stats)
                existing_categorical_codeable_concepts[existing_or.label] = existing_or

        # 2. then, we associate each column to its set of categorical values
        # if we already compute the cc of that value (e.g., several column have categorical values Yes/No/NA),
        # we do not recompute it and take it from the mapping
        for row in self.metadata.itertuples(index=False):
            column_name = row[self.metadata.columns.get_loc(MetadataColumns.COLUMN_NAME)]
            candidate_json_values = row[self.metadata.columns.get_loc(MetadataColumns.JSON_VALUES)]
            column_type = row[self.metadata.columns.get_loc(MetadataColumns.ETL_TYPE)]
            if candidate_json_values != "":
                # we get the possible categorical values for the column, e.g., F, or M, or NA for sex
                try:
                    json_categorical_values = json.loads(candidate_json_values)
                except Exception:
                    self.quality_stats.add_categorical_colum_with_unparseable_json(column_name=column_name, broken_json=candidate_json_values)
                    json_categorical_values = {}
                categories_for_column = {}  # we append this dictionary to the list only if it has at least one categorical value
                # { 'sex': {
                #   'm': {"system": "snomed", "code": "248153007", "label": "m (Male)"},
                #   'f': {"system": "snomed", "code": "248152002", "label": "f (Female)"},
                #   'inadeguata': {"system": "snomed", "code": "71978007", "label": "inadeguata (inadequate)"},
                #   ...
                # }, ...}
                for json_categorical_value in json_categorical_values:
                    normalized_categorical_value = MetadataColumns.normalize_value(json_categorical_value["value"])
                    if normalized_categorical_value not in categories_for_column:
                        # the categorical value does not exist yet in the mapping, thus:
                        # - it may be retrieved from the db and be added to the mapping
                        # - or, it may be computed for the first time
                        if normalized_categorical_value not in existing_categorical_codeable_concepts.keys():
                            # this is a categorical value that we have never seen (not even in previous executions),
                            # we need to create an OntologyResource for it from scratch
                            # json_categorical_value is of the form: {'value': 'X', 'explanation': 'some definition', 'onto_system_Y': 'onto_code_Z' }
                            if len(json_categorical_value) == 2 and "value" in json_categorical_value and "explanation" in json_categorical_value:
                                # this is the specific case when categories are not mapped to a code, but they are encoded
                                # therefore the mapping is simply the encoded value (1, 2, 3, etc) to the label (found in the "explanation" field)
                                # we need to use empty system and code instead of None otherwise NoneType exception
                                onto_resource = OntologyResource(system="", code="", label=json_categorical_value["explanation"], quality_stats=self.quality_stats)
                                categories_for_column[normalized_categorical_value] = onto_resource.to_json()
                            else:
                                for key, val in json_categorical_value.items():
                                    # for any key value pair that is not about the value or the explanation
                                    # (i.e., loinc and snomedct columns), we create an OntologyResource, which we add to the CodeableConcept
                                    # we need to do a loop because there may be several ontology terms for a single mapping
                                    if key != "value" and key != "explanation":
                                        # here, we do normalize the ontology name to be able to get the corresponding enum
                                        # however, we do not normalize the code, because it needs extra attention (due to spaces in post-coordinated codes, etc)
                                        ontology = Ontologies.get_enum_from_name(ontology_name=Ontologies.normalize_name(key))
                                        onto_resource = OntologyResource(system=ontology, code=val, label=None, quality_stats=self.quality_stats)
                                        if onto_resource.system != "" and onto_resource.code != "":
                                            categories_for_column[normalized_categorical_value] = onto_resource.to_json()
                                        else:
                                            # the ontology system is unknown or no code has been provided,
                                            # thus the OntologyResource contains only None fields,
                                            # thus we do not record it as a possible category
                                            pass

                        else:
                            # an OntologyResource already exists for this value (it has been retrieved from the db),
                            # we simply add it to the mapping
                            log.debug(f"The categorical value {normalized_categorical_value} already exists in the database as a CC. Taking it from here.")
                            categories_for_column[normalized_categorical_value] = existing_categorical_codeable_concepts[normalized_categorical_value].to_json()
                    if column_name not in self.mapping_column_to_categorical_value and len(categories_for_column) > 0:
                        # there is at least one category for this column
                        self.mapping_column_to_categorical_value[column_name] = categories_for_column
            else:
                # if this was supposed to be categorical (thus having values), we count it in the reporting
                # otherwise, this is not a categorical column (thus, everything is fine)
                if column_type == DataTypes.CATEGORY:
                    # log.info(f"no JSON categories for column {column_name}")
                    self.quality_stats.add_categorical_column_with_no_json(column_name=column_name)
        log.debug(f"{self.mapping_column_to_categorical_value}")

    def compute_column_to_unit(self) -> None:
        self.mapping_column_to_unit = {}

        for row in self.metadata.itertuples(index=False):
            unit = row[self.metadata.columns.get_loc(MetadataColumns.VAR_UNIT)]
            if unit != "":
                self.mapping_column_to_unit[row[self.metadata.columns.get_loc(MetadataColumns.COLUMN_NAME)]] = unit
            else:
                self.mapping_column_to_unit[row[self.metadata.columns.get_loc(MetadataColumns.COLUMN_NAME)]] = None

        if len(self.mapping_column_to_unit) > 10:
            log.debug(dict(itertools.islice(self.mapping_column_to_unit.items(), 10)))
        else:
            log.debug(self.mapping_column_to_unit)

    def compute_column_to_domain(self) -> None:
        self.mapping_column_to_domain = {}

        for row in self.metadata.itertuples(index=False):
            domain = row[self.metadata.columns.get_loc(MetadataColumns.DOMAIN)]
            try:
                domain = json.loads(domain)
                self.mapping_column_to_domain[row[self.metadata.columns.get_loc(MetadataColumns.COLUMN_NAME)]] = domain
            except:
                self.mapping_column_to_domain[row[self.metadata.columns.get_loc(MetadataColumns.COLUMN_NAME)]] = None
        log.debug(self.mapping_column_to_domain)
