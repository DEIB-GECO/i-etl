import pandas as pd
from pandas import DataFrame

from database.Execution import Execution
from enums.Profile import Profile
from preprocessing.Preprocess import Preprocess


class PreprocessCovid(Preprocess):
    def __init__(self, execution: Execution, data: DataFrame, profile: str):
        super().__init__(execution=execution, data=data, profile=profile)

    def preprocess(self):
        if self.profile == Profile.CLINICAL:
            # process samples data to transpose them
            self.data.drop(["hospital", "interpolated", "time_start", "time_end"], axis=1, inplace=True)
            new_df_as_json = {}
            columns = list(self.data["test"].unique())
            all_ids = list(self.data["id"].unique())
            one_patient = {}
            all_patients = []

            if "id" not in new_df_as_json:
                new_df_as_json["id"] = []
            for column in columns:
                new_df_as_json[column] = []

            for pid in all_ids:
                # print(f"****{pid}****")
                tests_for_patient = DataFrame(self.data.loc[self.data["id"] == pid]["test"])
                tests_for_patient = tests_for_patient.reset_index(drop=True)
                values_for_patient = DataFrame(self.data.loc[self.data["id"] == pid]["value"])
                values_for_patient = values_for_patient.reset_index(drop=True)
                max_occurrence = max(tests_for_patient.value_counts())

                one_patient["id"] = [pid for _ in range(max_occurrence)]
                for column in columns:
                    one_patient[column] = [None for _ in range(max_occurrence)]
                # print(f"one_patient = {one_patient}")
                # print(f"tests_for_patient = {tests_for_patient}")
                # print(f"values_for_patient = {values_for_patient}")
                for i in range(len(values_for_patient)):
                    the_test = tests_for_patient.iloc[i].iloc[0]
                    the_value = values_for_patient.iloc[i].iloc[0]
                    # print(f"value={the_value}; ")
                    # print(f"tests_for_patient[i]={the_test}")
                    # print(f"index {i%max_occurrence} in array: {one_patient[the_test]}")
                    one_patient[the_test][i%max_occurrence] = the_value
                    # print(f"one_patient = {one_patient}")
                all_patients.append(one_patient)
                one_patient = {}

            final_json = {}
            for patient_as_json in all_patients:
                for key in patient_as_json:
                    if key not in final_json:
                        final_json[key] = []
                    final_json[key].extend(patient_as_json[key])
            # print(json.dumps(all_patients))
            # print(json.dumps(final_json))

            self.data = pd.DataFrame(final_json)
            self.data["sid"] = [f"s{i}" for i in range(1, len(self.data)+1)]
