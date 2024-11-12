from analysis.DistributionPlot import DistributionPlot
from database.Database import Database


class BasicPlot:
    def __init__(self, database: Database):
        self.database = database

    def run(self):
        # #2. MongoDB compass
        # # Show the entry for Hospital
        # # Show first Patient
        # # Show structure of one LabFeature
        # # all LabRecord instances for patient 1: {"has_subject": "Patient1"}
        # # LabRecord "Ethnicity" for Patient 1: {"$and": [{"has_subject": "Patient1"}, {"instantiates": "77"}]}
        #
        # # 3. comment the 3 lines above
        # # uncomment lines below
        lab_feature_url = ""  # TODO build_url(LAB_FEATURE_TABLE_NAME, 88)  # premature baby
        cursor = self.database.get_value_distribution_of_lab_feature(lab_feature_url, -1)
        plot = DistributionPlot(cursor, lab_feature_url, "Premature Baby",
                                False)  # do not print the cursor before, otherwise it would consume it
        plot.draw()

        lab_feature_url = ""  # TODO build_url(LAB_FEATURE_TABLE_NAME, 77)  # ethnicity
        cursor = self.database.get_value_distribution_of_lab_feature(lab_feature_url, 20)
        plot = DistributionPlot(cursor, lab_feature_url, "Ethnicity",
                                True)  # do not print the cursor before, otherwise it would consume it
        plot.draw()

        # cursor = self.database.get_min_value_of_lab_record(TableNames.LABORATORY_RECORD_TABLE_NAME, "76")
        # cursor = self.database.get_avg_value_of_lab_record(TableNames.LABORATORY_RECORD_TABLE_NAME, "76")
