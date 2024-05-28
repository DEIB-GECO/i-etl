class BasicStatistics():
    def __init__(self):
        pass

    def run(self):
        # #2. MongoDB compass
        # # Show the entry for Hospital
        # # Show first Patient
        # # Show structure of one Examination
        # # all examination records for patient 1: {"subject.reference": "Patient1"}
        # # examination "Ethnicity" for Patient 1: {"$and": [{"subject.reference": "Patient1"}, {"instantiate.reference": "77"}]}
        #
        # # 3. comment the 3 lines above
        # # uncomment lines below
        if self.compute_plots:
            examination_url = ""  # TODO build_url(EXAMINATION_TABLE_NAME, 88)  # premature baby
            cursor = self.database.get_value_distribution_of_examination(EXAMINATION_RECORD_TABLE_NAME, examination_url,
                                                                         -1)
            plot = DistributionPlot(cursor, examination_url, "Premature Baby",
                                    False)  # do not print the cursor before, otherwise it would consume it
            plot.draw()

            examination_url = ""  # TODO build_url(EXAMINATION_TABLE_NAME, 77)  # ethnicity
            cursor = self.database.get_value_distribution_of_examination(EXAMINATION_RECORD_TABLE_NAME, examination_url,
                                                                         20)
            plot = DistributionPlot(cursor, examination_url, "Ethnicity",
                                    True)  # do not print the cursor before, otherwise it would consume it
            plot.draw()

        # cursor = self.database.get_min_value_of_examination_record(TableNames.EXAMINATION_RECORD_TABLE_NAME, "76")
        # cursor = self.database.get_avg_value_of_examination_record(TableNames.EXAMINATION_RECORD_TABLE_NAME, "76")
