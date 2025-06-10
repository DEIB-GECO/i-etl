from data_retriever.DataRetriever import DataRetriever

if __name__ == '__main__':
    # ssh -L 27018:131.175.120.86:27018 barret@131.175.120.86
    # command to open SSH tunnel to access the databases on GENEX from my laptop
    # to be run before running this script

    # ssh -L 27018:131.175.120.88:27018 barret@131.175.120.88
    # command to open SSH tunnel to access the databases on GECO from my laptop
    # to be run before running this script

    # SJD
    FEATURE_CODES = {"label": "snomedct:398152000", "filename": "snomedct:12953007"}  # "face_hpo": '8116006:398302004=(278201002="HPO")', "gene": "82256003:734841007"}
    FEATURES_FILTERS = {"label": None, "filename": None}
    FEATURES_VALUE_PROCESS = {"label": None, "filename": None}
    dataRetriever = DataRetriever(mongodb_url="mongodb://131.175.120.88:27018/", db_name="better_sjd",
                                  query_type="data",
                                  feature_selected=FEATURE_CODES, feature_value_process=FEATURES_VALUE_PROCESS,
                                  feature_filters=FEATURES_FILTERS)
    dataRetriever.run()
    print(dataRetriever.the_dataframe)
    dataRetriever.the_dataframe.to_csv("table_sjd.csv")

    # # IMGGE
    # FEATURE_CODES = {"hypotonia": "hp:0001252", "vcf_path": "snomedct:12953007"}
    # FEATURES_FILTERS = {"sex": {"code": "snomedct:734000001", "filter": {"sex.label": "Female"}}}
    # FEATURES_VALUE_PROCESS = {"hypotonia": None, "vcf_path": None, "sex": "get_label"}
    # dataRetriever = DataRetriever(mongodb_url="mongodb://localhost:27018/", db_name="better_imgge",
    #                               query_type="data",
    #                               feature_selected=FEATURE_CODES, feature_value_process=FEATURES_VALUE_PROCESS,
    #                               feature_filters=FEATURES_FILTERS)
    # dataRetriever.run()
    # print(dataRetriever.the_dataframe)
    # dataRetriever.the_dataframe.to_csv("table_imgge.csv")
    #
    # # Metadata SJD
    # FEATURE_CODES = ["snomed ct:398152000", "snomedct:12953007", "snomedct:82256003:  734841007"]
    # dataRetriever = DataRetriever(mongodb_url="mongodb://localhost:27018/", db_name="better_hsjd",
    #                               query_type="metadata", feature_list=FEATURE_CODES)
    # dataRetriever.run()
    # print(dataRetriever.the_dataframe)
    # dataRetriever.the_dataframe.to_csv("metadata_sjd.csv")
