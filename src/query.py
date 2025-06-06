from data_retriever.DataRetriever import DataRetriever

if __name__ == '__main__':
    # ssh -L 27018:131.175.120.86:27018 barret@131.175.120.86
    # command to open SSH tunnel to access the databases on GENEX from my laptop
    # to be run before running this script

    # SJD
    FEATURE_CODES = {"hypotonia": "398152000", "vcf_path": "12953007"}  # , "gene": "82256003:734841007"}
    FEATURES_FILTERS = {"sex": {"code": "734000001", "filter": {"sex.label": "Female"}}}
    FEATURES_VALUE_PROCESS = {"hypotonia": None, "vcf_path": None, "sex": "get_label"}
    dataRetriever = DataRetriever(mongodb_url="mongodb://localhost:27018/", db_name="better_hsjd",
                                  feature_selected=FEATURE_CODES, feature_value_process=FEATURES_VALUE_PROCESS,
                                  feature_filters=FEATURES_FILTERS)
    dataRetriever.run()
    print(dataRetriever.the_dataframe)
    # dataRetriever.the_dataframe.to_csv("/home/barret/ietl-folder/sjd/table_sjd.csv", index=False)

    # # IMGGE
    # FEATURE_CODES = {"hypotonia": "0001252", "vcf_path": "12953007"}
    # FEATURES_VALUE_PROCESS = {"hypotonia": None, "vcf_path": None}
    # dataRetriever = DataRetriever(mongodb_url="mongodb://localhost:27018/", db_name="better_imgge", feature_codes=FEATURE_CODES, feature_value_process=FEATURES_VALUE_PROCESS)
    # dataRetriever.run()
    # print(dataRetriever.the_dataframe)
    # dataRetriever.the_dataframe.to_csv("/home/barret/ietl-folder/imgge/table_imgge.csv", index=False)
