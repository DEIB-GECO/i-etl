from utils.file_utils import read_tabular_file_as_string
from utils.setup_logger import log

if __name__ == '__main__':
    df_demographics = read_tabular_file_as_string("../datasets/expes/covid/demographics_both_hospitals.csv")
    log.info(df_demographics)
    for column in df_demographics.columns:
        log.info(f"{column} ({len(df_demographics[column].unique()) == len(df_demographics[column])}): {list(df_demographics[column].unique())}")

    log.info("-------------------------------")
    log.info(f"nb diabetes cells: {len(df_demographics["diabetes"])}")
    log.info(f"empty cells: {len(df_demographics[df_demographics['diabetes'] == ""])}")

    # check if (and how many) patient ids are present in only one dataset, and similarly are in both datasets
    df_dynamic = read_tabular_file_as_string("../datasets/expes/covid/dynamics_clean_both_hospitals.csv")
    log.info(df_dynamic)
    dynamic_pids = list(df_dynamic["id"].unique())
    log.info(f"dyn pids: {dynamic_pids}")
    demographics_pids = list(df_demographics["id"].unique())
    log.info(f"demo pids: {demographics_pids}")
    log.info(f"nb demo pids: {len(demographics_pids)} vs. nb unique demo pids: {len(set(demographics_pids))}")
    pids_both = []
    pids_demo_only = []
    pids_dyn_only = []
    all_pids = list(dynamic_pids)  # deep copy
    all_pids.extend(list(demographics_pids))  # deep copy
    all_pids = list(set(all_pids))
    log.info(f"all pids: {all_pids}")
    log.info(f"len dyn: {len(dynamic_pids)}; len demo: {len(demographics_pids)}; len union set: {len(all_pids)}")
    for pid in all_pids:
        if pid in demographics_pids and pid in dynamic_pids:
            pids_both.append(pid)
        elif pid in demographics_pids and pid not in dynamic_pids:
            pids_demo_only.append(pid)
        elif pid in dynamic_pids and pid not in demographics_pids:
            pids_dyn_only.append(pid)

    log.info(f"only dyn: {len(pids_dyn_only)}; only demo: {len(pids_demo_only)}; both: {len(pids_both)}")
    log.info(pids_dyn_only)
    log.info(pids_demo_only)
    log.info(pids_both)
