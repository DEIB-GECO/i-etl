# BETTER-fairificator
The FAIRification tools for BETTER project.


### Installation

**From the root of the project, i.e., in `BETTER-fairificator` folder**

1. Make sure to have a recent **Python 3** version, e.g., Python 3.12 (tested with Python 3.12 only)
2. Run the configuration script: `bash configure.sh`
3. Activate the virtual environment: `source .venv-better-fairificator/bin/activate`
4. To run the ETL script: `python3 src/main.py --hospital_name=<hospital_name> --database_name=<database_name> --metadata_filepath=<path/to/metadata.csv> --data_filepath=<path/to/data.csv> --drop=<drop>` where:
  - `--hospital_name` ranges over: \["IT_BUZZI_UC1", "RS_IMGGE", "ES_HSJD", "IT_BUZZI_UC3", "ES_TERRASSA", "DE_UKK", "ES_LAFE", "IL_HMC"\] (required)
  - `--database_name` is the database name (optional, default: `better_default`)
  - `--metadata_filepath` is the (absolute) path of the metadata file (required)
  - `--data_filepath=datasets/data/BUZZI/screening.csv` is the (absolute) path of the data file (required)
  - `--drop=False` ranges over \["True", "False"\] and indicates whether to drop the database. WARNING: if set to True, this action is not reversible!
  - An example: `python3 src/main.py --hospital_name=IT_BUZZI_UC1 --database_name=currenttest --metadata_filepath=datasets/metadata/IT-Buzzi-variables.csv --data_filepath=datasets/data/BUZZI/screening.csv --drop=False`
5. To run the tests: `python3 -m unittest discover`

