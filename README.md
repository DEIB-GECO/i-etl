# I-ETL
The ETL algorithm creates interoperable databases for the BETTER project. It relies on MongoDB to store the data and reads a .env file for the configuration. The code is written in Python and encapsulated in a Docker.


## 1. Requirements

- Docker Desktop is installed on the host machine.
- **All input files are in the same folder on your machine, and they have the exact same name as specified in the metadata.**
  - Specifically, for VCF data: 
    - You can group them in a sub-folder (or not).
    - If you grouped them in a folder, e.g., named `vcf-folder`, please provide `vcf-folder/*.vcf` for the data files in the `.env` file.
    - If you did not group them, please provide `*.vcf` for the data files in the `.env` file.
    - The VCF files should be named with the patient ID (exactly and only the patient ID used the other data).
- **In your data files: variables are columns, patients are rows** and patients have identifiers (which will be further anonymized by I-ETL).
- **The column name for the patient ID, respectively the sample ID, is the same for all the files used to build the database.**

## 2. Use I-ETL to create an interoperable database with your data

1. Get the image of the I-ETL:
  - Either download the TAR image available in the repository (recommended)
    - Go to the deployment artifacts page: https://git.rwth-aachen.de/padme-development/external/better/data-cataloging/etl/-/artifacts
    - Click on the "folder" icon of the latest valid build (the most recent one with a green &#9745;)
    - Download the **TAR archive** named `the-ietl-image.tar` (be sure to download a `.tar`, not a `.zip`)
  - Or build it from the repository (not recommended, see section "For developers")
2. Download the `comose.yaml` file, available in the repository (https://git.rwth-aachen.de/padme-development/external/better/data-cataloging/etl/-/blob/main/compose.yaml?ref_type=heads)
3. Download the settings file `.env` file, available in the repository (https://git.rwth-aachen.de/padme-development/external/better/data-cataloging/etl/-/blob/main/.env?ref_type=heads)
4. Download your metadata file in the Better Drive: https://drive.google.com/drive/u/1/folders/1J-3C2g06WbC1gUE_3KaDp3_v1uKHXxFV
  - `ES-HSJD-variables.xlsx` for SJD hospital
  - `IT-BUZZI-variables.xlsx` for BUZZI hospital in use-case 1
  - `RS-IMGGE-variables.xlsx` for IMGGE hospital
  - `UC2-variables.xlsx` for LAFE and HMC hospitals
  - `UC3-variables-02-04-2025.xlsx` for TERRASSA, UKK and BUZZI hospitals
4. Create a folder, e.g., named `better`, with:
  - The I-ETL Docker (TAR) image 
  - The `.env` file template
  - The `compose.yaml` file
  - The metadata file
5. In that folder, load the TAR image within the Docker: `docker load < the-ietl-image.tar`
6. In that folder, fill the `.env` file with your own settings (see Section 3)
7. In that folder, launch I-ETL by running the following commands:
  - `export CONTEXT_MODE=DEV`
  - `export ETL_ENV_FILE_NAME=.env`
  - `export ABSOLUTE_PATH_ENV_FILE=X` where `X` is the absolute path to your `.env` file
  - `docker compose --env-file ${ABSOLUTE_PATH_ENV_FILE} up -d` (`-d` stands for `--daemon`, meaning that I-ETL will run as a background process).
7. To check whether I-ETL has finished, you can run `docker ps`: if `the-etl` does not show in the list, this means that it is done.
8. To check the logs of the ETL, you have two options: 
  - If you have specified the parameter `SERVER_FOLDER_LOG_ETL` in your `.env`, you can look at the log files produced in the folder you specified in that parameter;
  - Otherwise, use `docker logs the-etl`.

## 3. Parameters in the `.env` file

The `.env` file is a file to specify several parameters that I-ETL needs to run properly. 
This includes parameters about the database, the files to use, the language, etc.

The provided `.env` file is a _template_: you have to fill each parameter with your own value. 

### About input data (synthetic or real) given to the ETL
| Parameter name           | Description                                                   | Values        | Example                                           |
|--------------------------|---------------------------------------------------------------|---------------|---------------------------------------------------|
| `SERVER_FOLDER_METADATA` | The absolute path to the folder containing the metadata file. | A folder path | `/home/better/data`                               |
| `METADATA`               | The metadata filename.                                        | A filename    | `ES-HSJD-variables.xlsx`                          |
| `SERVER_FOLDER_DATA`     | The absolute path to the folder containing the datasets.      | A folder path | `/home/better/data`                               |
| `DATA_FILES`             | The list of comma-separated filenames.                        | Filename(s)   | `Baseline_Clinical_Table.xlsx,Genomic_Table.xlsx,vcf-folder/*.vcf` |

### About the database 

| Parameter name          | Description                                                                                                                             | Values                                                                                                                                                               | Example               |
|-------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------|
| `HOSPITAL_NAME`         | The hospital name.                                                                                                                      | `it_buzzi_uc1`, `rs_imgge`, `es_hsjd`, `it_buzzi_uc3`, `es_terrassa`, `de_ukk`, `es_lafe`, `il_hmc`                                                                  | any value in the list |
| `DB_NAME`               | The database name.                                                                                                                      | `better_database` or any string without special character except `_` (underscore). **Please use `better_database` for any database created for the Better project.** | `better_database`     |
| `DB_DROP`               | Whether to drop the database. **WARNING: if True, this deletes the database before creating a new one: this action is NOT reversible!** | `False`, `True`                                                                                                                                                      |                       |
| `SERVER_FOLDER_MONGODB` | The absolute path to the folder in which MongoDB will store its databases.                                                              | A folder path                                                                                                                                                        | `/home/mongodb-data`  |

### About the ETL
| Parameter name            | Description                                                                                                        | Values                                                           | Example                              |
|:--------------------------|--------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------|--------------------------------------|
| `SERVER_FOLDER_LOG_ETL`   | The absolute path to the folder in which I-ETL will write its log files.                                           | A folder path                                                    | `/home/better/logs`                  |
| `USE_LOCALE`              | The locale to be used for reading numerics and dates.                                                              | `en_GB`, `en_US`, `es_ES`, `it_IT`, `sr_RS`.                     | `en_GB`                              |
| `COLUMNS_TO_REMOVE`       | The list of columns that are too sensitive, thus NOT included in the database.                                     | `[]` (empty list), or a list with strings being the column names | [`patient_address`, `date_of_birth`] |
| `RECORD_CARRIER_PATIENTS` | **For Buzzi hospital in use-case 1 only**: whether to records carrier and diseased patients or only diseased ones. | `False`, `True`                                                  |                                      |
| `PATIENT_ID`              | The name of the column in the data containing patient IDs                                                          | Any column name                                                  | `Patient ID`                         |
| `SAMPLE_ID`               | The name of the column in the data containing sample IDs                                                           | ` ` (empty) if you do not have sample data, else a column name   | `sample_id`                          |


## 4. Querying the ETL database within a train

### Steps 
Given a user input (explained below), the class `DataRetriever` takes care of:
1. Generating the MongoDB query to fetch the data or metadata in the database
2. Loading the retrieved data, respectively metadata, in a Pandas DataFrame

The user input is:
- the information to connect to the database (MongoDB URL and database name)
- the features (also called "variables") the user is interested in when collecting data
- the post-process methods to "flatten"/"normalize" the data values if needed

The package is available in `pip` meaning that it should be added to the requirements of the train (as in `pip install data-retriever`). To be sure that you have all the required package:
1. Download the `requirements.txt` file in this repository
2. Create a virtual Python environment (using Python venv or Anaconda)
3. Activate that environment
4. Install all the required packages with `pip install -r requirements.txt`
5. Install the `data-retriever` package with `pip install data-retriever` (latest version is 1.7)
6. Then, you should adapt the main file `query.py` with your own settings (database name, etc.)
7. Finally, you can run the `query.py` file with `python3 query.py`

### Example to retrieve data

A main example is available in the `query.py` file (https://git.rwth-aachen.de/padme-development/external/better/data-cataloging/etl/-/blob/main/src/query.py).  

Lines 9 to 11, respectively 21 to 23, contain the user input:
- The variable `FEATURE_CODES` is a dictionary (map) to associate a variable name to the ontology term that has been associated to it in the metadata (https://drive.google.com/drive/u/1/folders/1J-3C2g06WbC1gUE_3KaDp3_v1uKHXxFV)
- The variable `FEATURE_FILTERS` is a dictionary (map) to associate a variable name to (a) its code under the key `code`, and (b) the filter to apply under the key `filter`. The value of the `filter` should be a MongoDB `match`. Note that the variables mentioned in the filters may differ from the ones selected. 
- The variable `FEATURES_VALUE_PROCESS` is a dictionary (map) to associate a variable to a MongoDB operator to process/flatten the fetched values. The values in this dictionary can be either `get_label` to get the human-readable name of a category, or any other MongoDB operator. It should be used for the variables leading to non-atomic values (especially dictionaries). If no process is needed (because the value is atomic) or you do not know which MongoDB operator to choose, use `None`.

The next lines create a new `DataRetriever` with the information for the MongoDB connection, user variables, and the query type which is `data`. The method `run()` generates the MongoDB query to fetch the data from the specified database. 
Then, it loads the fetched data into a DataFrame. This DataFrame is accessible in the variable `the_dataframe` (see `dataRetriever.the_dataframe`). Finally, the dataframe is exported to a CSV file.

For instance, in the `query.py` file, the first query fetches, for all female patients, their associated VCF filepath and whether they have hypotonia. The second query retrieves the same information for the IMGGE hospital. 

## Example to retrieve metadata

A main example is available in the `query.py` file (https://git.rwth-aachen.de/padme-development/external/better/data-cataloging/etl/-/blob/main/src/query.py).  

Line 33 contains the user input:
- The variable `FEATURE_CODES` is an array (list) of the variable codes we want to collect metadata for (to find the variable codes, look at the metadata: https://drive.google.com/drive/u/1/folders/1J-3C2g06WbC1gUE_3KaDp3_v1uKHXxFV)

The next lines create a new `DataRetriever` with the information for the MongoDB connection, user variables, and the query type which is `metadata`. The method `run()` generates the MongoDB query to fetch the metadata from the specified database. 
Then, it loads the fetched metadata into a DataFrame. This DataFrame is accessible in the variable `the_dataframe` (see `dataRetriever.the_dataframe`). Finally, the dataframe is exported to a CSV file.

For instance, in the `query.py` file, the last query fetches all metadata information about variables `hypotonia`, `vcf_path` and `gene`. The retrieved information concerns the variable name, code, data type, categories, visibility, etc. (all information specified in the metadata: to find the variable codes, look at the metadata: https://drive.google.com/drive/u/1/folders/1J-3C2g06WbC1gUE_3KaDp3_v1uKHXxFV)

## 5. For developers

### Build the Docker image

To be used when working with the I-ETL repository

1. Install Docker Desktop and open it
2. From the root of the project, run `docker build . --tag ietl`
3. If an error saying `ERROR: Cannot connect to the Docker daemon at XXX. Is the docker daemon running?` occurs, Docker Desktop has not started. 
4. If an error saying `error getting credentials` occurs while building, go to your Docker config file (probably `~/.docker/config.json`) and remove the line `credsStore`. Then, save the file and build again the image.

### Steps to deploy the Docker image within a center

To be used when deploying I-ETL within a center

1. Locally, build the Docker image: see above section
2. Locally, create a TAR image of I-ETL (only with the ETL, not with the mongo): `docker save ietl > the-ietl-image.tar`
3. Send that TAR image to the host machine, e.g., using `scp the-ietl-image.tar "username@A.B.C.D:/somewhere/in/host/machine"`
4. Send the env file to the host machine in the same folder as the TAR image, e.g., using `scp .env "username@A.B.C.D:/somewhere/in/host/machine"`
5. Send the compose file to the host machine in the same folder as the TAR image, e.g., using `scp compose.yaml "username@A.B.C.D:/somewhere/in/host/machine`
6. In the host machine, move to `/somewhere/in/host/machine/` using `cd /somewhere/in/host/machine`
7. In the host machine, load the TAR image within the Docker of the host machine: `docker load < the-ietl-image.tar`
8. In the host machine, follow any above scenario, i.e., tune the .env file and run I-ETL
