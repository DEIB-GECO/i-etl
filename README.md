# I-ETL
The ETL algorithm to create interoperable databases for the BETTER project.


## 1. Requirements

- Docker Desktop is installed on the host machine.

## 2. Running I-ETL (create an interoperable database with your data)

1. Get the image of the I-ETL:
  - Either download the TAR image available in the repository (recommended)
    - Go to the deployment artifacts page: https://git.rwth-aachen.de/padme-development/external/better/data-cataloging/etl/-/artifacts
    - Click on the "folder" icon of the latest build (generally the first line of the table in the page)
    - Download the TAR archive named `the-ietl-image.tar`
  - Or build it from the repository (not recommended, see section "For developers")
2. Download the `comose.yaml` file, available in the repository (https://git.rwth-aachen.de/padme-development/external/better/data-cataloging/etl/-/blob/main/compose.yaml?ref_type=heads)
3. Download the settings file `.env` file, available in the repository (https://git.rwth-aachen.de/padme-development/external/better/data-cataloging/etl/-/blob/main/.env?ref_type=heads)
4. Create a folder with:
  - The I-ETL Docker (TAR) image 
  - The `.env` file template
  - The `compose.yaml` file
5. In that folder, load the TAR image within the Docker: `docker load < the-ietl-image.tar`
6. In that folder, follow any scenario described in the "Scenarios" section. The complete list of parameters is described in the "Parameters" section.
7. To check whether the ETL has finished, one can run `docker ps`: if `the-etl` does not show in the list, this means that it is done.
8. To check the logs of the ETL: 
  - One can either look at the log file produced by I-ETL if one has specified the `SERVER_FOLDER_LOG_ETL` parameter
  - Otherwise, use `docker logs the-etl`.

<!--
In that folder, run `X=Y docker compose up -d` to launch the FAIRification, such that:
`X` will be a supplementary environment variable, whose value is `Y`
`-d`, meaning `--daemon`, runs as a background process.
-->


## 3. Scenarios

### Minimal configuration for each scenario
**For any scenario**, you need to set within the `.env` file:
1. The hospital name in `HOSPITAL_NAME` (see accepted values in Section 4)
2. The database name in `DB_NAME` 
3. The absolute path to your MongoDB folder in `SERVER_FOLDER_MONGODB` (this can be any folder on the host machine; it will contain MongoDB's data)
4. The locale to be used to read your data in `USE_LOCALE` (this is important to read dates and numeric values with your country's norm)
5. _If you want to keep the execution log: the absolute path to your log folder in `SERVER_FOLDER_LOG_ETL` (this can be any folder on the host machine; it will contain the ETL's log files)_
   

### Scenario 1: run the ETL with real data

#### We expect:
  - **Variables are columns, patients are rows** and patient have identifiers (which will be further anonymized by I-ETL), so please pre-process your data if this is not the case.
  - The input files have the **exact same name as specified in the metadata** (https://drive.google.com/drive/u/0/folders/1eJOtoXj192Z9u0VENn4BdOfJ2wm5xZ5Z)

#### 1. Set the following parameters in the `.env` file: 
   - `SERVER_FOLDER_METADATA` with the absolute path to the folder containing the metadata file.
   - `METADATA` with the name of the metadata file.
   - `SERVER_FOLDER_DATA` with the absolute path to the folder containing your data files.
   - `DATA_FILES` with the comma-separated list of the data file names (no space).
   - `PATIENT_ID` with the column name containing your patient IDs (e.g., `PatientID`, or `pid`, etc.).
   - _If you previously loaded data in the database, and you add new data to the existing database, set `DB_DROP=False`. **If this is NOT set to `False`, previously loaded data will be lost.**_
#### 2. Run the following commands (in the host terminal): 
  - `export CONTEXT_MODE=DEV`
  - `export ETL_ENV_FILE_NAME=.env`
  - `export ABSOLUTE_PATH_ENV_FILE=X` where `X` is the absolute path to your `.env` file

#### 3. Run I-ETL
  - From your folder (step 5 in section 2), run `docker compose --env-file ${ABSOLUTE_PATH_ENV_FILE} up -d` (`-d` stands for `--daemon`, meaning that I-ETL will run as a background process).

### Scenario 2: generate synthetic data only

#### 1. Set the following parameters in the `.env` file: 
   1. `SERVER_FOLDER_DATA_GENERATION` with the folder for storing synthetic data. 
   2. `NB_ROWS` with the number of rows (~= number of patients) to generate.
   3. _Note: synthetic data will be generated based on the hospital name because each hospital has its own structure of data._  

#### 2. Run the following commands in your terminal: 
  - `export CONTEXT_MODE=GEN`
  - `export ETL_ENV_FILE_NAME=.env`
  - `export ABSOLUTE_PATH_ENV_FILE=X` where `X` is the absolute path to your `.env` file

#### 3. Generate synthetic data
  - From your folder (step 5 in section 2), run `docker compose --env-file ${ABSOLUTE_PATH_ENV_FILE} up -d`


### Scenario 3: generate synthetic data and run I-ETL on it

#### 1. Follow scenario 2
#### 2. Follow scenario 1


## 4. Parameters (in `.env` file)

The template for the environment file is given in `.env`.  
Parameters with a * (star) are required, others can be left empty. 
The first value in the column "Values" is the default one.

### About data generation

We provide synthetic data generator for each medical center in the BETTER project. 
They rely on the metadata described by each center and generate `NB_ROWS` rows of data.  

| Parameter name                  | Description                                                                        | Values                                       |
|---------------------------------|------------------------------------------------------------------------------------|----------------------------------------------|
| `SERVER_FOLDER_DATA_GENERATION` | The absolute path to the folder into which generated synthetic data will be saved. | `/dev/null` or an absolute folder path       |
| `NB_ROWS`                       | The number of generated patients.                                                  | `100` or any other non-zero positive integer |

### About input data (synthetic or real) given to the ETL
| Parameter name            | Description                                                                     | Values                                 |
|---------------------------|---------------------------------------------------------------------------------|----------------------------------------|
| `SERVER_FOLDER_METADATA`* | The absolute path to the folder containing the metadata file.                   | `/dev/null` or an absolute folder path |
| `METADATA`*               | The metadata filename.                                                          | a filename                             |
| `SERVER_FOLDER_DATA`*     | The absolute path to the folder containing the datasets.                        | `/dev/null` or a folder path           |
| `DATA_FILES`              | The list of comma-separated filenames.                                          | ` ` (empty) or filename(s)             |
| `SERVER_FOLDER_PIDS`*     | The absolute path to the folder containing the patient anonymization data.      | `/dev/null` or a folder path           |
| `ANONYMIZED_PIDS`         | The patient anonymized IDs filename. I-ETL will create it if it does not exist. | ` `(empty) or filename(s)              |

### About the database 

The FAIR database is in the Docker image, but can be accessed by opening a port, e.g., 27018. 
Then, one can access it using that port (for instance, `mongosh --port 27018`).

| Parameter name           | Description                                                                        | Values                                                                                              |
|--------------------------|------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| `HOSPITAL_NAME`*         | The hospital name.                                                                 | `it_buzzi_uc1`, `rs_imgge`, `es_hsjd`, `it_buzzi_uc3`, `es_terrassa`, `de_ukk`, `es_lafe`, `il_hmc` | 
| `DB_NAME`*               | The database name.                                                                 | `better_default`                                                                                    | 
| `DB_DROP`                | Whether to drop the database. **WARNING: if True, this action is not reversible!** | `False`, `True`                                                                                     |
| `SERVER_FOLDER_MONGODB`* | The absolute path to the folder in which MongoDB will store its databases.         | a folder path                                                                                       |

### About the ETL
| Parameter name            | Description                                                                                | Values                                                           |
|:--------------------------|--------------------------------------------------------------------------------------------|------------------------------------------------------------------|
| `SERVER_FOLDER_LOG_ETL`*  | The absolute path to the folder in which I-ETL will write its log files.                   | `/dev/null` or a folder path                                     |
| `USE_LOCALE`*             | The locale to be used for reading numerics and dates.                                      | `en_GB`, `en_US`, `fr_FR`, `it_IT`, etc.                         |
| `COLUMNS_TO_REMOVE`*      | The set of column names to not include in the final database.                              | `[]` (empty list), or a list with strings being the column names |
| `RECORD_CARRIER_PATIENTS` | Whether to records patients carrying diseases without being affected as diagnosed patients | `False`, `True`                                                  |
| `PATIENT_ID`              | The name of the column in the data containing patient IDs                                  | `Patient ID`, or any other column name                           |
| `SAMPLE_ID`               | The name of the column in the data containing sample IDs                                   | ` ` (empty) if you do not have sample data, else a column name   |



## 4. For developers

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
