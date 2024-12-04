# BETTER-fairificator
The FAIRification tools for BETTER project.


## 1. Requirements

- Docker Desktop is installed on the host machine

## 2. Running the BETTER-fairificator

- Get the image of the FAIRificator:
  - Either download the TAR image available in the repository (recommended)
  - Or build it from the repository (not recommended, see section "For developers")
- Create a folder with:
  - The FAIRificator Docker (TAR) image 
  - The `.env` file template
  - The `compose.yaml` file
- In that folder, load the TAR image within the Docker: `docker load < the-fairificator-image.tar`
- In that folder, follow any scenario described in the "Scenarios" section. The complete list of parameters is described in the "Parameters" section.
- To check whether the ETL has finished, one can run `docker ps`: if `the-etl` does not show in the list, this means that it is done.
- To check the logs of the ETL: 
  - One can either look at the log file produced by the ETL if one has specified the `SERVER_FOLDER_LOG_ETL` parameter
  - Otherwise, use `docker logs the-etl` and check whether the last line is "ETL has finished. Writing logs in files before exiting."

<!--
In that folder, run `X=Y docker compose up -d` to launch the FAIRification, such that:
`X` will be a supplementary environment variable, whose value is `Y`
`-d`, meaning `--daemon`, runs as a background process.
-->


## 3. Scenarios

### Minimal configuration for each scenario
**For any scenario**, you need to set within the `.env` file:
1. The hospital name: `HOSPITAL_NAME`
2. The database name: `DB_NAME`
3. The MongoDB folder path: `SERVER_FOLDER_MONGODB`
4. _If you want to keep the execution log: `SERVER_FOLDER_LOG_ETL`_
5. _If your data uses USA-styled dates and numbers: `USE_EN_LOCALE=True`_
   

### Scenario 1: run the ETL with real data

1. We expect to have variables as columns and patients as rows. Please pre-process your data if this is not the case. 
We also expect that each patient has an identifier (which will be further anonymized). 
2. Set also the following parameters in the `.env` file: 
   1. The folder and filepath to the metadata: `SERVER_FOLDER_METADATA` and `METADATA`
   2. The folders and filepaths to your (real or synthetic) data: `SERVER_FOLDER_PHENOTYPIC`, `PHENOTYPIC`, etc
   3. _If you previously loaded data in the database, and you want to keep it: `DB_DROP=False`. **It this is NOT set to `False`, previously loaded data will be lost.**_
3. Run `CONTEXT_MODE=DEV;MY_ENV_FILE=.env docker compose up -d`

### Scenario 2: generate synthetic data only

1. Set also the following parameters in the `.env` file: 
   1. The folder for storing synthetic data: `SERVER_FOLDER_DATA_GENERATION`
   2. The number of rows of synthetic data: `NB_ROWS`
2. Run `CONTEXT_MODE=GEN;MY_ENV_FILE=.env docker compose up -d`


### Scenario 3: generate synthetic data and run the ETL on it

1. Follow scenario 2
2. Follow scenario 1

### Scenario 4: obtain catalogue data from an existing database

1. Set also the following parameters in the `.env` file: 
   1. **Set `DB_DROP=False`**
2. Run `CONTEXT_MODE=CATALOGUE;MY_ENV_FILE=.env docker compose up -d`


## 4. Parameters (in `.env` file)

The template for the environment file is given in `.env`.  
Parameters with a * (star) are required, others can be left empty. 
The first value in the column "Values" is the default one.

### About data generation

We provide synthetic data generator for each medical center in the BETTER project. 
They rely on the metadata described by each center and generate `NB_ROWS` rows of data.  

| Parameter name                   | Description                                                                       | Values                       |
|----------------------------------|-----------------------------------------------------------------------------------|------------------------------|
| `SERVER_FOLDER_DATA_GENERATION`* | The absolute (server) path to the folder into which synthetic data will be saved. | `/dev/null` or a folder path |
| `NB_ROWS`*                       | The number of patient generated.                                                  | 1000 or any other integer    |

### About input data (synthetic or real) given to the ETL
| Parameter name              | Description                                                                         | Values                       |
|-----------------------------|-------------------------------------------------------------------------------------|------------------------------|
| `SERVER_FOLDER_METADATA`*   | The absolute (server) path to the folder containing the metadata file.              | `/dev/null` or a folder path |
| `METADATA`*                 | The metadata filename.                                                              | a filename                   |
| `SERVER_FOLDER_PHENOTYPIC`* | The absolute (server) path to the folder containing phenotypic data.                | `/dev/null` or a folder path |
| `PHENOTYPIC`                | The list of one or several phenotypic data filenames, separated with commas (,).    | empty or filename(s)         |
| `SERVER_FOLDER_DIAGNOSIS`*  | The absolute (server) path to the folder containing diagnosis data.                 | `/dev/null` or a folder path |
| `DIAGNOSIS`                 | The list of one or several diagnosis data filenames, separated with commas (,).     | empty or filename(s)         |
| `SERVER_FOLDER_MEDICINE`*   | The absolute (server) path to the folder containing medicine data.                  | `/dev/null` or a folder path |
| `MEDICINE`                  | The list of one or several medicine data filenames, separated with commas (,).      | empty or filename(s)         |
| `SERVER_FOLDER_IMAGING`*    | The absolute (server) path to the folder containing imaging data.                   | `/dev/null` or a folder path |
| `IMAGING`                   | The list of one or several imaging data filenames, separated with commas (,).       | empty or filename(s)         |
| `SERVER_FOLDER_GENOMIC`*    | The absolute (server) path to the folder containing genomic data.                   | `/dev/null` or a folder path |
| `GENOMIC`                   | The list of one or several genomic data filenames, separated with commas (,).       | empty or filenames           |
| `SERVER_FOLDER_PIDS`*       | The absolute (server) path to the folder containing the patient anonymization data. | `/dev/null` or a folder path |
| `ANONYMIZED_PIDS`           | The patient anonymized IDs filename. The ETL will create it if it does not exist.   | empty or filename(s)         |

### About the database 

The FAIR database is in the Docker image, but can be accessed by opening a port, e.g., 27018. 
Then, on can access it using that port (for instance, `mongosh --port 27018`).

| Parameter name           | Description                                                                         | Values                                                                                              |
|--------------------------|-------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| `HOSPITAL_NAME`*         | The hospital name.                                                                  | `it_buzzi_uc1`, `rs_imgge`, `es_hsjd`, `it_buzzi_uc3`, `es_terrassa`, `de_ukk`, `es_lafe`, `il_hmc` | 
| `DB_NAME`*               | The database name.                                                                  | `better_docker`                                                                                     | 
| `DB_DROP`*               | Whether to drop the database. **WARNING: if True, this action is not reversible!**  | `False`, `True`                                                                                     |
| `SERVER_FOLDER_MONGODB`* | The absolute (server) path to the folder in which MongoDB will store its databases. | a filepath                                                                                          |
| `DB_UPSERT_POLICY`*      | Whether to update or not existing data (duplicates).                                | `DO_NOTHING`, `REPLACE`                                                                             |

### About the ETL
| Parameter name           | Description                                                                         | Values                                                           |
|:-------------------------|-------------------------------------------------------------------------------------|------------------------------------------------------------------|
| `SERVER_FOLDER_LOG_ETL`* | The absolute (server) path to the folder in which the ETL will write its log files. | `/dev/null` or a folder path                                     |
| `USE_EN_LOCALE`*         | Whether to use the en_US locale instead of the one of the country.                  | `False`, `True`                                                  |
| `COLUMNS_TO_REMOVE`*     | The set of column names to not include in the final database.                       | `[]` (empty list), or a list with strings being the column names | 



## 4. For developers

### Build the Docker image

To be used when working with the FAIRificator repository

1. Install Docker Desktop and open it
2. From the root of the project, run `sudo docker build . --tag fairificator`
3. If an error saying `ERROR: Cannot connect to the Docker daemon at XXX. Is the docker daemon running?` occurs, Docker Desktop has not started. 
4. If an error saying `error getting credentials` occurs while building, go to your Docker config file (probably `~/.docker/config.json`) and remove the line `credsStore`. Then, save the file and build again the image.

### Steps to deploy the Docker image within a center

To be used when deploying the ETL within a center

1. Build the Docker image: see above section
2. Create a TAR image of the FAIRificator (only with the ETL, not with the mongo): `docker save fairificator > the-fairificator-image.tar`
3. Send that TAR image to the host machine, e.g., using `scp the-fairificator-image.tar "username@A.B.C.D:/somewhere/in/host/machine"`
4. Send the env file to the host machine in the same folder as the TAR image, e.g., using `scp .env "username@A.B.C.D:/somewhere/in/host/machine"`
5. Send the compose file to the host machine in the same folder as the TAR image, e.g., using `scp compose.yaml "username@A.B.C.D:/somewhere/in/host/machine`
6. Move to `/somewhere/in/host/` using `cd /somewhere/in/host/`
7. Load the TAR image within the Docker of the host machine: `docker load < the-fairificator-image.tar`
8. Follow any above scenario, i.e., tune the .env file and run the FAIRificator
