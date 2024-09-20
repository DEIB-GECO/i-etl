# BETTER-fairificator
The FAIRification tools for BETTER project.


## Installation and execution (with Docker)

**Requirements:**
- Docker Desktop installed on the host machine
- The Docker image of the FAIRification
- The Docker image of mongo (publicly available in the DockerHub)
- The `.env` (with no default values for the filepaths; to be completed manually with the server folder structure before running the ETL)
- The `compose.yml`

**Steps:**
1. Put `compose.yml`, `.env` and the two Docker images in the same folder
2. From that specific folder, run `docker compose up -d`. _Note_: `-d` means `--daemon`, meaning that the ETL will run as a background process.
    - To check whether the ETL has finished, one can run `docker ps`: 
      if `the-etl` does not show, this means that it is done. 


## Parameters (in `.env` file)

An example of a valid `.env` file is available in `.env.example`. 
**The final file containing environment variables should ALWAYS be labelled `.env`.**

Parameters with a * (star) are required, others can be left empty.

### Related to the data to give to the ETL
| Parameter name             | Description                                                                                                        | 
|----------------------------|--------------------------------------------------------------------------------------------------------------------|
| `SERVER_FOLDER_METADATA`*  | The absolute (server) path to the folder containing the metadata file.                                             |
| `METADATA`*                | The metadata filename.                                                                                             |
| `SERVER_FOLDER_LABORATORY` | The absolute (server) path to the folder containing laboratory data.<br/>Default value: `/dev/null`                |
| `LABORATORY`               | The list of one or several laboratory data filenames, separated with commas (,).<br/>Default value: empty          |
| `SERVER_FOLDER_DIAGNOSIS`  | The absolute (server) path to the folder containing diagnosis data.<br/>Default value: `/dev/null`                 | 
| `DIAGNOSIS`                | The list of one or several diagnosis data filenames, separated with commas (,).<br/>Default value: empty           | 
| `SERVER_FOLDER_MEDICINE`   | The absolute (server) path to the folder containing medicine data.<br/>Default value: `/dev/null`                  |
| `MEDICINE`                 | The list of one or several medicine data filenames, separated with commas (,).<br/>Default value: empty            |
| `SERVER_FOLDER_IMAGING`    | The absolute (server) path to the folder containing imaging data.<br/>Default value: `/dev/null`                   |
| `IMAGING`                  | The list of one or several imaging data filenames, separated with commas (,).<br/>Default value: empty             |
| `SERVER_FOLDER_GENOMIC`    | The absolute (server) path to the folder containing genomic data.<br/>Default value: `/dev/null`                   |
| `GENOMIC`                  | The list of one or several genomic data filenames, separated with commas (,).<br/>Default value: empty             |
| `SERVER_FOLDER_PIDS`       | The absolute (server) path to the folder containing the patient anonymization data.<br/>Default value: `/dev/null` |
| `ANONYMIZED_PIDS`          | The patient anonymized IDs filename. The ETL will create it if it does not exist.<br/>Default value: empty         |

### Related to the database

| Parameter name           | Description                                                                                                                        |
|--------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| `HOSPITAL_NAME`*         | The hospital name.<br/>Values: `it_buzzi_uc1`, `rs_imgge`, `es_hsjd`, `it_buzzi_uc3`, `es_terrassa`, `de-ukk`, `es_lafe`, `il_hmc` | 
| `DB_NAME`*               | The database name.<br/>Default value: `better_docker`                                                                              |                                                                                                                          | 
| `DB_DROP`*               | Whether to drop the database. **WARNING: if set to True, this action is not reversible!**<br/>Values: `True`, `False`              | 
| `SERVER_FOLDER_MONGODB`* | The absolute (server) path to the folder in which MongoDB will store its databases.                                                |                                                                                                                          |
| `DB_UPSERT_POLICY`       | Whether to update or not existing data (duplicates)<br/>Values: `DO_NOTHING`, `REPLACE`<br/>Default value: `DO_NOTHING`            |

### Related to the ETL
| Parameter name           | Description                                                                                                               |
|:-------------------------|---------------------------------------------------------------------------------------------------------------------------|
| `SERVER_FOLDER_LOG_ETL`* | The absolute (server) path to the folder in which the ETL will write its log files.                                       |
| `EXTRACT`                | Whether to perform the Extract step of the ETL.<br/>Values: `True`, `False`<br/>Default value: `True`                     |
| `TRANSFORM`              | Whether to perform the Transform step of the ETL.<br/>Values: `True`, `False`<br/>Default value: `True`                   |
| `LOAD`                   | Whether to perform the Load step of the ETL.<br/>Values: `True`, `False`<br/>Default value: `True`                        |
| `USE_EN_LOCALE`          | Whether to use the en_US locale instead of the one of the country.<br/>Values: `True`, `False`<br/>Default value: `False` |


## For developers

### Building the Docker image

1. Install Docker Desktop and open it
2. From the root of the project, run `sudo docker build . --tag fairificator`
3. If an error about `error getting credentials` occurs while building, go to your Docker config file (probably `~/.docker/config.json`) and remove the line `credsStore`. Then, save the file and build again the image.