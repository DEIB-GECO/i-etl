# BETTER-fairificator
The FAIRification tools for BETTER project.


## Installation and execution

## With Docker image (recommended)

Requirements:
- Docker Desktop installed
- The Docker image

**From the root of the project, i.e., in `BETTER-fairificator` folder**

```shell
sudo docker run fairificator -d
```

To check whether the ETL has finished, one can run `docker logs <container_id> --follow` where `<container_id>` is the id of the container (obtained by `docker ps`). Otherwise, one can also attach stdout to Docker to print logs "in real time".


## Standalone

Requirements:
- A running MongoDB server (tested with v7)
- Python (tested with 3.12)

**From the root of the project, i.e., in `BETTER-fairificator` folder**, run the following instructions to 
create a virtual environment dedicated to the FAIRificator and to install Python requirements. 
```shell
python3 -m venv .venv-better-fairificator
source .venv-better-fairificator/bin/activate
python3 -m pip install --upgrade pip
pip3 install -r requirements.txt --no-cache-dir
```

To run the ETL script: `python3 src/main.py --hospital_name=X --database_name=Y ...` with the parameters listed below.


### Parameters

#### Related to the data to give to the ETL
| Parameter name    | Description                                                                           | Required | Values                 |
|-------------------|---------------------------------------------------------------------------------------|----------|------------------------|
| `--metadata`      | the absolute path to the metadata file.                                               | True     |                        |
| `--laboratory`    | the absolute path to one or several laboratory data files, separated with commas (,). | False    |                        | 
| `--diagnosis`     | the absolute path to one or several diagnosis data files, separated with commas (,).  | False    |                        |
| `--medicine`      | the absolute path to one or several medicine data files, separated with commas (,).   | False    |                        |
| `--imaging`       | the absolute path to one or several imaging data files, separated with commas (,).    | False    |                        |
| `--genomic`       | the absolute path to one or several genomic data files, separated with commas (,).    | False    |                        | 
| `--use_en_locale` | Whether to use the en_US locale instead of the one of the country.                    | True     | - "True"<br/>- "False" |

#### Related to the ETL itself

| Parameter name       | Description                                                                           | Required | Values                                                                                                                                   |
|----------------------|---------------------------------------------------------------------------------------|----------|------------------------------------------------------------------------------------------------------------------------------------------|
| `--hospital_name`    | the hospital name                                                                     | True     | - "it_buzzi_uc1"<br/>- "rs_imgge"<br/>- "es_hsjd"<br/>- "it_buzzi_uc3"<br/>- "es_terrassa"<br/>- "de-ukk"<br/>- "es_lafe"<br/>- "il_hmc" |
| `--db_connection`    | The connection string to the mongodb server.                                          | True     |                                                                                                                                          | 
| `--db_name`          | the database name.                                                                    | True     |                                                                                                                                          | 
| `--db_drop`          | Whether to drop the database. WARNING: if set to True, this action is not reversible! | True     | - "True"<br/>- "False"                                                                                                                   |
| `--extract`          | Whether to perform the Extract step of the ETL.                                       | True     | - "True"<br/>- "False"                                                                                                                   |
| `--analyze`          | Whether to perform a data analysis on the provided files.                             | True     | - "True"<br/>- "False"                                                                                                                   |
| `--transform`        | Whether to perform the Transform step of the ETL.                                     | True     | - "True"<br/>- "False"                                                                                                                   |
| `--load`             | Whether to perform the Load step of the ETL.                                          | True     | - "True"<br/>- "False"                                                                                                                   |
| `--db_upsert_policy` | Whether to update or do nothing when upserting tuples.                                | True     | - "DO_NOTHING"<br/>- "REPLACE"                                                                                                           |


## For developers

To run the test suite (when installed without Docker): `python3 -m unittest discover`

To build the Docker image: `sudo docker build . --tag fairificator` (at the project root). 
If there are errors about credentials, remove the line `credsStore` in your Docker config file (probably at `~/.docker/config.json`). 
You also need to have Docker Desktop installed and running to be able to build Docker instances. 
